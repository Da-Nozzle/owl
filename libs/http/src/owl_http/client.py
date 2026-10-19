import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx
from tenacity import (
    RetryCallState,
    Retrying,
    retry_if_exception,
    retry_if_result,
    stop_after_attempt,
)

from owl_http.settings import HttpClientSettings

# Over a rate limit or quota, or a server's own failure: may be answered differently later.
RETRY_STATUSES = frozenset({429, *range(500, 600)})
# A connection that failed, dropped or went silent; a request that cannot work fails at once.
RETRY_ERRORS = (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)
# A server gives the reason it refused a request in the body, not the status line.
ERROR_DETAIL_CHARACTERS = 500


def build_http_client(
    user_agent: str,
    settings: HttpClientSettings,
    *,
    transport: httpx.BaseTransport | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> httpx.Client:
    """An error status raises `httpx.HTTPStatusError`, with the start of the body, once its
    retries are over. Tests replace the network under the retries with `transport`."""
    # A client given a transport reads no proxy from the environment (HTTPS_PROXY): one behind a
    # proxy needs it given to RetryingTransport's inner httpx.HTTPTransport.
    return httpx.Client(
        headers={"User-Agent": user_agent},
        timeout=settings.timeout_seconds,
        follow_redirects=True,
        transport=RetryingTransport(settings, inner=transport, sleep=sleep),
        event_hooks={"response": [raise_for_error_status]},
    )


def build_streaming_http_client(
    user_agent: str,
    settings: HttpClientSettings,
    *,
    transport: httpx.BaseTransport | None = None,
) -> httpx.Client:
    """A client whose bodies are read as they arrive (`client.stream()`), for files too big to
    hold in memory. It retries nothing, as a body half read cannot be asked for again from inside
    the request: `retrying_whole_downloads` retries each download from its start instead."""
    return httpx.Client(
        headers={"User-Agent": user_agent},
        timeout=settings.timeout_seconds,
        follow_redirects=True,
        transport=transport,
        event_hooks={"response": [raise_for_error_status]},
    )


def retrying_whole_downloads(
    settings: HttpClientSettings, *, sleep: Callable[[float], None] = time.sleep
) -> Retrying:
    """Retries a whole download through a streaming client as RetryingTransport retries an
    attempt: after a timeout or a dropped connection, before its body or while it is read, or a
    429 or 5xx (which the client's hook raises), waiting what Retry-After asks for or the
    policy's backoff. Anything else the download raises fails it at once."""

    def wait_seconds(state: RetryCallState) -> float:
        error = state.outcome.exception() if state.outcome is not None else None
        asked = None
        if isinstance(error, httpx.HTTPStatusError):
            asked = retry_after_seconds(
                error.response.headers.get("Retry-After"), datetime.now(UTC)
            )
        return settings.retry.wait_seconds(state.attempt_number, asked)

    return Retrying(
        stop=stop_after_attempt(settings.retry.attempts),
        wait=wait_seconds,
        retry=retry_if_exception(is_transient_download_error),
        sleep=sleep,
        reraise=True,
    )


class RetryingTransport(httpx.BaseTransport):
    """Retries each request as the settings' policy allows, each attempt starting at least
    `min_interval_seconds` after the last. An attempt reads the whole body, so a server going
    silent midway is retried too; `client.stream()` gets a body read whole. Thread-safe."""

    def __init__(
        self,
        settings: HttpClientSettings,
        *,
        inner: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._settings = settings
        self._inner = inner
        self._sleep = sleep
        self._lock = threading.Lock()
        self._pacer = (
            Pacer(settings.min_interval_seconds, wait=sleep)
            if settings.min_interval_seconds > 0
            else None
        )

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        retrying = Retrying(
            stop=stop_after_attempt(self._settings.retry.attempts),
            wait=self._wait_seconds,
            retry=retry_if_exception(is_transient) | retry_if_result(is_retried_response),
            sleep=self._sleep,
            retry_error_callback=last_outcome,
        )
        return retrying(self._attempt, request)

    def close(self) -> None:
        with self._lock:
            if self._inner is not None:
                self._inner.close()

    def _attempt(self, request: httpx.Request) -> httpx.Response:
        if self._pacer is not None:
            self._pacer.wait_turn()
        response = self._transport().handle_request(request)
        # Still encoded, for the client to decode once. Read from the stream, as a MockTransport's
        # response is read already and its iter_raw() refuses to read it again.
        assert isinstance(response.stream, httpx.SyncByteStream)
        try:
            body = b"".join(response.stream)
        finally:
            response.close()
        # Not content=, whose stream the client never closes, which is what sets `elapsed`.
        return httpx.Response(
            response.status_code,
            headers=response.headers,
            stream=httpx.ByteStream(body),
            extensions=response.extensions,
        )

    def _wait_seconds(self, state: RetryCallState) -> float:
        outcome = state.outcome
        if outcome is None or outcome.failed:
            return self._settings.retry.wait_seconds(state.attempt_number)
        asked = retry_after_seconds(outcome.result().headers.get("Retry-After"), datetime.now(UTC))
        return self._settings.retry.wait_seconds(state.attempt_number, asked)

    def _transport(self) -> httpx.BaseTransport:
        # Built on the first request: it loads the CA bundle (about 0.2 s), which loading the
        # Dagster definitions should not pay for.
        with self._lock:
            if self._inner is None:
                self._inner = httpx.HTTPTransport()
            return self._inner


@dataclass
class Pacer:
    """Lets calls start no sooner than `interval_seconds` after the last one, from any thread."""

    interval_seconds: float
    clock: Callable[[], float] = time.monotonic
    wait: Callable[[float], None] = time.sleep
    _last_start: float | None = field(default=None, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def wait_turn(self) -> None:
        with self._lock:
            if self._last_start is not None:
                remaining = self._last_start + self.interval_seconds - self.clock()
                if remaining > 0:
                    self.wait(remaining)
            self._last_start = self.clock()


def is_transient(error: BaseException) -> bool:
    return isinstance(error, RETRY_ERRORS)


def is_retried_response(response: httpx.Response) -> bool:
    return response.status_code in RETRY_STATUSES


def is_transient_download_error(error: BaseException) -> bool:
    """A transient error, or an error status the client's hook raised that is worth retrying."""
    return is_transient(error) or (
        isinstance(error, httpx.HTTPStatusError) and is_retried_response(error.response)
    )


def last_outcome(state: RetryCallState) -> httpx.Response:
    """The last attempt's response, or its error raised again."""
    assert state.outcome is not None
    return state.outcome.result()


def raise_for_error_status(response: httpx.Response) -> None:
    """Raises with the start of the body, where a server says why; httpx's `raise_for_status`
    leaves it out, and raises for a redirect too, which the hook sees on its way."""
    if not response.is_error:
        return
    response.read()
    request = response.request
    raise httpx.HTTPStatusError(
        f"{response.status_code} {response.reason_phrase} from {request.method} {request.url}: "
        f"{response.text[:ERROR_DETAIL_CHARACTERS]}",
        request=request,
        response=response,
    )


def retry_after_seconds(value: str | None, now: datetime) -> float | None:
    """Retry-After, a number of seconds or an HTTP date, as seconds from `now`; None unless it
    asks for a wait."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        seconds = float(value)
    else:
        try:
            moment = parsedate_to_datetime(value)
        except TypeError, ValueError:
            return None
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        seconds = (moment - now).total_seconds()
    return seconds if seconds > 0 else None
