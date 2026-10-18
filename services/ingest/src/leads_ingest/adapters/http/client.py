"""Every request a source sends goes through an httpx client built here (`build_http_client`),
configured by the source's `HttpClientSettings`: the pipeline's user agent, redirects followed,
the source's timeout, and a transport that retries with tenacity what a server may answer
differently a moment later and spaces requests apart. Adapters get a plain `httpx.Client`.

A client given a transport reads no proxy from the environment (HTTPS_PROXY and the like): httpx
reads them only for the transport it builds itself. A deployment behind a proxy needs one given to
`RetryingTransport`'s inner `httpx.HTTPTransport`."""

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

from leads_ingest.adapters.http.settings import HttpClientSettings

# A response that may be different if asked again: over a rate limit or quota (429), or a
# server's own failure (5xx), which a proxy in front of a portal answers for a moment at times.
RETRY_STATUSES = frozenset({429, *range(500, 600)})  # Code 429, and all 5xx codes
# A connection that failed, dropped or went silent. Not a request that cannot work (an unsupported
# scheme, a malformed header), which fails at once.
RETRY_ERRORS = (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)
# The longest part of an error response kept in the exception: a server explains a refused request
# (Google: a field it does not know, a key not allowed the API) in the body, not the status line.
ERROR_DETAIL_CHARACTERS = 500


def build_http_client(
    user_agent: str,
    settings: HttpClientSettings,
    *,
    transport: httpx.BaseTransport | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> httpx.Client:
    """A source's client. A response with an error status raises `httpx.HTTPStatusError`, with
    the start of its body, once it is not retried or its retries are over. Tests replace the
    network under the retries with `transport` (an `httpx.MockTransport`), and the waits with
    `sleep`."""
    return httpx.Client(
        headers={"User-Agent": user_agent},
        timeout=settings.timeout_seconds,
        follow_redirects=True,
        transport=RetryingTransport(settings, inner=transport, sleep=sleep),
        event_hooks={"response": [raise_for_error_status]},
    )


class RetryingTransport(httpx.BaseTransport):
    """Sends each request through `inner` as many times as the settings' retry policy allows,
    each attempt no sooner than `min_interval_seconds` after the last started. An attempt reads the
    whole body, so a server that goes silent halfway through one is retried too, and a response is
    always read whole, `client.stream()` included. Once the attempts are used, the last response
    is the answer (the client's hook raises for its status), or the last error is raised.

    `inner`, httpx's own transport with its connection pool unless a test gives one, is built on
    the first request: building one loads the CA bundle, about 0.2 s, which loading the definitions
    should not pay. Its pool, httpx's default of 100 connections, holds more than any source has in
    flight (google_places.workers). Safe for several threads at once."""

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
        # The body as the server sent it (still encoded, if it was), for the client to decode
        # once. Read from the stream, as httpx's client does: a response made in memory (a
        # MockTransport's) is read already, and its iter_raw() refuses to read it again.
        assert isinstance(response.stream, httpx.SyncByteStream)
        try:
            body = b"".join(response.stream)
        finally:
            response.close()
        # A stream, not content=: httpx reads content= at once, and the client then never
        # closes the response's stream, which is what sets its `elapsed`.
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
        with self._lock:
            if self._inner is None:
                self._inner = httpx.HTTPTransport()
            return self._inner


@dataclass
class Pacer:
    """Lets calls start no sooner than `interval_seconds` after the last one started, whichever
    thread makes them, to stay under a server's rate limit."""

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


def last_outcome(state: RetryCallState) -> httpx.Response:
    """The last attempt's response, or its error raised again."""
    assert state.outcome is not None
    return state.outcome.result()


def raise_for_error_status(response: httpx.Response) -> None:
    """The client's response hook. httpx's `raise_for_status` names the status and URL only, and
    raises for a redirect too, which the hook sees on its way; the reason a server gives for an
    error is in its body."""
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
    """A Retry-After header as seconds from `now`: it is a number of seconds or an HTTP date.
    None if there is none, or it cannot be read, or it asks for no wait."""
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
