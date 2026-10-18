import gzip
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from leads_http.client import Pacer, build_http_client, retry_after_seconds
from leads_http.settings import HttpClientSettings, RetryPolicy

URL = "https://source.test/file.csv"
SETTINGS = HttpClientSettings(
    timeout_seconds=1,
    retry=RetryPolicy(attempts=3, backoff_seconds=1, max_wait_seconds=60),
    min_interval_seconds=0,
)
NOW = datetime(2026, 10, 18, 7, 28, tzinfo=UTC)


@dataclass
class FakeClock:
    """A monotonic clock that a wait moves forward."""

    now: float = 100.0
    waits: list[float] = field(default_factory=list)

    def __call__(self) -> float:
        return self.now

    def wait(self, seconds: float) -> None:
        self.waits.append(seconds)
        self.now += seconds


class StallingBody(httpx.SyncByteStream):
    """A body whose server goes silent after its first bytes."""

    def __iter__(self) -> Iterator[bytes]:
        yield b"id,"
        raise httpx.ReadTimeout("silent")


def answering(
    *answers: httpx.Response | Exception,
) -> tuple[httpx.MockTransport, list[httpx.Request]]:
    """A transport giving each request the next answer, raising it if it is an error, and the
    list of requests it was sent."""
    sent: list[httpx.Request] = []
    remaining: Iterator[httpx.Response | Exception] = iter(answers)

    def handle(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        answer = next(remaining)
        if isinstance(answer, Exception):
            raise answer
        return answer

    return httpx.MockTransport(handle), sent


def client_with(
    transport: httpx.MockTransport, waits: list[float], settings: HttpClientSettings = SETTINGS
) -> httpx.Client:
    return build_http_client("leads-test/1.0", settings, transport=transport, sleep=waits.append)


def test_a_server_error_is_retried_after_a_doubling_wait() -> None:
    # Arrange
    transport, sent = answering(
        httpx.Response(503), httpx.Response(502), httpx.Response(200, content=b"id")
    )
    waits: list[float] = []

    # Act
    response = client_with(transport, waits).get(URL)

    # Assert
    assert response.content == b"id"
    assert (len(sent), waits) == (3, [1, 2])


def test_a_dropped_or_silent_connection_is_retried() -> None:
    # Arrange
    transport, sent = answering(
        httpx.ConnectError("refused"),
        httpx.ReadTimeout("silent"),
        httpx.Response(200, content=b"id"),
    )

    # Act
    response = client_with(transport, []).get(URL)

    # Assert
    assert (response.content, len(sent)) == (b"id", 3)


def test_a_body_that_stops_halfway_is_asked_for_again() -> None:
    # Arrange
    transport, sent = answering(
        httpx.Response(200, stream=StallingBody()), httpx.Response(200, content=b"id,name")
    )

    # Act
    response = client_with(transport, []).get(URL)

    # Assert
    assert (response.content, len(sent)) == (b"id,name", 2)


def test_a_refused_request_fails_at_once_with_the_servers_reason() -> None:
    # Arrange
    transport, sent = answering(httpx.Response(400, json={"error": "Invalid field: places.foo"}))
    waits: list[float] = []

    # Act / Assert
    with pytest.raises(httpx.HTTPStatusError, match=r"(?s)400 Bad Request .*Invalid field"):
        client_with(transport, waits).get(URL)
    assert (len(sent), waits) == (1, [])


def test_the_last_answer_fails_the_request_once_the_attempts_are_used() -> None:
    # Arrange
    transport, sent = answering(*(httpx.Response(500, text="down") for _ in range(3)))

    # Act / Assert
    with pytest.raises(httpx.HTTPStatusError, match=r"500 Internal Server Error .*: down"):
        client_with(transport, []).get(URL)
    assert len(sent) == SETTINGS.retry.attempts


def test_the_last_error_is_raised_once_the_attempts_are_used() -> None:
    # Arrange
    transport, _ = answering(*(httpx.ConnectError("refused") for _ in range(3)))

    # Act / Assert
    with pytest.raises(httpx.ConnectError, match="refused"):
        client_with(transport, []).get(URL)


def test_the_wait_a_server_asks_for_is_kept_up_to_the_policys_most() -> None:
    # Arrange
    transport, _ = answering(
        httpx.Response(429, headers={"Retry-After": "7"}),
        httpx.Response(429, headers={"Retry-After": "600"}),
        httpx.Response(200),
    )
    waits: list[float] = []

    # Act
    client_with(transport, waits).get(URL)

    # Assert
    assert waits == [7, 60]


def test_a_redirect_is_followed_not_raised() -> None:
    # Arrange
    transport, sent = answering(
        httpx.Response(302, headers={"Location": "https://source.test/moved.csv"}),
        httpx.Response(200, content=b"id"),
    )

    # Act
    response = client_with(transport, []).get(URL)

    # Assert
    assert response.content == b"id"
    assert [str(request.url) for request in sent] == [URL, "https://source.test/moved.csv"]


def test_a_compressed_body_is_decoded_once() -> None:
    # Arrange
    transport, _ = answering(
        httpx.Response(200, content=gzip.compress(b"id"), headers={"Content-Encoding": "gzip"})
    )

    # Act
    response = client_with(transport, []).get(URL)

    # Assert
    assert response.content == b"id"


def test_a_response_is_read_and_closed_as_any_httpx_response() -> None:
    # Arrange
    transport, _ = answering(httpx.Response(200, content=b"id"))

    # Act
    response = client_with(transport, []).get(URL)

    # Assert
    assert response.is_closed
    assert response.elapsed >= timedelta(0)


def test_requests_name_the_pipeline_as_their_user_agent() -> None:
    # Arrange
    transport, sent = answering(httpx.Response(200))

    # Act
    client_with(transport, []).get(URL)

    # Assert
    assert sent[0].headers["User-Agent"] == "leads-test/1.0"


def test_a_spaced_client_spaces_its_retries_too() -> None:
    # Arrange
    transport, _ = answering(httpx.Response(503), httpx.Response(200))
    waits: list[float] = []
    spaced = HttpClientSettings(
        timeout_seconds=1,
        retry=RetryPolicy(attempts=2, backoff_seconds=0.25, max_wait_seconds=60),
        min_interval_seconds=1,
    )

    # Act
    client_with(transport, waits, spaced).get(URL)

    # Assert: the backoff, then the pacer's wait for the rest of the interval. The waits are
    # recorded, not slept, so the pacer finds almost none of its interval gone.
    backoff, paced_wait = waits
    assert backoff == 0.25
    assert 0.9 < paced_wait <= 1


@pytest.mark.parametrize(
    ("header", "seconds"),
    [
        ("30", 30),
        (" 30 ", 30),
        ("Sun, 18 Oct 2026 07:28:45 GMT", 45),
        ("0", None),
        ("Sun, 18 Oct 2026 07:27:00 GMT", None),
        ("soon", None),
        (None, None),
    ],
)
def test_retry_after_is_seconds_or_a_date_and_only_a_wait_to_come(
    header: str | None, seconds: float | None
) -> None:
    # Act / Assert
    assert retry_after_seconds(header, NOW) == seconds


def test_paced_calls_start_at_least_the_interval_apart() -> None:
    # Arrange
    clock = FakeClock()
    pacer = Pacer(1.0, clock=clock, wait=clock.wait)

    # Act
    pacer.wait_turn()
    clock.now += 0.25
    pacer.wait_turn()
    pacer.wait_turn()

    # Assert
    assert clock.waits == [0.75, 1.0]


def test_a_paced_call_after_a_long_pause_does_not_wait() -> None:
    # Arrange
    clock = FakeClock()
    pacer = Pacer(1.0, clock=clock, wait=clock.wait)

    # Act
    pacer.wait_turn()
    clock.now += 5
    pacer.wait_turn()

    # Assert
    assert clock.waits == []
