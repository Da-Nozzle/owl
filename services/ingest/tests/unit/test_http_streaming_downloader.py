from collections.abc import Iterator

import httpx
import pytest

from owl_http.settings import HttpClientSettings, RetryPolicy
from owl_ingest.adapters.http.streaming_downloader import HttpStreamingDownloader
from owl_ingest.ports.downloader import OpenedDownload

URL = "https://source.test/Vieta/:format/csv"
SETTINGS = HttpClientSettings(
    timeout_seconds=1,
    retry=RetryPolicy(attempts=3, backoff_seconds=1, max_wait_seconds=60),
    min_interval_seconds=0,
)


class DroppedBody(httpx.SyncByteStream):
    """A body whose connection drops after its first bytes, as the portal's do on big exports."""

    def __iter__(self) -> Iterator[bytes]:
        yield b"_id,"
        raise httpx.RemoteProtocolError("peer closed connection without sending complete message")


def answering(*responses: httpx.Response) -> tuple[httpx.MockTransport, list[httpx.Request]]:
    sent: list[httpx.Request] = []
    remaining = iter(responses)

    def handle(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return next(remaining)

    return httpx.MockTransport(handle), sent


def downloader(transport: httpx.MockTransport, waits: list[float]) -> HttpStreamingDownloader:
    return HttpStreamingDownloader("owl-test/1.0", SETTINGS, transport, sleep=waits.append)


def test_a_body_dropped_midway_is_written_again_from_its_start() -> None:
    # Arrange
    transport, sent = answering(
        httpx.Response(200, stream=DroppedBody()),
        httpx.Response(200, content=b"_id,darbo_vietos_id\n", headers={"ETag": "e2"}),
    )
    written: list[list[bytes]] = []
    waits: list[float] = []

    def write(download: OpenedDownload) -> str | None:
        written.append([])
        for chunk in download.chunks:
            written[-1].append(chunk)
        return download.etag

    # Act
    etag = downloader(transport, waits).stream(URL, write)

    # Assert
    assert (etag, len(sent), waits) == ("e2", 2, [1])
    # The first attempt's bytes never filled a chunk before the drop; the second's are whole.
    assert written == [[], [b"_id,darbo_vietos_id\n"]]


def test_a_body_left_unread_is_not_downloaded() -> None:
    # Arrange
    transport, sent = answering(httpx.Response(200, stream=DroppedBody(), headers={"ETag": "e1"}))

    # Act
    etag = downloader(transport, []).stream(URL, lambda download: download.etag)

    # Assert
    assert (etag, len(sent)) == ("e1", 1)


def test_the_writers_own_error_fails_the_download_at_once() -> None:
    # Arrange
    transport, sent = answering(httpx.Response(200, content=b"<html>"))

    def write(download: OpenedDownload) -> None:
        raise ValueError("not a CSV export")

    # Act / Assert
    with pytest.raises(ValueError, match="not a CSV export"):
        downloader(transport, []).stream(URL, write)
    assert len(sent) == 1


def test_a_spaced_downloader_spaces_its_attempts() -> None:
    # Arrange
    transport, _ = answering(httpx.Response(503), httpx.Response(200))
    waits: list[float] = []
    spaced = HttpClientSettings(
        timeout_seconds=1,
        retry=RetryPolicy(attempts=2, backoff_seconds=0.25, max_wait_seconds=60),
        min_interval_seconds=1,
    )

    # Act
    HttpStreamingDownloader("owl-test/1.0", spaced, transport, sleep=waits.append).stream(
        URL, lambda download: None
    )

    # Assert: the backoff, then the pacer's wait for the rest of the interval (not slept).
    backoff, paced_wait = waits
    assert backoff == 0.25
    assert 0.9 < paced_wait <= 1
