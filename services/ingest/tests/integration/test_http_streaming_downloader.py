import httpx
import pytest
from pytest_httpserver import HTTPServer

from owl_http.settings import load_http_client_settings
from owl_ingest.adapters.http.streaming_downloader import CHUNK_BYTES, HttpStreamingDownloader
from owl_ingest.ports.downloader import OpenedDownload
from owl_testing.config import build_config

# Answers match on path only, as an unexpected request gets a 500, retried with backoff, which
# would fail slowly. Headers are checked from the server's log instead.


def downloader() -> HttpStreamingDownloader:
    settings = load_http_client_settings(build_config(), "jar.http")
    return HttpStreamingDownloader("owl-test/1.0", settings, sleep=lambda _: None)


def test_a_body_is_handed_over_in_chunks_with_its_version(httpserver: HTTPServer) -> None:
    # Arrange
    body = b"x" * (3 * CHUNK_BYTES + 5)
    httpserver.expect_oneshot_request("/Vieta/:format/csv").respond_with_data(
        body, headers={"ETag": "18acf9bb", "Last-Modified": "Sun, 18 Oct 2026 02:40:18 GMT"}
    )
    url = httpserver.url_for("/Vieta/:format/csv")

    def sizes(download: OpenedDownload) -> tuple[object, ...]:
        chunks = [len(chunk) for chunk in download.chunks]
        return download.url, download.etag, download.last_modified, chunks

    # Act
    handed = downloader().stream(url, sizes)

    # Assert
    chunks = [CHUNK_BYTES, CHUNK_BYTES, CHUNK_BYTES, 5]
    assert handed == (url, "18acf9bb", "Sun, 18 Oct 2026 02:40:18 GMT", chunks)
    [(request, _)] = httpserver.log
    assert request.headers["User-Agent"] == "owl-test/1.0"


def test_a_missing_file_fails_the_download(httpserver: HTTPServer) -> None:
    # Arrange
    httpserver.expect_oneshot_request("/missing").respond_with_data(b"", status=404)

    # Act / Assert
    with pytest.raises(httpx.HTTPStatusError, match="404"):
        downloader().stream(httpserver.url_for("/missing"), lambda download: None)
