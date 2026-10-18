import httpx
import pytest
from pytest_httpserver import HTTPServer

from leads_http.client import build_http_client
from leads_http.settings import load_http_client_settings
from leads_ingest.adapters.http.downloader import HttpDownloader
from leads_testing.config import build_config

# Answers match on path only, as an unexpected request gets a 500, retried with backoff, which
# would fail slowly. Headers are checked from the server's log instead.


def downloader() -> HttpDownloader:
    settings = load_http_client_settings(build_config(), "sodra.http")
    return HttpDownloader(build_http_client("leads-test/1.0", settings))


def test_a_download_is_the_files_bytes_and_its_version(httpserver: HTTPServer) -> None:
    # Arrange
    httpserver.expect_oneshot_request("/2026/monthly-2026.csv.zip").respond_with_data(
        b"PK\x03\x04",
        headers={"Last-Modified": "Tue, 29 Sep 2026 11:23:05 GMT", "ETag": "bc4ad5d3"},
    )
    url = httpserver.url_for("/2026/monthly-2026.csv.zip")

    # Act
    download = downloader().download(url)

    # Assert
    assert (download.url, download.body, download.last_modified, download.etag) == (
        url,
        b"PK\x03\x04",
        "Tue, 29 Sep 2026 11:23:05 GMT",
        "bc4ad5d3",
    )


def test_a_download_names_the_pipeline_as_its_user_agent(httpserver: HTTPServer) -> None:
    # Arrange
    httpserver.expect_oneshot_request("/file.zip").respond_with_data(b"PK")

    # Act
    downloader().download(httpserver.url_for("/file.zip"))

    # Assert
    [(request, _)] = httpserver.log
    assert request.headers["User-Agent"] == "leads-test/1.0"


def test_a_file_served_without_a_version_has_none(httpserver: HTTPServer) -> None:
    # Arrange
    httpserver.expect_oneshot_request("/file.zip").respond_with_data(b"PK")

    # Act
    download = downloader().download(httpserver.url_for("/file.zip"))

    # Assert
    assert (download.last_modified, download.etag) == (None, None)


def test_a_missing_file_fails_the_download(httpserver: HTTPServer) -> None:
    # Arrange
    httpserver.expect_oneshot_request("/missing.zip").respond_with_data(b"", status=404)

    # Act / Assert
    with pytest.raises(httpx.HTTPStatusError, match="404"):
        downloader().download(httpserver.url_for("/missing.zip"))


def test_a_moved_file_is_downloaded_from_where_it_moved(httpserver: HTTPServer) -> None:
    # Arrange
    httpserver.expect_oneshot_request("/old.zip").respond_with_data(
        b"", status=302, headers={"Location": httpserver.url_for("/new.zip")}
    )
    httpserver.expect_oneshot_request("/new.zip").respond_with_data(b"PK")
    url = httpserver.url_for("/old.zip")

    # Act
    download = downloader().download(url)

    # Assert
    assert (download.url, download.body) == (url, b"PK")
