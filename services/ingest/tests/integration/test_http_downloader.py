import pytest
from dlt.sources.helpers.requests import HTTPError
from pytest_httpserver import HTTPServer

from leads_ingest.adapters.http.downloader import HttpDownloader

# Answers are matched on path only: a request the server does not expect gets a 500, which the
# downloader retries with backoff, so a header mismatch would fail slowly. Headers are checked
# from the server's log instead.


def test_a_download_is_the_files_bytes_and_its_version(httpserver: HTTPServer) -> None:
    # Arrange
    httpserver.expect_oneshot_request("/2026/monthly-2026.csv.zip").respond_with_data(
        b"PK\x03\x04",
        headers={"Last-Modified": "Tue, 29 Sep 2026 11:23:05 GMT", "ETag": "bc4ad5d3"},
    )
    url = httpserver.url_for("/2026/monthly-2026.csv.zip")

    # Act
    download = HttpDownloader("leads-test/1.0").download(url)

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
    HttpDownloader("leads-test/1.0").download(httpserver.url_for("/file.zip"))

    # Assert
    [(request, _)] = httpserver.log
    assert request.headers["User-Agent"] == "leads-test/1.0"


def test_a_file_served_without_a_version_has_none(httpserver: HTTPServer) -> None:
    # Arrange
    httpserver.expect_oneshot_request("/file.zip").respond_with_data(b"PK")

    # Act
    download = HttpDownloader("leads-test/1.0").download(httpserver.url_for("/file.zip"))

    # Assert
    assert (download.last_modified, download.etag) == (None, None)


def test_a_missing_file_fails_the_download(httpserver: HTTPServer) -> None:
    # Arrange
    httpserver.expect_oneshot_request("/missing.zip").respond_with_data(b"", status=404)

    # Act / Assert
    with pytest.raises(HTTPError, match="404"):
        HttpDownloader("leads-test/1.0").download(httpserver.url_for("/missing.zip"))
