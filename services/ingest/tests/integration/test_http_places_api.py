import json

import httpx
import pytest
from pytest_httpserver import HTTPServer

from owl_http.client import build_http_client
from owl_http.settings import load_http_client_settings
from owl_ingest.adapters.http.places_api import HttpPlacesApi
from owl_testing.config import build_config

# Answers are matched on path only (see test_http_downloader.py); what was sent is checked from
# the server's log.
REQUEST = {"textQuery": "kavinė", "pageSize": 20, "pageToken": "next"}
PAGE = {"places": [{"id": "ChIJVWXHjL6W3UYRs4Ob0shgvkY"}]}


def search(httpserver: HTTPServer, waits: list[float] | None = None) -> HttpPlacesApi:
    """The API at `httpserver`, recording its retries' waits in `waits` instead of waiting."""
    recorded = [] if waits is None else waits
    settings = load_http_client_settings(build_config(), "google_places.http")
    client = build_http_client("owl-test/1.0", settings, sleep=recorded.append)
    return HttpPlacesApi(
        client=client,
        api_key="key-123",
        search_text_url=httpserver.url_for("/v1/places:searchText"),
    )


def test_a_search_posts_its_body_with_the_key_and_field_mask(httpserver: HTTPServer) -> None:
    # Arrange
    httpserver.expect_oneshot_request("/v1/places:searchText").respond_with_json(PAGE)

    # Act
    search(httpserver).search_text(REQUEST, "places.id,nextPageToken")

    # Assert
    [(request, _)] = httpserver.log
    assert request.method == "POST"
    assert json.loads(request.data) == REQUEST
    assert request.headers["X-Goog-Api-Key"] == "key-123"
    assert request.headers["X-Goog-FieldMask"] == "places.id,nextPageToken"
    assert request.headers["User-Agent"] == "owl-test/1.0"


def test_a_page_is_kept_as_sent_with_the_request_it_answered_but_not_the_key(
    httpserver: HTTPServer,
) -> None:
    # Arrange
    body = json.dumps(PAGE, indent=2).encode()
    httpserver.expect_oneshot_request("/v1/places:searchText").respond_with_data(
        body, content_type="application/json"
    )
    api = search(httpserver)

    # Act
    download = api.search_text(REQUEST, "places.id,nextPageToken")

    # Assert
    assert (download.url, download.body) == (api.search_text_url, body)
    assert download.request == {"field_mask": "places.id,nextPageToken", "body": REQUEST}
    assert "key-123" not in json.dumps(download.request)


def test_a_refused_search_fails_with_googles_reason(httpserver: HTTPServer) -> None:
    # Arrange
    error = {"error": {"code": 400, "message": "Invalid field: places.foo", "status": "INVALID"}}
    httpserver.expect_oneshot_request("/v1/places:searchText").respond_with_json(error, status=400)

    # Act / Assert
    with pytest.raises(httpx.HTTPStatusError, match=r"(?s)400 .*Invalid field: places\.foo"):
        search(httpserver).search_text(REQUEST, "places.foo")


def test_a_search_over_the_quota_is_sent_again_after_the_wait_google_asks_for(
    httpserver: HTTPServer,
) -> None:
    # Arrange
    httpserver.expect_ordered_request("/v1/places:searchText").respond_with_json(
        {"error": {"code": 429, "status": "RESOURCE_EXHAUSTED"}},
        status=429,
        headers={"Retry-After": "1"},
    )
    httpserver.expect_ordered_request("/v1/places:searchText").respond_with_json(PAGE)
    waits: list[float] = []

    # Act
    download = search(httpserver, waits).search_text(REQUEST, "places.id,nextPageToken")

    # Assert
    assert json.loads(download.body) == PAGE
    assert len(httpserver.log) == 2
    assert waits == [1]
