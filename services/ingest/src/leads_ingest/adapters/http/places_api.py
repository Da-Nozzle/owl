from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from dlt.sources.helpers import requests
from dlt.sources.helpers.requests import Client

from leads_ingest.adapters.http.downloader import API_TIMEOUT_SECONDS
from leads_ingest.ports.downloader import Download

# The longest part of an error response kept in the exception: Google explains a refused request
# (a field it does not know, a key not allowed the API) in the body, not the status line.
ERROR_DETAIL_CHARACTERS = 500
# A sweep sends its searches without counting the per-minute quota (google_places.workers), so a
# request over it is the usual case, not the exception: each is sent again after 2, 4, 8, 16 and
# 32 seconds, then every minute, ten times in all, about five minutes, or after the wait Google's
# Retry-After asks for. dlt's default, five times in about 15 seconds, would fail a sweep inside
# one minute of quota. Timeouts and 5xx are retried the same way.
RETRY_ATTEMPTS = 10
RETRY_BACKOFF_SECONDS = 2
RETRY_MAX_WAIT_SECONDS = 60
# Connections kept open at once, at least as many as a sweep has requests in flight.
MAX_CONNECTIONS = 64


def quota_waiting_client() -> Client:
    """dlt's requests client (a session per thread, so a sweep's threads can share it), retrying
    as RETRY_ATTEMPTS says."""
    return Client(
        request_max_attempts=RETRY_ATTEMPTS,
        request_backoff_factor=RETRY_BACKOFF_SECONDS,
        request_max_retry_delay=RETRY_MAX_WAIT_SECONDS,
        max_connections=MAX_CONNECTIONS,
    )


@dataclass(frozen=True, slots=True)
class HttpPlacesApi:
    """The Places API (New) over HTTPS, as the project whose key it holds, safe to call from
    several threads at once. A request answered 429 (over the per-minute quota), 5xx or not at
    all is waited for and sent again, for about five minutes (RETRY_ATTEMPTS). Google's pricing
    does not say whether it bills a request it answers with an error; at worst a retry costs one
    request more."""

    api_key: str
    user_agent: str
    search_text_url: str
    timeout_seconds: float = API_TIMEOUT_SECONDS
    client: Client = field(default_factory=quota_waiting_client, compare=False, repr=False)

    def search_text(self, request: Mapping[str, object], field_mask: str) -> Download:
        try:
            response = self.client.post(
                self.search_text_url,
                json=dict(request),
                headers={
                    "X-Goog-Api-Key": self.api_key,
                    "X-Goog-FieldMask": field_mask,
                    "User-Agent": self.user_agent,
                },
                timeout=self.timeout_seconds,
            )
        except requests.HTTPError as error:
            # dlt's helper raises for an error status, with the status line only.
            body = error.response.text if error.response is not None else ""
            raise requests.HTTPError(
                f"{error}: {body[:ERROR_DETAIL_CHARACTERS]}", response=error.response
            ) from error
        return Download(
            url=self.search_text_url,
            body=response.content,
            fetched_at=datetime.now(UTC),
            last_modified=None,
            request={"field_mask": field_mask, "body": dict(request)},
        )
