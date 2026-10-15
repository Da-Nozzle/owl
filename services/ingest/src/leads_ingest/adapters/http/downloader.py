from dataclasses import dataclass
from datetime import UTC, datetime

from dlt.sources.helpers import requests

from leads_ingest.ports.downloader import Download

TIMEOUT_SECONDS = 300


@dataclass(frozen=True, slots=True)
class HttpDownloader:
    """GETs a whole file; dlt's requests helper retries timeouts and 5xx with backoff."""

    user_agent: str

    def download(self, url: str) -> Download:
        response = requests.get(
            url, headers={"User-Agent": self.user_agent}, timeout=TIMEOUT_SECONDS
        )
        response.raise_for_status()
        return Download(
            url=url,
            body=response.content,
            fetched_at=datetime.now(UTC),
            last_modified=response.headers.get("Last-Modified"),
        )
