import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from dlt.sources.helpers import requests

from leads_ingest.ports.downloader import Download, Downloader

# How long a server may send nothing (to connect, or between two reads) before the request is
# given up and retried. Files get long; an API answers small requests in under a second, so one
# that has sent nothing for 30 s has hung, as get.data.gov.lt's changelog did for a few of every
# 50 requests on 2026-10-17, and is better retried at once.
TIMEOUT_SECONDS = 300
API_TIMEOUT_SECONDS = 30


@dataclass(frozen=True, slots=True)
class HttpDownloader:
    """GETs a whole file; dlt's requests helper retries timeouts and 5xx with backoff."""

    user_agent: str
    timeout_seconds: float = TIMEOUT_SECONDS

    def download(self, url: str) -> Download:
        response = requests.get(
            url, headers={"User-Agent": self.user_agent}, timeout=self.timeout_seconds
        )
        response.raise_for_status()
        return Download(
            url=url,
            body=response.content,
            fetched_at=datetime.now(UTC),
            last_modified=response.headers.get("Last-Modified"),
            etag=response.headers.get("ETag"),
        )


@dataclass
class ThrottledDownloader:
    """A Downloader that starts a request no sooner than `interval_seconds` after the last one
    started, to stay under a server's rate limit when it is read in many small requests."""

    downloader: Downloader
    interval_seconds: float
    clock: Callable[[], float] = time.monotonic
    wait: Callable[[float], None] = time.sleep
    _last_start: float | None = field(default=None, init=False)

    def download(self, url: str) -> Download:
        if self._last_start is not None:
            remaining = self._last_start + self.interval_seconds - self.clock()
            if remaining > 0:
                self.wait(remaining)
        self._last_start = self.clock()
        return self.downloader.download(url)
