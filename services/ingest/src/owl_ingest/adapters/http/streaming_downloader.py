import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from owl_http.client import Pacer, build_streaming_http_client, retrying_whole_downloads
from owl_http.settings import HttpClientSettings
from owl_ingest.ports.downloader import OpenedDownload

# Decoded bytes handed to the writer at a time: few writes, little held.
CHUNK_BYTES = 1 << 20


@dataclass
class HttpStreamingDownloader:
    """Hands each body to its writer in chunks as it arrives, so no file is held in memory whole.
    A download that fails midway is retried from its start, as `settings` allows, each attempt
    starting at least `min_interval_seconds` after the last. Tests replace the network with
    `transport`."""

    user_agent: str
    settings: HttpClientSettings
    transport: httpx.BaseTransport | None = None
    sleep: Callable[[float], None] = time.sleep
    _pacer: Pacer | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        if self.settings.min_interval_seconds > 0:
            self._pacer = Pacer(self.settings.min_interval_seconds, wait=self.sleep)

    def stream[T](self, url: str, write: Callable[[OpenedDownload], T]) -> T:
        # Built per download, not with the definitions: a client loads the CA bundle (0.2 s).
        with build_streaming_http_client(
            self.user_agent, self.settings, transport=self.transport
        ) as client:
            retrying = retrying_whole_downloads(self.settings, sleep=self.sleep)
            return retrying(self._attempt, client, url, write)

    def _attempt[T](
        self, client: httpx.Client, url: str, write: Callable[[OpenedDownload], T]
    ) -> T:
        if self._pacer is not None:
            self._pacer.wait_turn()
        with client.stream("GET", url) as response:
            return write(
                OpenedDownload(
                    url=url,
                    fetched_at=datetime.now(UTC),
                    last_modified=response.headers.get("Last-Modified"),
                    etag=response.headers.get("ETag"),
                    # Decoded as Download.body is, so both record a version's bytes the same way.
                    chunks=response.iter_bytes(CHUNK_BYTES),
                )
            )
