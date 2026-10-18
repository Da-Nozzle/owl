from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

from leads_ingest.ports.downloader import Download


@dataclass(frozen=True, slots=True)
class HttpDownloader:
    client: httpx.Client

    def download(self, url: str) -> Download:
        response = self.client.get(url)
        return Download(
            url=url,
            body=response.content,
            fetched_at=datetime.now(UTC),
            last_modified=response.headers.get("Last-Modified"),
            etag=response.headers.get("ETag"),
        )
