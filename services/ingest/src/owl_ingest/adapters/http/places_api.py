from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from owl_ingest.ports.downloader import Download


@dataclass(frozen=True, slots=True)
class HttpPlacesApi:
    """Safe to call from several threads at once; `client` waits out the per-minute quota."""

    client: httpx.Client = field(compare=False)
    api_key: str = field(repr=False)
    search_text_url: str

    def search_text(self, request: Mapping[str, object], field_mask: str) -> Download:
        response = self.client.post(
            self.search_text_url,
            json=dict(request),
            headers={"X-Goog-Api-Key": self.api_key, "X-Goog-FieldMask": field_mask},
        )
        return Download(
            url=self.search_text_url,
            body=response.content,
            fetched_at=datetime.now(UTC),
            last_modified=None,
            request={"field_mask": field_mask, "body": dict(request)},
        )
