from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from leads_ingest.ports.downloader import Download


@dataclass(frozen=True, slots=True)
class HttpPlacesApi:
    """The Places API (New) over HTTPS, as the project whose key it holds, through `client`
    (`build_http_client` with `google_places.http`, which waits out the per-minute quota), safe to
    call from several threads at once."""

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
