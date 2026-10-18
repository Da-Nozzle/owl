from collections.abc import Mapping
from typing import Protocol

from leads_ingest.ports.downloader import Download


class PlacesApi(Protocol):
    """Google's Places API (New). A request is billed at the SKU of the most expensive field its
    field mask names."""

    def search_text(self, request: Mapping[str, object], field_mask: str) -> Download:
        """One page of a Text Search, as the API sent it, with the request it answered."""
        ...
