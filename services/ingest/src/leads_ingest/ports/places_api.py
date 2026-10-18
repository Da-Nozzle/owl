from collections.abc import Mapping
from typing import Protocol

from leads_ingest.ports.downloader import Download


class PlacesApi(Protocol):
    """Google's Places API (New), as far as the pipeline searches it. Every request is billed,
    at the SKU of the most expensive field its field mask names: a mask of place ids only costs
    nothing, one with ratings, opening hours or phone numbers is billed as Enterprise."""

    def search_text(self, request: Mapping[str, object], field_mask: str) -> Download:
        """One Text Search request (`places:searchText`) with its JSON body and field mask: one
        page of results as the API sent it, with the request it answered."""
        ...
