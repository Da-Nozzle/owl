import heapq
from dataclasses import dataclass
from datetime import UTC, datetime

from leads_ingest.parallel import expand_in_parallel
from leads_ingest.ports.places_api import PlacesApi
from leads_ingest.ports.search_areas import SearchArea, SearchMode
from leads_ingest.sources.google_places.file_names import require_distinct_query_names
from leads_ingest.sources.google_places.settings import GooglePlacesSettings
from leads_ingest.sources.google_places.text_search import (
    PLAN_FIELD_MASK,
    RESULT_CAP,
    has_next_page,
    page_places,
    search_pages,
    search_request,
)
from leads_ingest.sources.google_places.tiles import Tile

type SearchTarget = tuple[SearchArea, str, Tile]


@dataclass(frozen=True, slots=True)
class PlannedSearch:
    """One query over one tile of a search area, as the free plan found it."""

    area: SearchArea
    query: str
    tile: Tile
    pages: int
    place_ids: frozenset[str]


@dataclass(frozen=True, slots=True)
class AreaPrice:
    """What the free plan found in one search area, and what fetching it alone would pay for."""

    area: SearchArea
    free_requests: int
    places: int
    paid_requests: int


@dataclass(frozen=True, slots=True)
class SweepPlan:
    """Every search area planned for free and priced. `to_pay` are the searches the next fetch
    pays for: those holding every place found in the areas to fetch, each place paid for once."""

    searches: list[PlannedSearch]
    areas: list[AreaPrice]
    to_pay: list[PlannedSearch]
    max_paid_requests: int
    planned_at: datetime

    @property
    def paid_requests(self) -> int:
        return sum(search.pages for search in self.to_pay)

    @property
    def within_budget(self) -> bool:
        return self.paid_requests <= self.max_paid_requests

    @property
    def fetches_any_area(self) -> bool:
        return any(price.area.mode is SearchMode.FETCH for price in self.areas)


def plan_fetch(
    settings: GooglePlacesSettings, api: PlacesApi, areas: list[SearchArea]
) -> SweepPlan:
    searches = plan_sweep(settings, api, areas)
    return SweepPlan(
        searches=searches,
        areas=[
            AreaPrice(
                area=area,
                free_requests=sum(search.pages for search in in_area),
                places=len(set().union(*(search.place_ids for search in in_area))),
                paid_requests=sum(search.pages for search in searches_to_pay(in_area)),
            )
            for area in areas
            if (in_area := [search for search in searches if search.area == area])
        ],
        to_pay=searches_to_pay(
            [search for search in searches if search.area.mode is SearchMode.FETCH]
        ),
        max_paid_requests=settings.max_paid_requests,
        planned_at=datetime.now(UTC),
    )


def plan_sweep(
    settings: GooglePlacesSettings, api: PlacesApi, areas: list[SearchArea]
) -> list[PlannedSearch]:
    """Searches each query over each area's box with place ids only, then over the quarters of
    each tile too wide or capped. Sorted by area and query, each tile before its quarters (r, r0,
    r00, ..., r1), whatever order they were answered in, so the same answers are paid alike."""
    require_distinct_query_names(settings.queries)
    roots = [(area, query) for area in areas for query in settings.queries]

    def search(target: SearchTarget) -> tuple[PlannedSearch, list[SearchTarget]]:
        area, query, tile = target
        request = search_request(query, tile.box, settings)
        pages = list(search_pages(api, request, PLAN_FIELD_MASK))
        place_ids = [str(found["id"]) for page in pages for found in page_places(page)]
        # Over a wide tile a search finds only some of the places, even below the cap.
        wide = bool(place_ids) and tile.size_meters > settings.max_tile_meters
        capped = len(place_ids) >= RESULT_CAP or has_next_page(pages[-1])
        split = wide or (capped and tile.size_meters / 2 >= settings.min_tile_meters)
        planned = PlannedSearch(area, query, tile, len(pages), frozenset(place_ids))
        return planned, [(area, query, quarter) for quarter in tile.quarters()] if split else []

    searches = expand_in_parallel(
        search,
        [(area, query, Tile.whole(area.box)) for area, query in roots],
        settings.workers,
    )
    order = {root: index for index, root in enumerate(roots)}
    return sorted(searches, key=lambda search: (order[search.area, search.query], search.tile.path))


def searches_to_pay(plan: list[PlannedSearch]) -> list[PlannedSearch]:
    """The searches that together hold every place the plan found, chosen greedily by places not
    held yet per page, the first on a tie; in the plan's order."""
    candidates = [search for search in plan if search.place_ids]
    remaining = set().union(*(search.place_ids for search in candidates))
    heap = [
        (-len(search.place_ids) / search.pages, index) for index, search in enumerate(candidates)
    ]
    heapq.heapify(heap)
    chosen = set()
    while remaining:
        # A search's count only falls as others are chosen, so it is counted again only when it
        # comes to the top (lazy greedy).
        _, index = heapq.heappop(heap)
        search = candidates[index]
        entry = (-len(search.place_ids & remaining) / search.pages, index)
        if heap and entry > heap[0]:
            heapq.heappush(heap, entry)
            continue
        chosen.add(index)
        remaining -= search.place_ids
    return [search for index, search in enumerate(candidates) if index in chosen]
