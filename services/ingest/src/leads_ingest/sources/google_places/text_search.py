"""Google's Places API (New) Text Search, swept over search areas (`areas.py`: municipalities,
each a bounding box), each area on its own. A search returns at most 60
places, the most relevant first, in pages of up to 20, and over a large tile it finds only some
of the places that match even below that cap: on 2026-10-17 a 1.8 km tile over the Old Town found
13 cafés, where smaller tiles found hundreds. So each area is split into tiles, each searched: a
tile with places is split into quarters while it is wider than `max_tile_meters`, or while its
search is capped and its quarters are at least `min_tile_meters` wide. Results stay ranked by
relevance, not exhaustive: a tile's search finds a few places its quarters' do not, and the other
way round, so every tile's places count.

Each page is billed at the SKU of the most expensive field asked for, and a page of place ids
only is free. So a fetch plans its sweep with ids only, which tells it the places every search of
every query finds, then pays, for the fields the leads need, only for the searches that together
hold all of those places, picked greedily by new places per page: a place found by several
queries, by a tile and its quarters, or in two areas whose boxes overlap, is paid for once. The
same plan prices the areas, each alone and the fetched ones together, for nothing (`plan_fetch`),
and a fetch pays for that plan (`pay_for_plan`), as many pages of each search as the plan had, so
it knows what it will pay before it pays anything. Both send `workers` searches at once, and
neither counts Google's per-minute quota: the API client waits out a request over it. A search
asked again gives the same places whatever fields it asks for, but for a few at the margin: on
2026-10-17, a plan of the Old Town made ten minutes after a fetch found 800 places, 773 of them
among the 803 the fetch had paid for, and one of the fetch's searches had grown by a page between
its plan and its payment."""

import heapq
import json
import math
import re
import unicodedata
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Self

from leads_ingest.ports.downloader import Download
from leads_ingest.ports.places_api import PlacesApi
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.sources.google_places.areas import SearchArea, SearchMode
from leads_ingest.sources.google_places.settings import Area, GooglePlacesSettings

SOURCE_NAME = "google_places"
# A sweep is made once a month, so a snapshot is keyed by its month, which its dbt models read
# from the archive path (_snapshot_month).
SNAPSHOT_KEY_FORMAT = "%Y-%m"
# The API's documented page size and cap ("may change"). No page past the cap is asked for: a third
# page may still carry a token, after the API dropped some of its 60 candidates (20, 20 and 18
# places, then an empty fourth page, in one of 721 "restaurant" searches on 2026-10-17), and such
# a search is capped too. Were the cap raised, more tiles would count as capped and be split.
PAGE_SIZE = 20
RESULT_CAP = 60
MAX_PAGES = RESULT_CAP // PAGE_SIZE
# Free (Text Search Essentials IDs Only).
PLAN_FIELD_MASK = "places.id,nextPageToken"
# The plan a snapshot's pages were paid for, in the snapshot beside them (`plan_download`).
PLAN_FILE = "plan.json"
# What the leads need of a place, billed as Text Search Enterprise for its rating, review count,
# opening hours, website and phone; the other fields are Pro and cost nothing more beside them.
# No Atmosphere field (reviews, serves_*, ...), which is billed higher still.
PLACE_FIELD_MASK = ",".join(
    (
        "places.id",
        "places.displayName",
        "places.formattedAddress",
        "places.addressComponents",
        "places.location",
        "places.types",
        "places.primaryType",
        "places.businessStatus",
        "places.googleMapsUri",
        "places.rating",
        "places.userRatingCount",
        "places.regularOpeningHours",
        "places.websiteUri",
        "places.nationalPhoneNumber",
        "nextPageToken",
    )
)
# A sweep's free plan is thousands of requests (7,690 for ten queries over Vilnius, 24 minutes
# sent one at a time, on 2026-10-17), and longer with every area; with `workers` at once it takes
# a fraction of that while the per-minute quota allows, then waits on it. It may take three hours.
FETCH_MAX_RUNTIME_SECONDS = 3 * 60 * 60
# Text Search Enterprise, 2026-10-17: free for the first 1,000 requests of a month on the billing
# account, then USD 35 per 1,000.
FREE_PAID_REQUESTS_A_MONTH = 1_000
USD_PER_PAID_REQUEST = 35 / 1_000
METRES_PER_DEGREE = 111_320
# Coordinates as sent: about a centimetre, and the same for the edge two quarters share.
COORDINATE_DECIMALS = 7


@dataclass(frozen=True, slots=True)
class Tile:
    """A rectangle of the swept area, named by its path from the whole area, `r`: each split
    appends the quarter's digit, 0 south-west, 1 south-east, 2 north-west, 3 north-east."""

    path: str
    area: Area

    @classmethod
    def whole(cls, area: Area) -> Self:
        return cls("r", area)

    @property
    def size_meters(self) -> float:
        """The longer of its sides."""
        area = self.area
        middle = math.radians((area.south + area.north) / 2)
        height = (area.north - area.south) * METRES_PER_DEGREE
        width = (area.east - area.west) * METRES_PER_DEGREE * math.cos(middle)
        return max(height, width)

    def quarters(self) -> list[Self]:
        area = self.area
        latitude, longitude = (area.south + area.north) / 2, (area.west + area.east) / 2
        corners = (
            Area(area.south, area.west, latitude, longitude),
            Area(area.south, longitude, latitude, area.east),
            Area(latitude, area.west, area.north, longitude),
            Area(latitude, longitude, area.north, area.east),
        )
        return [type(self)(f"{self.path}{index}", corner) for index, corner in enumerate(corners)]

    def restriction(self) -> dict[str, object]:
        """The tile as a search's `locationRestriction`; places on its edge are inside."""
        area = self.area

        def point(latitude: float, longitude: float) -> dict[str, float]:
            return {
                "latitude": round(latitude, COORDINATE_DECIMALS),
                "longitude": round(longitude, COORDINATE_DECIMALS),
            }

        return {
            "rectangle": {
                "low": point(area.south, area.west),
                "high": point(area.north, area.east),
            }
        }


@dataclass(frozen=True, slots=True)
class PlannedSearch:
    """One query over one tile of a search area, as the free plan found it: its pages and the
    places on them."""

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
    """Every area with a search mode, planned for free (`plan_sweep`) and priced: each area alone,
    and the next fetch, of the fetched areas, whose searches are those holding every place found
    in them (`searches_to_pay`), a place in two of them paid for once."""

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
    def fetches(self) -> bool:
        """Whether any area is to be fetched."""
        return any(price.area.mode is SearchMode.FETCH for price in self.areas)


def plan_fetch(
    settings: GooglePlacesSettings, api: PlacesApi, areas: list[SearchArea]
) -> SweepPlan:
    """Plans every area for free and prices it: what fetching it alone would pay for, and what
    the next fetch, of the fetched areas, will. Pays for nothing."""
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


def pay_for_plan(
    snapshot: str,
    archive: RawArchive,
    settings: GooglePlacesSettings,
    api: PlacesApi,
    plan: SweepPlan,
) -> dict[str, Download]:
    """Fetches the plan's searches (`to_pay`) into the archive with the fields the leads need, as
    many pages of each as the plan had, `workers` searches at once, one file per page as the API
    sent it, named after its area, query, tile and page; each file's sidecar keeps the request it
    answered. The plan itself goes first, as `plan.json`. Fails before paying for anything if no
    area is to be fetched, or if the plan's pages are more than `max_paid_requests`. Returns each
    file's download by name, the plan's first, then the pages in the plan's order; completing the
    snapshot is the caller's."""
    if not plan.fetches:
        raise ValueError(
            "No search area is to be fetched: give one the mode fetch in the "
            "google_search_scope seed"
        )
    if not plan.within_budget:
        raise ValueError(
            f"The sweep needs {plan.paid_requests} paid requests ({len(plan.to_pay)} searches), "
            f"more than google_places.max_paid_requests ({plan.max_paid_requests}); nothing "
            f"was paid"
        )
    archived = {PLAN_FILE: plan_download(plan, settings)}
    archive.save(SOURCE_NAME, snapshot, PLAN_FILE, archived[PLAN_FILE])

    def pay(search: PlannedSearch) -> list[Download]:
        request = search_request(search.query, search.tile, settings)
        return list(search_pages(api, request, PLACE_FIELD_MASK, search.pages))

    paid = in_parallel(pay, plan.to_pay, settings.workers)
    for search, downloads in zip(plan.to_pay, paid, strict=True):
        for page, download in enumerate(downloads, start=1):
            name = page_name(search.area, search.query, search.tile, page)
            archive.save(SOURCE_NAME, snapshot, name, download)
            archived[name] = download
    return archived


def plan_download(plan: SweepPlan, settings: GooglePlacesSettings) -> Download:
    """The plan as its snapshot keeps it: every search it made, by area, query and tile, with its
    pages, the places it found and whether it was paid for. A place's paid page names one search
    that found it; the plan names every one."""
    paid = set(plan.to_pay)
    document = {
        "planned_at": plan.planned_at.isoformat(),
        "max_paid_requests": plan.max_paid_requests,
        "searches": [
            {
                "area": search.area.code,
                "query": search.query,
                "tile": search.tile.path,
                "pages": search.pages,
                "paid": search in paid,
                "place_ids": sorted(search.place_ids),
            }
            for search in plan.searches
        ],
    }
    return Download(
        url=settings.search_text_url,
        body=json.dumps(document, ensure_ascii=False).encode(),
        fetched_at=plan.planned_at,
        last_modified=None,
        request={"field_mask": PLAN_FIELD_MASK},
    )


def plan_sweep(
    settings: GooglePlacesSettings, api: PlacesApi, areas: list[SearchArea]
) -> list[PlannedSearch]:
    """Every tile each query is searched over in each area, found with place ids only: the
    area's box, and the quarters of each tile with places that is wider than `max_tile_meters`,
    or whose search is capped while its quarters are at least `min_tile_meters` wide. `workers`
    searches at once, a tile's quarters as soon as it is searched. By area and query as given,
    each tile before its quarters (r, r0, r00, ..., r1), whatever order they were answered in,
    so that the same answers are paid for the same way (`searches_to_pay`)."""
    require_distinct_names(settings.queries)
    roots = [(area, query) for area in areas for query in settings.queries]

    def search(area: SearchArea, query: str, tile: Tile) -> tuple[PlannedSearch, list[Tile]]:
        request = search_request(query, tile, settings)
        pages = list(search_pages(api, request, PLAN_FIELD_MASK))
        place_ids = [str(found["id"]) for page in pages for found in page_places(page)]
        wide = bool(place_ids) and tile.size_meters > settings.max_tile_meters
        capped = len(place_ids) >= RESULT_CAP or has_next_page(pages[-1])
        split = wide or (capped and tile.size_meters / 2 >= settings.min_tile_meters)
        planned = PlannedSearch(area, query, tile, len(pages), frozenset(place_ids))
        return planned, tile.quarters() if split else []

    searches = []
    with ThreadPoolExecutor(settings.workers) as pool:
        pending: set[Future[tuple[PlannedSearch, list[Tile]]]] = {
            pool.submit(search, area, query, Tile.whole(area.area)) for area, query in roots
        }
        try:
            while pending:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    planned, quarters = future.result()
                    searches.append(planned)
                    pending |= {
                        pool.submit(search, planned.area, planned.query, quarter)
                        for quarter in quarters
                    }
        except BaseException:
            pool.shutdown(wait=False, cancel_futures=True)
            raise
    order = {root: index for index, root in enumerate(roots)}
    return sorted(searches, key=lambda search: (order[search.area, search.query], search.tile.path))


def in_parallel[T, R](function: Callable[[T], R], items: Sequence[T], workers: int) -> Iterator[R]:
    """`function` of each item, `workers` at once, in the items' order. Once one fails, or the
    caller stops reading, those not yet started are not."""
    with ThreadPoolExecutor(workers) as pool:
        futures = [pool.submit(function, item) for item in items]
        try:
            for future in futures:
                yield future.result()
        finally:
            for future in futures:
                future.cancel()


def searches_to_pay(plan: list[PlannedSearch]) -> list[PlannedSearch]:
    """The searches of the plan that together hold every place it found, chosen greedily: each
    time the one with the most places not held yet per page, the first of the plan on a tie. In
    the plan's order. Paying for every tile instead cost 538 pages for one query's 1,698 places on
    2026-10-17; these, 244. A search's count only falls as others are chosen, so the counts are
    kept in a heap and one is counted again only when it comes to the top (lazy greedy)."""
    candidates = [search for search in plan if search.place_ids]
    remaining = set().union(*(search.place_ids for search in candidates))
    heap = [
        (-len(search.place_ids) / search.pages, index) for index, search in enumerate(candidates)
    ]
    heapq.heapify(heap)
    chosen = set()
    while remaining:
        _, index = heapq.heappop(heap)
        search = candidates[index]
        entry = (-len(search.place_ids & remaining) / search.pages, index)
        if heap and entry > heap[0]:
            heapq.heappush(heap, entry)
            continue
        chosen.add(index)
        remaining -= search.place_ids
    return [search for index, search in enumerate(candidates) if index in chosen]


def search_request(query: str, tile: Tile, settings: GooglePlacesSettings) -> dict[str, object]:
    return {
        "textQuery": query,
        "pageSize": PAGE_SIZE,
        "languageCode": settings.language_code,
        "regionCode": settings.region_code,
        "locationRestriction": tile.restriction(),
    }


def search_pages(
    api: PlacesApi, request: Mapping[str, object], field_mask: str, pages: int = MAX_PAGES
) -> Iterator[Download]:
    """A search's pages, following each page's token to the next, up to `pages` of them (the
    cap's, unless fewer are asked for); a page of a later request repeats every parameter, as the
    API requires."""
    page_request = dict(request)
    for page in range(1, min(pages, MAX_PAGES) + 1):
        download = api.search_text(page_request, field_mask)
        yield download
        if not has_next_page(download) or page == pages:
            return
        page_request = {**request, "pageToken": page_document(download)["nextPageToken"]}


def has_next_page(download: Download) -> bool:
    return bool(page_document(download).get("nextPageToken"))


def page_document(download: Download) -> Mapping[str, object]:
    """A page as the API sends it: `{}` when nothing was found."""
    document = json.loads(download.body)
    if not isinstance(document, Mapping):
        raise ValueError(f"Not a Text Search page: {download.body[:200]!r}")
    return document


def page_places(download: Download) -> list[object]:
    places = page_document(download).get("places", [])
    if not isinstance(places, list):
        raise ValueError(f"Not a Text Search page: {download.body[:200]!r}")
    return places


def page_name(area: SearchArea, query: str, tile: Tile, page: int) -> str:
    """`places-<area code>-<query>-<tile>-<page>.json`: each area's tiles are named from its own
    box, `r`."""
    return f"places-{area.code}-{query_name(query)}-{tile.path}-{page}.json"


def query_name(query: str) -> str:
    """A query as it is written in file names: ASCII, lower case, words joined by `-`
    (`grožio salonas` is `grozio-salonas`)."""
    folded = unicodedata.normalize("NFKD", query).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", folded.lower()).strip("-")


def require_distinct_names(queries: tuple[str, ...]) -> None:
    """Two queries written the same in file names would overwrite each other's pages."""
    names = [query_name(query) for query in queries]
    if "" in names or len(set(names)) != len(names):
        raise ValueError(f"Queries {list(queries)} are not distinct in file names: {names}")
