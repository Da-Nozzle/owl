import json
import time
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

import pytest

from leads_ingest.ports.downloader import Download
from leads_ingest.sources.google_places.areas import SearchArea, SearchMode
from leads_ingest.sources.google_places.settings import Area, GooglePlacesSettings
from leads_ingest.sources.google_places.text_search import (
    PLACE_FIELD_MASK,
    PLAN_FIELD_MASK,
    PLAN_FILE,
    AreaPrice,
    PlannedSearch,
    Tile,
    pay_for_plan,
    plan_fetch,
    plan_sweep,
    query_name,
    searches_to_pay,
)
from leads_testing.ingest import FakePlacesApi, InMemoryRawArchive, place

# About 2.2 km north to south and 2.6 km west to east.
AREA = Area(south=54.68, west=25.26, north=54.70, east=25.30)
VILNIUS = SearchArea(13, "Vilniaus miesto sav.", SearchMode.FETCH, AREA)
AREAS = [VILNIUS]
# Tiles are split only when capped, unless a test sets a smaller max_tile_meters.
SETTINGS = GooglePlacesSettings(
    search_text_url="https://places.test/v1/places:searchText",
    snapshot_schedule="0 6 21 * *",
    queries=("kavinė",),
    language_code="lt",
    region_code="LT",
    max_tile_meters=5_000,
    min_tile_meters=500,
    max_paid_requests=100,
    workers=4,
)
# 64 places in an 8 x 8 grid filling the south-west quarter (r0), none on an edge of its
# quarters, and one place in the north-east quarter (r3), inside r3's own north-east quarter.
# The whole area and r0 are capped, at the first 60 of the cluster; r0's quarters hold 16 each,
# and the last four of the cluster are in r03.
CLUSTER = [
    place(f"c{row}{column}", 54.68 + (row + 0.5) * 0.01 / 8, 25.26 + (column + 0.5) * 0.02 / 8)
    for row in range(8)
    for column in range(8)
]
LONE = place("lone", 54.697, 25.295)
DENSE = {"kavinė": [*CLUSTER, LONE]}


def ids(body: bytes) -> list[object]:
    return [found["id"] for found in json.loads(body).get("places", [])]


def masks(api: FakePlacesApi) -> list[str]:
    return [mask for _, mask in api.requests]


def sweep(
    snapshot: str,
    archive: InMemoryRawArchive,
    settings: GooglePlacesSettings,
    api: Any,
    areas: list[SearchArea],
) -> dict[str, Download]:
    """A fetch: plans the areas, then pays for the plan."""
    return pay_for_plan(snapshot, archive, settings, api, plan_fetch(settings, api, areas))


def pages(archived: Mapping[str, Download]) -> list[str]:
    """The paid pages' names, in order, without the plan."""
    return [name for name in archived if name != PLAN_FILE]


def test_a_tile_under_the_cap_is_searched_once_free_then_once_paid() -> None:
    # Arrange
    api = FakePlacesApi({"kavinė": [place("a", 54.69, 25.27), LONE]})
    archive = InMemoryRawArchive()

    # Act
    archived = sweep("2026-09", archive, SETTINGS, api, AREAS)

    # Assert
    assert pages(archived) == ["places-13-kavine-r-1.json"]
    assert masks(api) == [PLAN_FIELD_MASK, PLACE_FIELD_MASK]
    assert ids(archived["places-13-kavine-r-1.json"].body) == ["a", "lone"]
    assert set(archive.downloads) == {
        ("google_places", "2026-09", PLAN_FILE),
        ("google_places", "2026-09", "places-13-kavine-r-1.json"),
    }
    assert archive.snapshots("google_places") == {}


def test_a_paid_page_holds_the_fields_the_leads_need() -> None:
    # Arrange
    rated = place("a", 54.69, 25.27, rating=4.6, userRatingCount=6722, reviews=["not asked"])
    api = FakePlacesApi({"kavinė": [rated]})

    # Act
    archived = sweep("2026-09", InMemoryRawArchive(), SETTINGS, api, AREAS)

    # Assert
    [sent] = json.loads(archived["places-13-kavine-r-1.json"].body)["places"]
    assert sent == {key: value for key, value in rated.items() if key != "reviews"}


def test_the_plan_splits_a_capped_tile_until_no_tile_is_and_finds_the_empty_ones() -> None:
    # Arrange
    api = FakePlacesApi(DENSE)

    # Act
    plan = plan_sweep(SETTINGS, api, AREAS)

    # Assert
    assert [(search.tile.path, search.pages, len(search.place_ids)) for search in plan] == [
        ("r", 3, 60),
        ("r0", 3, 60),
        ("r00", 1, 16),
        ("r01", 1, 16),
        ("r02", 1, 16),
        ("r03", 1, 16),
        ("r1", 1, 0),
        ("r2", 1, 0),
        ("r3", 1, 1),
    ]
    assert set(masks(api)) == {PLAN_FIELD_MASK}


def test_the_plan_splits_a_wide_tile_with_places_even_under_the_cap() -> None:
    # Arrange: a search over a wide tile finds only some of its places, so it is not trusted.
    api = FakePlacesApi({"kavinė": [LONE]})
    settings = replace(SETTINGS, max_tile_meters=1_000)

    # Act
    plan = plan_sweep(settings, api, AREAS)

    # Assert: r and r3 are wider than 1 km and hold the place; the empty r0 to r2 are not split.
    assert [(search.tile.path, len(search.place_ids)) for search in plan] == [
        ("r", 1),
        ("r0", 0),
        ("r1", 0),
        ("r2", 0),
        ("r3", 1),
        ("r30", 0),
        ("r31", 0),
        ("r32", 0),
        ("r33", 1),
    ]


def test_the_paid_searches_hold_every_place_found_each_paid_for_once() -> None:
    # Arrange
    api = FakePlacesApi(DENSE)

    # Act
    archived = sweep("2026-09", InMemoryRawArchive(), SETTINGS, api, AREAS)

    # Assert: r's three pages hold 60 of the cluster, r03 the four others, r3 the lone place;
    # r0 and its other quarters add nothing.
    assert pages(archived) == [
        "places-13-kavine-r-1.json",
        "places-13-kavine-r-2.json",
        "places-13-kavine-r-3.json",
        "places-13-kavine-r03-1.json",
        "places-13-kavine-r3-1.json",
    ]
    found = {found for download in archived.values() for found in ids(download.body)}
    assert found == {entry["id"] for entry in DENSE["kavinė"]}
    assert masks(api).count(PLACE_FIELD_MASK) == len(pages(archived))


def test_a_place_several_queries_find_is_paid_for_once() -> None:
    # Arrange
    api = FakePlacesApi({"kavinė": [LONE], "cafe": [LONE, place("b", 54.69, 25.27)]})
    settings = replace(SETTINGS, queries=("kavinė", "cafe"))

    # Act
    archived = sweep("2026-09", InMemoryRawArchive(), settings, api, AREAS)

    # Assert
    assert pages(archived) == ["places-13-cafe-r-1.json"]


def test_the_cheapest_cover_is_chosen_by_new_places_per_page() -> None:
    # Arrange: the wide search's three pages hold all five places, the two small ones three and
    # two in a page each.
    def search(path: str, pages: int, *place_ids: str) -> PlannedSearch:
        return PlannedSearch(VILNIUS, "kavinė", Tile(path, AREA), pages, frozenset(place_ids))

    plan = [search("r", 3, "a", "b", "c", "d", "e"), search("r0", 1, "a", "b", "c")]
    plan += [search("r1", 1, "d", "e"), search("r2", 1)]

    # Act
    paid = searches_to_pay(plan)

    # Assert
    assert [search.tile.path for search in paid] == ["r0", "r1"]


def test_a_search_that_grew_since_the_plan_is_paid_for_its_planned_pages_only() -> None:
    # Arrange: the plan finds 15 places, a page; by the time it is paid for, the tile has 40.
    plan = FakePlacesApi({"kavinė": CLUSTER[:15]})
    paid = FakePlacesApi({"kavinė": CLUSTER[:40]})

    class Growing:
        def search_text(self, request: Mapping[str, Any], field_mask: str) -> Download:
            api = paid if field_mask == PLACE_FIELD_MASK else plan
            return api.search_text(request, field_mask)

    # Act
    archived = sweep("2026-09", InMemoryRawArchive(), SETTINGS, Growing(), AREAS)

    # Assert
    assert pages(archived) == ["places-13-kavine-r-1.json"]
    assert len(paid.requests) == 1


def test_a_sweep_over_its_budget_fails_without_paying_for_anything() -> None:
    # Arrange: the sweep of DENSE pays for 5 pages.
    api = FakePlacesApi(DENSE)
    archive = InMemoryRawArchive()
    settings = replace(SETTINGS, max_paid_requests=4)

    # Act / Assert
    with pytest.raises(ValueError, match="needs 5 paid requests"):
        sweep("2026-09", archive, settings, api, AREAS)
    assert set(masks(api)) == {PLAN_FIELD_MASK}
    assert archive.downloads == {}


def test_a_capped_tile_is_not_split_below_the_smallest_tile() -> None:
    # Arrange
    api = FakePlacesApi(DENSE)
    settings = replace(SETTINGS, min_tile_meters=2_000)

    # Act
    plan = plan_sweep(settings, api, AREAS)

    # Assert
    assert [search.tile.path for search in plan] == ["r"]


def test_a_later_page_repeats_the_search_with_the_previous_pages_token() -> None:
    # Arrange
    api = FakePlacesApi(DENSE)

    # Act
    archived = sweep("2026-09", InMemoryRawArchive(), SETTINGS, api, AREAS)

    # Assert
    assert archived["places-13-kavine-r-2.json"].request == {
        "field_mask": PLACE_FIELD_MASK,
        "body": {
            "textQuery": "kavinė",
            "pageSize": 20,
            "languageCode": "lt",
            "regionCode": "LT",
            "locationRestriction": {
                "rectangle": {
                    "low": {"latitude": 54.68, "longitude": 25.26},
                    "high": {"latitude": 54.70, "longitude": 25.30},
                }
            },
            "pageToken": "20",
        },
    }


def test_each_query_is_swept_over_the_whole_area() -> None:
    # Arrange
    api = FakePlacesApi({"kavinė": [LONE], "grožio salonas": [place("salon", 54.69, 25.27)]})
    settings = replace(SETTINGS, queries=("kavinė", "grožio salonas"))

    # Act
    archived = sweep("2026-09", InMemoryRawArchive(), settings, api, AREAS)

    # Assert
    assert pages(archived) == ["places-13-kavine-r-1.json", "places-13-grozio-salonas-r-1.json"]


def test_queries_written_alike_in_file_names_are_refused_before_any_request() -> None:
    # Arrange
    api = FakePlacesApi(DENSE)
    settings = replace(SETTINGS, queries=("kavinė", "Kavine"))

    # Act / Assert
    with pytest.raises(ValueError, match="not distinct"):
        sweep("2026-09", InMemoryRawArchive(), settings, api, AREAS)
    assert api.requests == []


def test_a_third_page_with_a_token_is_capped_and_no_fourth_page_is_asked_for() -> None:
    # Arrange: the API lists more than 60 places, so its third page has a token.
    api = FakePlacesApi(DENSE, cap=80)

    # Act
    plan = plan_sweep(SETTINGS, api, AREAS)

    # Assert: the whole area and r0 (the cluster) are split, with three pages each.
    assert [(search.tile.path, search.pages) for search in plan][:2] == [("r", 3), ("r0", 3)]
    assert all(request.get("pageToken") != "60" for request, _ in api.requests)


def test_a_short_third_page_with_a_token_is_capped_too() -> None:
    # Arrange: as once on 2026-10-17, the API drops two of a capped search's 60 candidates from
    # its third page, and still sends a token.
    dense = FakePlacesApi(DENSE)

    class DroppingTwo:
        def search_text(self, request: Mapping[str, Any], field_mask: str) -> Download:
            download = dense.search_text(request, field_mask)
            if request.get("pageToken") != "40":
                return download
            document = json.loads(download.body)
            document["places"] = document["places"][:-2]
            document["nextPageToken"] = "60"
            return replace(download, body=json.dumps(document).encode())

    # Act
    plan = plan_sweep(SETTINGS, DroppingTwo(), AREAS)

    # Assert: 58 places, yet both are split as capped, and the empty fourth page is not asked for.
    assert [(search.tile.path, len(search.place_ids)) for search in plan][:3] == [
        ("r", 58),
        ("r0", 58),
        ("r00", 16),
    ]
    assert all(request.get("pageToken") != "60" for request, _ in dense.requests)


@pytest.mark.parametrize(
    ("query", "name"),
    [("kavinė", "kavine"), ("grožio salonas", "grozio-salonas"), ("Car Repair", "car-repair")],
)
def test_a_query_is_written_in_ascii_in_file_names(query: str, name: str) -> None:
    # Act / Assert
    assert query_name(query) == name


def test_a_tiles_quarters_cover_it_and_share_their_edges() -> None:
    # Arrange
    tile = Tile.whole(AREA)

    # Act
    quarters = tile.quarters()

    # Assert
    assert [(quarter.path, quarter.area) for quarter in quarters] == [
        ("r0", Area(54.68, 25.26, 54.69, 25.28)),
        ("r1", Area(54.68, 25.28, 54.69, 25.30)),
        ("r2", Area(54.69, 25.26, 54.70, 25.28)),
        ("r3", Area(54.69, 25.28, 54.70, 25.30)),
    ]
    assert round(tile.size_meters) == 2_574


def test_a_priced_area_is_planned_but_only_the_fetched_ones_are_paid_for() -> None:
    # Arrange
    kaunas = SearchArea(19, "Kauno miesto sav.", SearchMode.PRICE, Area(54.82, 23.75, 54.96, 24.10))
    api = FakePlacesApi({"kavinė": [LONE, place("kaunas", 54.89, 23.9)]})

    # Act
    archived = sweep("2026-09", InMemoryRawArchive(), SETTINGS, api, [VILNIUS, kaunas])

    # Assert
    assert pages(archived) == ["places-13-kavine-r-1.json"]

    def boxes(mask: str) -> set[float]:
        return {
            request["locationRestriction"]["rectangle"]["low"]["latitude"]
            for request, sent in api.requests
            if sent == mask
        }

    assert {54.68, 54.82} <= boxes(PLAN_FIELD_MASK)
    assert boxes(PLACE_FIELD_MASK) == {54.68}


def test_a_scope_with_no_fetched_area_is_planned_but_refused_before_paying() -> None:
    # Arrange
    api = FakePlacesApi(DENSE)
    archive = InMemoryRawArchive()
    priced = replace(VILNIUS, mode=SearchMode.PRICE)

    # Act / Assert
    with pytest.raises(ValueError, match="No search area is to be fetched"):
        sweep("2026-09", archive, SETTINGS, api, [priced])
    assert set(masks(api)) == {PLAN_FIELD_MASK}
    assert archive.downloads == {}


def test_the_plan_prices_each_area_alone_and_the_fetched_areas_together_for_free() -> None:
    # Arrange: the district's box holds the city's, so the city's places are in both.
    district = SearchArea(
        41, "Vilniaus rajono sav.", SearchMode.PRICE, Area(54.67, 25.25, 54.71, 25.31)
    )
    edge = place("edge", 54.705, 25.305)
    api = FakePlacesApi({"kavinė": [*DENSE["kavinė"], edge]})

    # Act
    plan = plan_fetch(SETTINGS, api, [VILNIUS, district])

    # Assert: the city alone is DENSE's plan (13 free pages) and cover (5 paid); the district
    # finds the city's 65 places and the edge. Only the city is fetched.
    assert plan.areas == [
        AreaPrice(area=VILNIUS, free_requests=13, places=65, paid_requests=5),
        AreaPrice(area=district, free_requests=15, places=66, paid_requests=7),
    ]
    assert {search.area for search in plan.to_pay} == {VILNIUS}
    assert (plan.paid_requests, plan.max_paid_requests, plan.within_budget) == (5, 100, True)
    assert set(masks(api)) == {PLAN_FIELD_MASK}


def test_the_plan_is_in_the_same_order_whatever_order_its_searches_are_answered_in() -> None:
    # Arrange: the first query's searches are answered last, the larger tiles slowest.
    dense = FakePlacesApi({"kavinė": DENSE["kavinė"], "cafe": DENSE["kavinė"]})

    class SlowFirstQuery:
        def search_text(self, request: Mapping[str, Any], field_mask: str) -> Download:
            if request["textQuery"] == "kavinė":
                rectangle = request["locationRestriction"]["rectangle"]
                time.sleep(rectangle["high"]["latitude"] - rectangle["low"]["latitude"])
            return dense.search_text(request, field_mask)

    settings = replace(SETTINGS, queries=("kavinė", "cafe"))

    # Act
    plan = plan_sweep(settings, SlowFirstQuery(), AREAS)

    # Assert
    tiles = ["r", "r0", "r00", "r01", "r02", "r03", "r1", "r2", "r3"]
    assert [(search.query, search.tile.path) for search in plan] == [
        *(("kavinė", tile) for tile in tiles),
        *(("cafe", tile) for tile in tiles),
    ]


def test_a_payment_that_fails_starts_no_further_search() -> None:
    # Arrange: three searches to pay for, one at a time; the first paid request fails.
    dense = FakePlacesApi(DENSE)

    class FailingToPay:
        def search_text(self, request: Mapping[str, Any], field_mask: str) -> Download:
            download = dense.search_text(request, field_mask)
            if field_mask == PLACE_FIELD_MASK:
                raise ConnectionError("Google went away")
            return download

    settings = replace(SETTINGS, workers=1)

    # Act / Assert
    with pytest.raises(ConnectionError):
        sweep("2026-09", InMemoryRawArchive(), settings, FailingToPay(), AREAS)
    assert masks(dense).count(PLACE_FIELD_MASK) == 1


def test_the_snapshot_keeps_the_plan_it_was_paid_for() -> None:
    # Arrange: the lone place is found by both queries; "cafe"'s search is paid for.
    api = FakePlacesApi({"kavinė": [LONE], "cafe": [LONE, place("b", 54.69, 25.27)]})
    settings = replace(SETTINGS, queries=("kavinė", "cafe"))
    archive = InMemoryRawArchive()

    # Act
    archived = sweep("2026-09", archive, settings, api, AREAS)

    # Assert
    assert next(iter(archived)) == PLAN_FILE
    assert archive.downloads[("google_places", "2026-09", PLAN_FILE)] == archived[PLAN_FILE]
    plan = json.loads(archived[PLAN_FILE].body)
    assert plan["searches"] == [
        {
            "area": 13,
            "query": "kavinė",
            "tile": "r",
            "pages": 1,
            "paid": False,
            "place_ids": ["lone"],
        },
        {
            "area": 13,
            "query": "cafe",
            "tile": "r",
            "pages": 1,
            "paid": True,
            "place_ids": ["b", "lone"],
        },
    ]
    assert plan["max_paid_requests"] == 100
    assert archived[PLAN_FILE].request == {"field_mask": PLAN_FIELD_MASK}
