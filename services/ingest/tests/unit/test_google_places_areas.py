import pytest

from leads_ingest.sources.google_places.areas import SearchArea, SearchMode, read_search_areas
from leads_ingest.sources.google_places.settings import Area
from leads_testing.ingest import FakeLakeReader

VILNIUS = {
    "municipality_code": 13,
    "municipality": "Vilniaus miesto sav.",
    "search_mode": "fetch",
    "south": 54.5689,
    "west": 25.0245,
    "north": 54.8324,
    "east": 25.4815,
}


def test_the_areas_with_a_search_mode_are_read_from_the_areas_model() -> None:
    # Arrange
    reader = FakeLakeReader([VILNIUS, {**VILNIUS, "municipality_code": 19, "search_mode": "price"}])

    # Act
    areas = read_search_areas(reader)

    # Assert
    box = Area(54.5689, 25.0245, 54.8324, 25.4815)
    assert areas == [
        SearchArea(13, "Vilniaus miesto sav.", SearchMode.FETCH, box),
        SearchArea(19, "Vilniaus miesto sav.", SearchMode.PRICE, box),
    ]
    [sql] = reader.queries
    assert "from lake.intermediate.int_google_search_areas" in sql
    assert "where search_mode is not null" in sql


def test_an_unknown_search_mode_is_refused() -> None:
    # Arrange
    reader = FakeLakeReader([{**VILNIUS, "search_mode": "scrape"}])

    # Act / Assert
    with pytest.raises(ValueError, match="scrape"):
        read_search_areas(reader)
