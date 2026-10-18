import pytest

from leads_ingest.sources.google_places.settings import (
    Area,
    GooglePlacesSettings,
    load_google_places_settings,
)
from leads_testing.config import build_config


def test_google_places_settings_are_read_from_config() -> None:
    # Arrange
    config = build_config(
        {
            "google_places.search_text_url": "https://places.test/search",
            "google_places.snapshot_schedule": "0 6 21 * *",
            "google_places.queries": ["kavinė", "car repair"],
            "google_places.language_code": "lt",
            "google_places.region_code": "LT",
            "google_places.max_tile_meters": 1000,
            "google_places.min_tile_meters": 100,
            "google_places.max_paid_requests": 1500,
            "google_places.workers": 16,
        }
    )

    # Act
    settings = load_google_places_settings(config)

    # Assert
    assert settings == GooglePlacesSettings(
        search_text_url="https://places.test/search",
        snapshot_schedule="0 6 21 * *",
        queries=("kavinė", "car repair"),
        language_code="lt",
        region_code="LT",
        max_tile_meters=1000.0,
        min_tile_meters=100.0,
        max_paid_requests=1500,
        workers=16,
    )


def test_loading_the_settings_reads_no_key() -> None:
    # Arrange: a secret store without the key, which fails any read of it. The key is read by
    # the live fetch, when it first sends a request.
    config = build_config(secrets={})

    # Act
    settings = load_google_places_settings(config)

    # Assert
    assert settings.queries == ("kavinė", "kirpykla", "autoservisas")


@pytest.mark.parametrize(
    ("queries", "error"),
    [("kavinė", TypeError), ([], ValueError), (["kavinė", "kavinė"], ValueError)],
)
def test_queries_must_be_a_list_of_distinct_strings(
    queries: object, error: type[Exception]
) -> None:
    # Arrange
    config = build_config({"google_places.queries": queries})

    # Act / Assert
    with pytest.raises(error, match=r"google_places\.queries"):
        load_google_places_settings(config)


def test_a_sweep_has_at_least_one_search_in_flight() -> None:
    # Arrange
    config = build_config({"google_places.workers": 0})

    # Act / Assert
    with pytest.raises(ValueError, match=r"google_places\.workers"):
        load_google_places_settings(config)


def test_an_area_must_run_south_west_to_north_east() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="south-west to north-east"):
        Area(south=54.83, west=25.02, north=54.57, east=25.48)
