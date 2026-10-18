import pytest

from owl_ingest.sources.google_places.settings import (
    GooglePlacesSettings,
    load_google_places_settings,
)
from owl_testing.config import build_config


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
            "google_places.fetch_max_runtime_seconds": 10_800,
            "google_places.free_paid_requests_a_month": 1_000,
            "google_places.usd_per_paid_request": 0.035,
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
        fetch_max_runtime_seconds=10_800,
        free_paid_requests_a_month=1_000,
        usd_per_paid_request=0.035,
    )


def test_loading_the_settings_reads_no_key() -> None:
    # Arrange: no key in the secret store, so reading it would fail.
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


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("workers", 0),
        ("fetch_max_runtime_seconds", 0),
        ("free_paid_requests_a_month", -1),
        ("usd_per_paid_request", -0.035),
    ],
)
def test_a_count_or_price_out_of_range_is_rejected(key: str, value: float) -> None:
    # Arrange
    config = build_config({f"google_places.{key}": value})

    # Act / Assert
    with pytest.raises(ValueError, match=rf"google_places\.{key}"):
        load_google_places_settings(config)
