from dataclasses import dataclass

from owl_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class GooglePlacesSettings:
    search_text_url: str
    snapshot_schedule: str
    # Each is searched over every search area.
    queries: tuple[str, ...]
    language_code: str
    region_code: str
    # A tile with places is split into quarters while wider than max_tile_meters, and a capped
    # one while its quarters are at least min_tile_meters wide.
    max_tile_meters: float
    min_tile_meters: float
    max_paid_requests: int
    # Searches in flight at once; the HTTP client waits out Google's per-minute quota.
    workers: int
    fetch_max_runtime_seconds: int
    free_paid_requests_a_month: int
    usd_per_paid_request: float


def load_google_places_settings(config: Configuration) -> GooglePlacesSettings:
    queries = config.get("google_places.queries")
    if not isinstance(queries, list) or not all(isinstance(query, str) for query in queries):
        raise TypeError("'google_places.queries' must be a list of strings")
    if not queries or len(set(queries)) != len(queries):
        raise ValueError(f"'google_places.queries' must be distinct and not empty: {queries}")
    usd_per_paid_request = config.get_float("google_places.usd_per_paid_request")
    if usd_per_paid_request < 0:
        raise ValueError(
            f"'google_places.usd_per_paid_request' is negative: {usd_per_paid_request}"
        )
    return GooglePlacesSettings(
        search_text_url=config.get_str("google_places.search_text_url"),
        snapshot_schedule=config.get_str("google_places.snapshot_schedule"),
        queries=tuple(queries),
        language_code=config.get_str("google_places.language_code"),
        region_code=config.get_str("google_places.region_code"),
        max_tile_meters=config.get_float("google_places.max_tile_meters"),
        min_tile_meters=config.get_float("google_places.min_tile_meters"),
        max_paid_requests=config.get_int("google_places.max_paid_requests"),
        workers=int_at_least(config, "workers", 1),
        fetch_max_runtime_seconds=int_at_least(config, "fetch_max_runtime_seconds", 1),
        free_paid_requests_a_month=int_at_least(config, "free_paid_requests_a_month", 0),
        usd_per_paid_request=usd_per_paid_request,
    )


def int_at_least(config: Configuration, key: str, minimum: int) -> int:
    value = config.get_int(f"google_places.{key}")
    if value < minimum:
        raise ValueError(f"'google_places.{key}' must be at least {minimum}: {value}")
    return value
