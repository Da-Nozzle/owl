from dataclasses import dataclass

from leads_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class Area:
    """A rectangle of latitude and longitude (WGS 84), as the Places API restricts a search: a
    search area's bounding box, or a tile of it."""

    south: float
    west: float
    north: float
    east: float

    def __post_init__(self) -> None:
        if not (self.south < self.north and self.west < self.east):
            raise ValueError(f"{self} is not a rectangle south-west to north-east")


@dataclass(frozen=True, slots=True)
class GooglePlacesSettings:
    # The Places API (New) Text Search endpoint.
    search_text_url: str
    # When a snapshot is fetched (cron, in pipeline.timezone); the snapshot is keyed by the
    # month of the tick it was fetched after.
    snapshot_schedule: str
    # The searches a snapshot makes, each over every search area (int_google_search_areas).
    queries: tuple[str, ...]
    # The language names and addresses are asked in, and the region results are formatted for.
    language_code: str
    region_code: str
    # A tile with places that is wider than this is split in four: a search over a larger tile
    # finds only some of its places even below the cap (13 of the Old Town's cafés in a 1.8 km
    # tile).
    max_tile_meters: float
    # A tile whose search is capped is split in four only while its quarters are at least this
    # many metres across; below, places beyond the cap are not found.
    min_tile_meters: float
    # The most paid requests one fetch may make. The fetch counts them before it pays for any,
    # and fails without paying if a sweep needs more.
    max_paid_requests: int
    # How many searches a sweep has in flight at once, its plan's and its payment's. Nothing
    # here counts Google's per-minute quota: a request over it is answered 429, and waited for
    # and sent again (HttpPlacesApi).
    workers: int


def load_google_places_settings(config: Configuration) -> GooglePlacesSettings:
    queries = config.get("google_places.queries")
    if not isinstance(queries, list) or not all(isinstance(query, str) for query in queries):
        raise TypeError("'google_places.queries' must be a list of strings")
    if not queries or len(set(queries)) != len(queries):
        raise ValueError(f"'google_places.queries' must be distinct and not empty: {queries}")
    return GooglePlacesSettings(
        search_text_url=config.get_str("google_places.search_text_url"),
        snapshot_schedule=config.get_str("google_places.snapshot_schedule"),
        queries=tuple(queries),
        language_code=config.get_str("google_places.language_code"),
        region_code=config.get_str("google_places.region_code"),
        max_tile_meters=number(config.get("google_places.max_tile_meters"), "max_tile_meters"),
        min_tile_meters=number(config.get("google_places.min_tile_meters"), "min_tile_meters"),
        max_paid_requests=config.get_int("google_places.max_paid_requests"),
        workers=positive(config.get_int("google_places.workers"), "workers"),
    )


def positive(value: int, key: str) -> int:
    if value < 1:
        raise ValueError(f"'google_places.{key}' must be at least 1: {value}")
    return value


def number(value: object, key: str) -> float:
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise TypeError(f"'google_places.{key}' is {type(value).__name__}, expected a number")
    return float(value)
