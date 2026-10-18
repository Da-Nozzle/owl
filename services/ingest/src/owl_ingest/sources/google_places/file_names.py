import re
import unicodedata

from owl_ingest.ports.search_areas import SearchArea
from owl_ingest.sources.google_places.tiles import Tile


def page_name(area: SearchArea, query: str, tile: Tile, page: int) -> str:
    return f"places-{area.code}-{query_name(query)}-{tile.path}-{page}.json"


def query_name(query: str) -> str:
    """ASCII, lower case, words joined by `-`: `grožio salonas` is `grozio-salonas`."""
    folded = unicodedata.normalize("NFKD", query).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "-", folded.lower()).strip("-")


def require_distinct_query_names(queries: tuple[str, ...]) -> None:
    """Two queries with the same name would overwrite each other's pages."""
    names = [query_name(query) for query in queries]
    if "" in names or len(set(names)) != len(names):
        raise ValueError(f"Queries {list(queries)} are not distinct in file names: {names}")
