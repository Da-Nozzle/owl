"""Where Google is searched: municipalities, as the dbt model `int_google_search_areas` lists them,
each with its bounding box and whether it is priced or fetched (the `google_search_scope` seed).
The areas are data, so the graph shows what a sweep covers, and adding a municipality to the seed
prices it before anything is paid for it."""

from dataclasses import dataclass
from enum import StrEnum

from leads_ingest.ports.lake_reader import LAKE, LakeReader
from leads_ingest.sources.google_places.settings import Area

# The dbt model the areas are read from, and its schema.
AREAS_MODEL = "int_google_search_areas"
AREAS_SCHEMA = "intermediate"


class SearchMode(StrEnum):
    """What is done with an area: `price` plans it for free and shows what fetching it would
    cost; `fetch` does that and the monthly fetch pays for it."""

    PRICE = "price"
    FETCH = "fetch"


@dataclass(frozen=True, slots=True)
class SearchArea:
    """A municipality to search, by its Address Register code."""

    code: int
    name: str
    mode: SearchMode
    area: Area


def read_search_areas(reader: LakeReader) -> list[SearchArea]:
    """The areas with a search mode, by code. Fails if the model is not built in the lake."""
    sql = f"""
        select municipality_code, municipality, search_mode, south, west, north, east
        from {LAKE}.{AREAS_SCHEMA}.{AREAS_MODEL}
        where search_mode is not null
        order by municipality_code
    """
    return [
        SearchArea(
            code=int(str(row["municipality_code"])),
            name=str(row["municipality"]),
            mode=SearchMode(str(row["search_mode"])),
            area=Area(*(float(str(row[side])) for side in ("south", "west", "north", "east"))),
        )
        for row in reader.rows(sql)
    ]
