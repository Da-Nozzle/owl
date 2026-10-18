from dataclasses import dataclass

import duckdb

from owl_ingest.ports.search_areas import BoundingBox, SearchArea, SearchMode
from owl_lake.connection import (
    attach_sql,
    load_lake_extensions_sql,
    s3_secret_sql,
    sql_literal,
)
from owl_lake.settings import LakeSettings

LAKE = "lake"
SEARCH_AREAS_SCHEMA = "intermediate"
SEARCH_AREAS_MODEL = "int_google_search_areas"
SEARCH_AREAS_SQL = f"""
    select municipality_code, municipality, search_mode, south, west, north, east
    from {LAKE}.{SEARCH_AREAS_SCHEMA}.{SEARCH_AREAS_MODEL}
    where search_mode is not null
    order by municipality_code
"""


@dataclass(frozen=True, slots=True)
class DuckLakeSearchAreaReader:
    """Reads the search areas' dbt model from the lake, attached read-only, with DuckDB capped
    at `memory_limit`."""

    settings: LakeSettings
    memory_limit: str

    def search_areas(self) -> list[SearchArea]:
        with duckdb.connect() as connection:
            for statement in attach_statements(self.settings, self.memory_limit):
                connection.execute(statement)
            return query_search_areas(connection)


def query_search_areas(connection: duckdb.DuckDBPyConnection) -> list[SearchArea]:
    rows = connection.execute(SEARCH_AREAS_SQL).fetchall()
    return [
        SearchArea(
            code=int(code),
            name=str(name),
            mode=SearchMode(mode),
            box=BoundingBox(float(south), float(west), float(north), float(east)),
        )
        for code, name, mode, south, west, north, east in rows
    ]


def attach_statements(settings: LakeSettings, memory_limit: str) -> list[str]:
    return [
        f"SET memory_limit = {sql_literal(memory_limit)}",
        *load_lake_extensions_sql(),
        s3_secret_sql(settings.storage),
        attach_sql(settings, LAKE, read_only=True),
    ]
