from collections.abc import Iterator

import duckdb
import pytest

from leads_ingest.adapters.ducklake.search_areas import (
    LAKE,
    SEARCH_AREAS_MODEL,
    SEARCH_AREAS_SCHEMA,
    attach_statements,
    query_search_areas,
)
from leads_ingest.ports.search_areas import BoundingBox, SearchArea, SearchMode
from leads_lake.settings import load_lake_settings
from leads_testing.config import build_config

MODEL = f"{LAKE}.{SEARCH_AREAS_SCHEMA}.{SEARCH_AREAS_MODEL}"
BOX = (54.5689, 25.0245, 54.8324, 25.4815)


@pytest.fixture
def lake() -> Iterator[duckdb.DuckDBPyConnection]:
    """An in-memory stand-in for the lake, holding an empty search areas model."""
    with duckdb.connect() as connection:
        connection.execute(f"ATTACH ':memory:' AS {LAKE}")
        connection.execute(f"CREATE SCHEMA {LAKE}.{SEARCH_AREAS_SCHEMA}")
        connection.execute(
            f"CREATE TABLE {MODEL} (municipality_code INTEGER, municipality VARCHAR, "
            "search_mode VARCHAR, south DOUBLE, west DOUBLE, north DOUBLE, east DOUBLE)"
        )
        yield connection


def insert(lake: duckdb.DuckDBPyConnection, *rows: tuple[object, ...]) -> None:
    lake.executemany(f"INSERT INTO {MODEL} VALUES (?, ?, ?, ?, ?, ?, ?)", list(rows))


def test_the_reader_attaches_the_lake_read_only_and_capped() -> None:
    # Arrange
    settings = load_lake_settings(build_config({"lake.s3.secret": "a'b"}))

    # Act
    statements = attach_statements(settings, "1GB")

    # Assert
    parsed = [duckdb.extract_statements(statement)[-1].type.name for statement in statements]
    assert parsed[0] == "SET"
    assert parsed[-2:] == ["CREATE", "ATTACH"]
    assert statements[0] == "SET memory_limit = '1GB'"
    assert statements[-1].endswith(
        " AS lake (DATA_PATH 's3://leads-lake/', METADATA_SCHEMA 'ducklake', READ_ONLY)"
    )


def test_the_areas_with_a_search_mode_are_read_by_code(lake: duckdb.DuckDBPyConnection) -> None:
    # Arrange
    insert(
        lake,
        (41, "Vilniaus rajono sav.", "price", *BOX),
        (19, "Kauno miesto sav.", None, *BOX),
        (13, "Vilniaus miesto sav.", "fetch", *BOX),
    )

    # Act
    areas = query_search_areas(lake)

    # Assert
    assert areas == [
        SearchArea(13, "Vilniaus miesto sav.", SearchMode.FETCH, BoundingBox(*BOX)),
        SearchArea(41, "Vilniaus rajono sav.", SearchMode.PRICE, BoundingBox(*BOX)),
    ]


def test_an_unknown_search_mode_is_refused(lake: duckdb.DuckDBPyConnection) -> None:
    # Arrange
    insert(lake, (13, "Vilniaus miesto sav.", "scrape", *BOX))

    # Act / Assert
    with pytest.raises(ValueError, match="scrape"):
        query_search_areas(lake)
