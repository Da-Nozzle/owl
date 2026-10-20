from collections.abc import Iterator
from contextlib import contextmanager

import duckdb
import pytest
from psycopg2 import sql
from s3fs import S3FileSystem

from owl_config.configuration import Configuration
from owl_ingest.adapters.s3.configuration import s3fs_options
from owl_lake.connection import attach_sql, load_lake_extensions_sql, s3_secret_sql
from owl_lake.settings import LakeSettings, load_lake_settings
from owl_testing.services import PostgresServer

LAKE = "lake"
# More rows than DuckLake keeps in its catalog instead of writing a Parquet file.
ROWS = 1000


@contextmanager
def attached(
    settings: LakeSettings, *, read_only: bool = False
) -> Iterator[duckdb.DuckDBPyConnection]:
    """A DuckDB of its own with the lake attached, as dbt's profile and the readers attach it."""
    with duckdb.connect() as connection:
        for statement in load_lake_extensions_sql():
            connection.execute(statement)
        connection.execute(s3_secret_sql(settings.storage))
        connection.execute(attach_sql(settings, LAKE, read_only=read_only))
        yield connection


def write_companies(settings: LakeSettings) -> None:
    with attached(settings) as connection:
        connection.execute(f"CREATE SCHEMA {LAKE}.staging")
        connection.execute(
            f"CREATE TABLE {LAKE}.staging.companies AS "
            f"SELECT range AS company_code, 'UAB ' || range AS name FROM range({ROWS})"
        )


def catalog_tables(postgres: PostgresServer, settings: LakeSettings) -> list[str]:
    """The tables the lake's catalog lists, from Postgres."""
    connection = postgres.connect(settings.catalog.database)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT table_name FROM {}.ducklake_table ORDER BY table_name").format(
                    sql.Identifier(settings.catalog.metadata_schema)
                )
            )
            return [name for (name,) in cursor.fetchall()]
    finally:
        connection.close()


def test_a_table_written_to_the_lake_is_read_back_from_its_catalog_and_bucket(
    services_config: Configuration, postgres: PostgresServer
) -> None:
    # Arrange
    settings = load_lake_settings(services_config)

    # Act
    write_companies(settings)

    # Assert: read by a DuckDB that did not write it, so from Postgres and S3 alone
    with attached(settings, read_only=True) as connection:
        read = connection.execute(
            f"SELECT count(*), max(name) FILTER (company_code = 7) FROM {LAKE}.staging.companies"
        ).fetchone()
    assert read == (ROWS, "UAB 7")
    assert catalog_tables(postgres, settings) == ["companies"]
    bucket = S3FileSystem(**s3fs_options(settings.storage))
    files = bucket.find(settings.storage.data_path)
    assert files
    assert all(file.endswith(".parquet") and "/staging/companies/" in file for file in files)


def test_the_lake_attached_read_only_refuses_a_write(services_config: Configuration) -> None:
    # Arrange
    settings = load_lake_settings(services_config)
    write_companies(settings)

    # Act / Assert
    with (
        attached(settings, read_only=True) as connection,
        pytest.raises(duckdb.InvalidInputException, match="read-only"),
    ):
        connection.execute(f"DELETE FROM {LAKE}.staging.companies")


def test_the_replay_lake_is_apart_from_the_live_one(services_config: Configuration) -> None:
    # Arrange: the same Postgres database and bucket, a schema and prefix apart
    live = load_lake_settings(services_config)
    replay = load_lake_settings(services_config, replay=True)

    # Act
    write_companies(live)

    # Assert
    with attached(replay) as connection:
        tables = connection.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = ?", [LAKE]
        ).fetchall()
    assert tables == []
