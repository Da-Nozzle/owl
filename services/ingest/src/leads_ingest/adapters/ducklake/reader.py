from collections.abc import Mapping
from dataclasses import dataclass

import duckdb

from leads_ingest.ports.lake_reader import LAKE
from leads_lake.connection import attach_sql, s3_secret_sql, sql_literal
from leads_lake.settings import LakeSettings


@dataclass(frozen=True, slots=True)
class DuckLakeReader:
    """Reads the lake with DuckDB in memory, the lake attached read-only as `lake` through the
    catalog, bucket and secret dbt uses, and DuckDB capped as dbt's processes are. Each read opens
    and closes a connection of its own."""

    settings: LakeSettings
    memory_limit: str

    def rows(self, sql: str) -> list[Mapping[str, object]]:
        with duckdb.connect() as connection:
            for statement in reader_statements(self.settings, self.memory_limit):
                connection.execute(statement)
            result = connection.execute(sql)
            columns = [column[0] for column in result.description or []]
            return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def reader_statements(settings: LakeSettings, memory_limit: str) -> list[str]:
    return [
        f"SET memory_limit = {sql_literal(memory_limit)}",
        *(f"INSTALL {name}; LOAD {name}" for name in ["httpfs", "postgres", "ducklake"]),
        s3_secret_sql(settings.storage),
        attach_sql(settings, LAKE, read_only=True),
    ]
