from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import dagster as dg
from dagster_dbt import DbtCliInvocation

from owl_dagster_orchestration.dbt.step_metadata import snapshot_metadata

RAW_MONTHLY = {
    "unique_id": "model.owl.raw_sodra__monthly",
    "name": "raw_sodra__monthly",
    "relation_name": '"lake"."raw_sodra"."monthly"',
    "meta": {"snapshot_column": "_snapshot_month", "fetched_at_column": "_file_fetched_at"},
}


@dataclass
class FakeAdapter:
    """A dbt adapter that answers every query with the same count and newest fetch."""

    count: int
    fetched_at: str | None = None
    queries: list[str] = field(default_factory=list)

    @contextmanager
    def connection_named(self, name: str) -> Iterator[None]:
        yield

    def execute(self, sql: str, *, fetch: bool) -> tuple[None, list[tuple[int, str | None]]]:
        self.queries.append(sql)
        return None, [(self.count, self.fetched_at)]


def finished_build(node: dict[str, Any], adapter: FakeAdapter) -> DbtCliInvocation:
    """A dbt build whose manifest holds the one node."""
    build = SimpleNamespace(
        manifest={"nodes": {node["unique_id"]: node}}, adapter=adapter, project_dir=Path.cwd()
    )
    return cast("DbtCliInvocation", build)


def output_of(node: dict[str, Any], *, table_rows: int) -> dg.Output:
    return dg.Output(
        None,
        output_name=node["name"],
        metadata={"unique_id": node["unique_id"], "dagster/row_count": table_rows},
    )


def test_a_snapshot_models_row_count_is_its_snapshots_rows() -> None:
    # Arrange
    adapter = FakeAdapter(count=7, fetched_at="2026-10-14T19:05:00+00:00")
    event = output_of(RAW_MONTHLY, table_rows=40)

    # Act
    counted = snapshot_metadata(event, finished_build(RAW_MONTHLY, adapter), "2026-09")

    # Assert
    assert counted.metadata["dagster/row_count"] == dg.MetadataValue.int(7)
    assert counted.metadata["table_row_count"] == dg.MetadataValue.int(40)
    assert adapter.queries == [
        'select count(*), max("_file_fetched_at") from "lake"."raw_sodra"."monthly" '
        "where \"_snapshot_month\" = '2026-09'"
    ]


def test_a_snapshots_slice_shows_which_download_it_was_built_from() -> None:
    # Arrange
    adapter = FakeAdapter(count=7, fetched_at="2026-10-14T19:05:00+00:00")

    # Act
    counted = snapshot_metadata(
        output_of(RAW_MONTHLY, table_rows=40), finished_build(RAW_MONTHLY, adapter), "2026-09"
    )

    # Assert
    assert counted.metadata["archive_fetched_at"] == dg.MetadataValue.text(
        "2026-10-14T19:05:00+00:00"
    )


def test_a_model_without_a_fetch_column_counts_rows_only() -> None:
    # Arrange
    node = {**RAW_MONTHLY, "meta": {"snapshot_column": "_snapshot_month"}}
    adapter = FakeAdapter(count=7)

    # Act
    counted = snapshot_metadata(
        output_of(node, table_rows=40), finished_build(node, adapter), "2026-09"
    )

    # Assert
    assert "archive_fetched_at" not in counted.metadata
    assert adapter.queries == [
        'select count(*), null from "lake"."raw_sodra"."monthly" '
        "where \"_snapshot_month\" = '2026-09'"
    ]


def test_a_model_without_a_snapshot_column_keeps_its_tables_row_count() -> None:
    # Arrange
    node = {**RAW_MONTHLY, "meta": {}}
    adapter = FakeAdapter(count=7)
    event = output_of(node, table_rows=40)

    # Act
    counted = snapshot_metadata(event, finished_build(node, adapter), "2026-09")

    # Assert
    assert counted is event
    assert adapter.queries == []


def test_events_that_are_not_outputs_are_left_alone() -> None:
    # Arrange
    check = dg.AssetCheckResult(passed=True, asset_key="raw", check_name="c")
    build = finished_build(RAW_MONTHLY, FakeAdapter(count=7))

    # Act / Assert
    assert snapshot_metadata(check, build, "2026-09") is check
