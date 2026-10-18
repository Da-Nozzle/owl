from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import dagster as dg
import duckdb
from dagster_dbt import DbtCliInvocation

from leads_dagster_orchestration.assets.transform import (
    COMPILED_SQL,
    ensure_metadata_database,
    keep_main_state,
    with_compiled_sql,
)

MODEL = "model.leads_transform.stg_sodra__monthly"
SEED = "seed.leads_transform.municipalities"


@dataclass
class FinishedBuild:
    """What with_compiled_sql reads of a dbt invocation: its manifest and its target folder."""

    manifest: dict[str, Any]
    target_path: Path


def finished_build(target_path: Path) -> DbtCliInvocation:
    nodes = {
        MODEL: {
            "resource_type": "model",
            "package_name": "leads_transform",
            "original_file_path": "models/staging/sodra/stg_sodra__monthly.sql",
        },
        SEED: {
            "resource_type": "seed",
            "package_name": "leads_transform",
            "original_file_path": "seeds/municipalities.csv",
        },
    }
    return cast("DbtCliInvocation", FinishedBuild({"nodes": nodes}, target_path))


def built(unique_id: str) -> dg.Output:
    return dg.Output(None, metadata={"unique_id": unique_id})


def test_main_state_is_replaced_by_the_latest_builds_manifest(tmp_path: Path) -> None:
    # Arrange
    state_dir = tmp_path / "target" / "main"
    state_dir.mkdir(parents=True)
    (state_dir / "manifest.json").write_text('{"build": 1}')
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"build": 2}')

    # Act
    keep_main_state(manifest, state_dir)

    # Assert
    assert [path.name for path in state_dir.iterdir()] == ["manifest.json"]
    assert (state_dir / "manifest.json").read_text() == '{"build": 2}'


def test_main_state_is_kept_from_the_first_build(tmp_path: Path) -> None:
    # Arrange
    state_dir = tmp_path / "target" / "main"
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"build": 1}')

    # Act
    keep_main_state(manifest, state_dir)

    # Assert
    assert (state_dir / "manifest.json").read_text() == '{"build": 1}'


def test_metadata_database_opens_read_only(tmp_path: Path) -> None:
    # Arrange
    path = tmp_path / "target" / "dagster.duckdb"

    # Act
    ensure_metadata_database(path)
    ensure_metadata_database(path)

    # Assert
    with duckdb.connect(str(path), read_only=True) as connection:
        assert connection.execute("select 1").fetchone() == (1,)
    assert [entry.name for entry in path.parent.iterdir()] == ["dagster.duckdb"]


def test_a_built_model_carries_the_sql_dbt_compiled_for_it(tmp_path: Path) -> None:
    # Arrange
    compiled = tmp_path / "compiled/leads_transform/models/staging/sodra/stg_sodra__monthly.sql"
    compiled.parent.mkdir(parents=True)
    compiled.write_text('select code from "lake"."raw_sodra"."monthly"')

    # Act
    event = with_compiled_sql(built(MODEL), finished_build(tmp_path))

    # Assert
    assert event.metadata[COMPILED_SQL] == dg.MetadataValue.text(
        'select code from "lake"."raw_sodra"."monthly"'
    )


def test_a_seed_or_a_model_dbt_compiled_nothing_for_carries_no_sql(tmp_path: Path) -> None:
    # Act
    events = [with_compiled_sql(built(node), finished_build(tmp_path)) for node in (MODEL, SEED)]

    # Assert
    assert [COMPILED_SQL in event.metadata for event in events] == [False, False]
