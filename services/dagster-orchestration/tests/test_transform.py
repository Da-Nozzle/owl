import os
from pathlib import Path

import duckdb
import pytest

from leads_dagster_orchestration.assets.transform import (
    dbt_sources,
    ensure_metadata_database,
    environment_variable,
    has_shared_models,
    keep_main_state,
)

ENVIRONMENT = os.environ  # noqa: TID251 - checks what a step sets for dbt


def manifest_of(*nodes: tuple[str, list[str]]) -> dict[str, object]:
    return {
        "nodes": {
            f"{resource_type}.{'.'.join(fqn)}": {
                "resource_type": resource_type,
                "fqn": fqn,
                "original_file_path": "/".join(fqn[1:]),
            }
            for resource_type, fqn in nodes
        }
    }


def test_each_source_folder_is_a_step_and_tests_are_not() -> None:
    # Arrange
    manifest = manifest_of(
        ("model", ["leads", "raw", "sodra", "raw_sodra__monthly"]),
        ("model", ["leads", "staging", "sodra", "stg_sodra__monthly"]),
        ("model", ["leads", "staging", "jar", "stg_jar__entities"]),
        ("test", ["leads", "sodra_no_column_dropped"]),
    )

    # Act
    sources = dbt_sources(manifest)

    # Assert
    assert sources == ["jar", "sodra"]


def test_a_staging_model_outside_a_source_folder_is_rejected() -> None:
    # Arrange
    manifest = manifest_of(("model", ["leads", "staging", "stg_sodra__monthly"]))

    # Act / Assert
    with pytest.raises(ValueError, match="models/staging/<source>/"):
        dbt_sources(manifest)


def test_shared_step_exists_only_for_models_outside_the_source_layers() -> None:
    # Arrange
    sources_only = manifest_of(
        ("model", ["leads", "raw", "sodra", "raw_sodra__monthly"]),
        ("test", ["leads", "sodra_no_column_dropped"]),
    )
    with_mart = manifest_of(("model", ["leads", "marts", "fct_leads"]))
    with_seed = manifest_of(("seed", ["leads", "scoring_rules"]))

    # Act / Assert
    assert not has_shared_models(sources_only)
    assert has_shared_models(with_mart)
    assert has_shared_models(with_seed)


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


def test_an_environment_variable_is_set_only_within_its_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("LEADS_TEST_KEPT", "before")
    monkeypatch.delenv("LEADS_TEST_ADDED", raising=False)

    # Act
    with environment_variable("LEADS_TEST_KEPT", "2026-09"):
        kept_within = ENVIRONMENT["LEADS_TEST_KEPT"]
    with environment_variable("LEADS_TEST_ADDED", "2026-09"):
        added_within = ENVIRONMENT["LEADS_TEST_ADDED"]

    # Assert
    assert (kept_within, ENVIRONMENT["LEADS_TEST_KEPT"]) == ("2026-09", "before")
    assert added_within == "2026-09"
    assert "LEADS_TEST_ADDED" not in ENVIRONMENT
