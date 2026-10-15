from collections.abc import MutableMapping
from typing import cast

import pytest
from dagster._core.definitions.definitions_load_context import (
    DefinitionsLoadContext,
    DefinitionsLoadType,
)
from dagster_dbt import DbtProject

from leads_dagster_orchestration.assets.transform import (
    dbt_sources,
    environment_variable,
    has_shared_models,
    prepare_on_initial_load,
    run_month_count_query,
    set_dbt_environment,
)


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


def test_an_environment_variable_is_set_only_within_its_block(
    environ: MutableMapping[str, str],
) -> None:
    # Arrange
    environ["LEADS_TEST_KEPT"] = "before"
    environ.pop("LEADS_TEST_ADDED", None)

    # Act
    with environment_variable("LEADS_TEST_KEPT", "2026-09"):
        kept_within = environ["LEADS_TEST_KEPT"]
    with environment_variable("LEADS_TEST_ADDED", "2026-09"):
        added_within = environ["LEADS_TEST_ADDED"]

    # Assert
    assert (kept_within, environ["LEADS_TEST_KEPT"]) == ("2026-09", "before")
    assert added_within == "2026-09"
    assert "LEADS_TEST_ADDED" not in environ


def test_dbt_environment_is_set_in_this_process(environ: MutableMapping[str, str]) -> None:
    # Arrange
    environ["LEADS_ARCHIVE_PATH"] = "/workspace/replay"
    environ.pop("LEADS_TEST_ADDED", None)

    # Act
    set_dbt_environment({"LEADS_ARCHIVE_PATH": "s3://leads-lake/archive", "LEADS_TEST_ADDED": "13"})

    # Assert
    assert environ["LEADS_ARCHIVE_PATH"] == "s3://leads-lake/archive"
    assert environ["LEADS_TEST_ADDED"] == "13"


def test_a_model_with_a_run_month_column_counts_the_run_months_rows() -> None:
    # Arrange
    node = {
        "relation_name": '"lake"."raw_sodra"."monthly"',
        "meta": {"run_month_column": "_snapshot_month"},
    }

    # Act
    query = run_month_count_query(node, "2026-09")

    # Assert
    assert query == (
        'select count(*) from "lake"."raw_sodra"."monthly" where "_snapshot_month" = \'2026-09\''
    )


def test_a_model_without_a_run_month_column_keeps_the_tables_count() -> None:
    # Act / Assert
    assert run_month_count_query({"relation_name": "t", "meta": {}}, "2026-09") is None


class PreparedProject:
    """Counts the preparations a DbtProject would have made."""

    def __init__(self) -> None:
        self.preparations = 0

    def prepare_if_dev(self) -> None:
        self.preparations += 1


def preparations_when_loaded_as(load_type: DefinitionsLoadType) -> int:
    project = PreparedProject()
    with DefinitionsLoadContext.scoped(DefinitionsLoadContext(load_type)):
        prepare_on_initial_load(cast("DbtProject", project))
    return project.preparations


def test_the_code_servers_load_prepares_the_dbt_project() -> None:
    # Act / Assert
    assert preparations_when_loaded_as(DefinitionsLoadType.INITIALIZATION) == 1


def test_a_run_rebuilding_the_definitions_does_not_prepare_the_dbt_project() -> None:
    # Act / Assert
    assert preparations_when_loaded_as(DefinitionsLoadType.RECONSTRUCTION) == 0
