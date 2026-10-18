import json
import os
import sysconfig
from pathlib import Path

import pytest

from leads_dagster_orchestration.dev_tools.dbt_cli import built_models
from leads_dagster_orchestration.dev_tools.launch import launch_environment, split_lake_option
from leads_dagster_orchestration.dev_tools.mcp_servers import dbt_mcp_environment, load_mcp_settings
from leads_ingest.settings import PipelineMode
from leads_testing.config import build_config


def test_project_scripts_come_before_other_installs_on_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("PATH", os.pathsep.join(["/home/dev/.local/bin", "/usr/bin"]))

    # Act
    environment = launch_environment({"LEADS_S3_REGION": "eu-central-1"})

    # Assert
    assert environment["PATH"].split(os.pathsep) == [
        sysconfig.get_path("scripts"),
        "/home/dev/.local/bin",
        "/usr/bin",
    ]
    assert environment["LEADS_S3_REGION"] == "eu-central-1"


@pytest.mark.parametrize(
    "arguments",
    [
        ["--lake", "live", "build", "-s", "stg_sodra__monthly"],
        ["build", "--lake=live", "-s", "stg_sodra__monthly"],
        ["build", "-s", "stg_sodra__monthly", "--lake", "live"],
    ],
)
def test_the_lake_option_is_taken_out_of_dbts_arguments(arguments: list[str]) -> None:
    # Act
    mode, rest = split_lake_option(arguments, default=None)

    # Assert
    assert (mode, rest) == (PipelineMode.LIVE, ["build", "-s", "stg_sodra__monthly"])


def test_without_the_lake_option_the_default_lake_is_used() -> None:
    # Act / Assert
    assert split_lake_option(["show"], default=PipelineMode.REPLAY) == (
        PipelineMode.REPLAY,
        ["show"],
    )


@pytest.mark.parametrize("arguments", [["build"], ["--lake", "staging"], ["--lake"]])
def test_a_missing_or_unknown_lake_stops_the_tool(arguments: list[str]) -> None:
    # Act / Assert
    with pytest.raises(SystemExit, match="lake"):
        split_lake_option(arguments, default=None)


def test_the_models_to_diff_are_those_the_last_dev_command_built(tmp_path: Path) -> None:
    # Arrange
    run_results = tmp_path / "run_results.json"
    run_results.write_text(
        json.dumps(
            {
                "results": [
                    {"unique_id": "model.leads.stg_sodra__monthly", "status": "success"},
                    {"unique_id": "test.leads.not_null_stg_x.abc", "status": "pass"},
                    {"unique_id": "model.leads.fct_leads", "status": "error"},
                    {"unique_id": "model.leads.int_scores.v2", "status": "success"},
                ]
            }
        )
    )

    # Act
    models = built_models(run_results)

    # Assert
    assert models == ["model.leads.stg_sodra__monthly", "model.leads.int_scores.v2"]


def test_no_dev_command_yet_means_no_models_to_diff(tmp_path: Path) -> None:
    # Act / Assert
    assert built_models(tmp_path / "run_results.json") == []


def test_the_dbt_mcp_server_waits_on_dbt_as_long_as_configured() -> None:
    # Arrange
    config = build_config({"mcp.dbt.command_timeout_seconds": 600})

    # Act
    environment = dbt_mcp_environment(load_mcp_settings(config).dbt_command_timeout_seconds)

    # Assert
    assert environment["DBT_CLI_TIMEOUT"] == "600"
