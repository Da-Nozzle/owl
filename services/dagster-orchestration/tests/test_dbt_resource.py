import importlib.metadata
from pathlib import Path

import dagster_dbt.core.resource
import pytest
from dagster_dbt import DbtProject

from leads_dagster_orchestration.dbt_resource import (
    DBT_CORE_PACKAGE,
    dbt_core_executable,
    dbt_core_resource,
)

TRANSFORM_PROJECT = Path(__file__).parents[2] / "transform"


def test_the_dbt_version_is_the_installed_packages_and_runs_no_dbt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    def no_dbt(command: list[str]) -> bytes:
        raise AssertionError(f"ran {command}")

    monkeypatch.setattr(dagster_dbt.core.resource, "check_output", no_dbt)
    resource = dbt_core_resource(DbtProject(project_dir=TRANSFORM_PROJECT))

    # Act
    version = resource._cli_version

    # Assert
    assert str(version) == importlib.metadata.version(DBT_CORE_PACKAGE)


def test_the_dbt_that_runs_is_the_installed_dbt_core() -> None:
    # Arrange
    package = importlib.metadata.distribution(DBT_CORE_PACKAGE)
    package_files = {Path(package.locate_file(file)).resolve() for file in package.files or []}

    # Act
    resource = dbt_core_resource(DbtProject(project_dir=TRANSFORM_PROJECT))

    # Assert
    assert Path(resource.dbt_executable) == dbt_core_executable()
    assert dbt_core_executable().resolve() in package_files
