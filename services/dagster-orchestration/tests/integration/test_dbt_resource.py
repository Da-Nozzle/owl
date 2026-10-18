import importlib.metadata
from pathlib import Path

import dagster_dbt.core.resource
import pytest
from dagster_dbt import DbtProject

from leads_dagster_orchestration.dbt.resource import (
    DBT_CORE_PACKAGE,
    dbt_core_executable,
    dbt_core_resource,
)


@pytest.fixture
def transform_project(pytestconfig: pytest.Config) -> DbtProject:
    return DbtProject(project_dir=pytestconfig.rootpath / "services" / "transform")


def test_the_dbt_version_is_the_installed_packages_and_runs_no_dbt(
    monkeypatch: pytest.MonkeyPatch, transform_project: DbtProject
) -> None:
    # Arrange
    def no_dbt(command: list[str]) -> bytes:
        raise AssertionError(f"ran {command}")

    monkeypatch.setattr(dagster_dbt.core.resource, "check_output", no_dbt)
    resource = dbt_core_resource(transform_project)

    # Act
    version = resource._cli_version

    # Assert
    assert str(version) == importlib.metadata.version(DBT_CORE_PACKAGE)


def test_the_dbt_that_runs_is_the_installed_dbt_core(transform_project: DbtProject) -> None:
    # Arrange
    package = importlib.metadata.distribution(DBT_CORE_PACKAGE)
    package_files = {Path(package.locate_file(file)).resolve() for file in package.files or []}

    # Act
    resource = dbt_core_resource(transform_project)

    # Assert
    assert Path(resource.dbt_executable) == dbt_core_executable()
    assert dbt_core_executable().resolve() in package_files
