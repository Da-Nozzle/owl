import importlib.metadata
import sysconfig
from functools import cached_property
from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject
from packaging.version import Version

DBT_CORE_PACKAGE = "dbt-core"


class DbtCoreCliResource(DbtCliResource):
    """Reads dbt's version from the installed package: dagster-dbt would run `dbt --version`
    in each run (about 3.5 s) to tell dbt Fusion from dbt-core."""

    @cached_property
    def _cli_version(self) -> Version:
        return Version(importlib.metadata.version(DBT_CORE_PACKAGE))


def dbt_core_resource(project: DbtProject) -> DbtCoreCliResource:
    """This environment's dbt, not the first on PATH, which can be the VS Code extension's dbt
    Fusion."""
    return DbtCoreCliResource(project_dir=project, dbt_executable=str(dbt_core_executable()))


def dbt_core_executable() -> Path:
    return Path(sysconfig.get_path("scripts")) / "dbt"
