import importlib.metadata
import sysconfig
from functools import cached_property
from pathlib import Path

from dagster_dbt import DbtCliResource, DbtProject
from packaging.version import Version

DBT_CORE_PACKAGE = "dbt-core"


class DbtCoreCliResource(DbtCliResource):
    """dagster-dbt's resource for the dbt-core installed in this environment. dagster-dbt runs
    `dbt --version` once per run to tell dbt Fusion (2.x) from dbt-core, and that tooke about 3.5 s.
    Now the version is read from the installed package instead,
    which is the dbt that runs, as the executable is this
    environment's (`dbt_core_resource`)."""

    @cached_property
    def _cli_version(self) -> Version:
        return Version(importlib.metadata.version(DBT_CORE_PACKAGE))


def dbt_core_resource(project: DbtProject) -> DbtCoreCliResource:
    """Runs this environment's dbt whatever comes first on PATH, where the dbt VS Code extension
    puts dbt Fusion (see the launchers)."""
    return DbtCoreCliResource(project_dir=project, dbt_executable=str(dbt_core_executable()))


def dbt_core_executable() -> Path:
    return Path(sysconfig.get_path("scripts")) / "dbt"
