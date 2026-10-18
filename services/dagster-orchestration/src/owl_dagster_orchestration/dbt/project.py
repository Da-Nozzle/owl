import shutil
import uuid
from pathlib import Path

import duckdb
from dagster._core.definitions.definitions_load_context import (
    DefinitionsLoadContext,
    DefinitionsLoadType,
)
from dagster_dbt import DbtProject

from owl_config.configuration import Configuration
from owl_dagster_orchestration.dbt.environment import (
    dagster_dbt_environment,
    dbt_environment,
    ensure_temp_directory,
    set_dbt_environment,
)
from owl_ingest.settings import PipelineMode


def load_transform_project(config: Configuration, mode: PipelineMode) -> DbtProject:
    """The dbt project, with this process's environment set for the mode's lake first."""
    variables = dbt_environment(config, mode) | dagster_dbt_environment(config, mode)
    set_dbt_environment(variables)
    project = DbtProject(
        project_dir=config.get_str("transform.project_dir"),
        target_path=variables["DBT_TARGET_PATH"],
    )
    prepare_on_initial_load(project)
    ensure_metadata_database(Path(variables["OWL_DBT_DUCKDB_PATH"]))
    ensure_temp_directory(variables)
    return project


def prepare_on_initial_load(project: DbtProject) -> None:
    """Parses under `dagster dev` at a code server's load only: a run's processes reuse that
    load's manifest rather than parse again (about 11 s each)."""
    if DefinitionsLoadContext.get().load_type is DefinitionsLoadType.INITIALIZATION:
        project.prepare_if_dev()


def ensure_metadata_database(path: Path) -> None:
    """dagster-dbt opens this file read-only to read each built table's columns and row count,
    which DuckDB cannot do in memory or on a missing file."""
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.{uuid.uuid4().hex}.partial")
    duckdb.connect(str(partial)).close()
    partial.replace(path)


def keep_main_state(manifest: Path, state_dir: Path) -> None:
    """Keeps a build's manifest as the state the dev target defers to, replaced in one step so
    no reader sees half a file."""
    state_dir.mkdir(parents=True, exist_ok=True)
    partial = state_dir / f"manifest.json.{uuid.uuid4().hex}.partial"
    shutil.copyfile(manifest, partial)
    partial.replace(state_dir / "manifest.json")
