import csv
import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

from owl_config.configuration import Configuration
from owl_ingest.settings import PipelineMode, load_pipeline_settings, raw_archive_path
from owl_lake.connection import ducklake_catalog_connection
from owl_lake.settings import load_lake_settings

# Read by the project's snapshot() macro.
SNAPSHOT_VARIABLE = "OWL_SNAPSHOT"
# Read by the project's risk_model_registry() macro.
RISK_MODELS_VARIABLE = "OWL_RISK_MODELS"
RISK_MODELS_SEED = "seeds/risk_models.csv"


def dbt_environment(config: Configuration, mode: PipelineMode) -> dict[str, str]:
    """What the dbt project reads through env_var(). The profile adds dbt's invocation id to the
    temp directory: DuckDB names its spill files the same in every process."""
    pipeline = load_pipeline_settings(config)
    lake = load_lake_settings(config, replay=mode is PipelineMode.REPLAY)
    project_dir = config.get_str("transform.project_dir")
    return {
        **risk_model_environment(Path(project_dir)),
        "OWL_ARCHIVE_PATH": raw_archive_path(mode, pipeline, lake.storage),
        "DBT_PROJECT_DIR": project_dir,
        "DBT_PROFILES_DIR": project_dir,
        "OWL_DUCKDB_MEMORY_LIMIT": config.get_str("transform.duckdb_memory_limit"),
        "OWL_DBT_THREADS": str(dbt_threads(config)),
        "OWL_DUCKDB_TEMP_DIRECTORY": f"{dbt_target_dir(config)}/duckdb_tmp",
        "OWL_DUCKLAKE_CATALOG": ducklake_catalog_connection(lake.catalog),
        "OWL_DUCKLAKE_METADATA_SCHEMA": lake.catalog.metadata_schema,
        "OWL_LAKE_DATA_PATH": lake.storage.data_path,
        "OWL_S3_ENDPOINT": lake.storage.endpoint,
        "OWL_S3_REGION": lake.storage.region,
        "OWL_S3_URL_STYLE": lake.storage.url_style,
        "OWL_S3_USE_SSL": "true" if lake.storage.use_ssl else "false",
        "OWL_S3_KEY_ID": lake.storage.key_id,
        "OWL_S3_SECRET": lake.storage.secret,
    }


def risk_model_environment(project_dir: Path) -> dict[str, str]:
    """The risk_models seed's enabled rows, as `wage_floor:1,...`. fct_score_components refs each
    of them, and dbt must know a model's refs while it parses, before any seed can be read, so the
    registry reaches it as a variable. Unset without the seed (a project that is not this one), so
    the macro fails rather than read an empty registry."""
    seed = project_dir / RISK_MODELS_SEED
    if not seed.exists():
        return {}
    with seed.open(newline="", encoding="utf-8") as rows:
        enabled = [
            f"{row['id']}:{row['version']}"
            for row in csv.DictReader(rows)
            if row["enabled"].strip().lower() == "true"
        ]
    return {RISK_MODELS_VARIABLE: ",".join(enabled)}


def ensure_temp_directory(environment: Mapping[str, str]) -> None:
    """DuckDB creates its spill folder when it first spills, but not that folder's parent: on a
    fresh checkout, the first build too big for memory failed."""
    Path(environment["OWL_DUCKDB_TEMP_DIRECTORY"]).mkdir(parents=True, exist_ok=True)


def dbt_threads(config: Configuration) -> int:
    threads = config.get_int("transform.dbt_threads")
    if threads < 1:
        raise ValueError(f"'transform.dbt_threads' must be at least 1: {threads}")
    return threads


def dagster_dbt_environment(config: Configuration, mode: PipelineMode) -> dict[str, str]:
    """Each code location parses into a folder of its own, or the two would overwrite each
    other's parse. Its DuckDB is a file there, which dagster-dbt reopens after a build."""
    location_dir = dbt_location_dir(config, mode)
    return {
        "DBT_TARGET_PATH": location_dir,
        "OWL_DBT_DUCKDB_PATH": f"{location_dir}/dagster.duckdb",
    }


def dbt_main_state_dir(config: Configuration, mode: PipelineMode) -> str:
    """Where a code location keeps its last build's manifest, which the dev target defers to."""
    return f"{dbt_location_dir(config, mode)}/main"


def dbt_location_dir(config: Configuration, mode: PipelineMode) -> str:
    return f"{dbt_target_dir(config)}/{mode.value}"


def dbt_target_dir(config: Configuration) -> str:
    return f"{config.get_str('transform.project_dir').rstrip('/')}/target"


def set_dbt_environment(variables: Mapping[str, str]) -> None:
    """Set when the definitions load, so the parse and every run see the same values: dbt
    re-parses whenever they differ from the saved parse's."""
    environment = os.environ  # noqa: TID251 - sets variables for dbt; reads no config
    environment.update(variables)


@contextmanager
def environment_variable(name: str, value: str) -> Iterator[None]:
    """For the dbt process started in this block. Not --vars: dbt re-parses whenever they differ
    from the startup parse's. Steps never run side by side in one process."""
    environment = os.environ  # noqa: TID251 - sets a variable for dbt; reads no config
    previous = environment.get(name)
    environment[name] = value
    try:
        yield
    finally:
        if previous is None:
            del environment[name]
        else:
            environment[name] = previous
