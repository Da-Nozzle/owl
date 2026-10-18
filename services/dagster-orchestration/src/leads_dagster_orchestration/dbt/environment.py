import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager

from leads_config.configuration import Configuration
from leads_ingest.settings import PipelineMode, load_pipeline_settings, raw_archive_path
from leads_lake.connection import ducklake_catalog_connection
from leads_lake.settings import load_lake_settings

# Read by the project's snapshot() macro.
SNAPSHOT_VARIABLE = "LEADS_SNAPSHOT"


def dbt_environment(config: Configuration, mode: PipelineMode) -> dict[str, str]:
    """What the dbt project reads through env_var(). The profile adds dbt's invocation id to the
    temp directory: DuckDB names its spill files the same in every process."""
    pipeline = load_pipeline_settings(config)
    lake = load_lake_settings(config, replay=mode is PipelineMode.REPLAY)
    return {
        "LEADS_ARCHIVE_PATH": raw_archive_path(mode, pipeline, lake.storage),
        "DBT_PROJECT_DIR": config.get_str("transform.project_dir"),
        "DBT_PROFILES_DIR": config.get_str("transform.project_dir"),
        "LEADS_DUCKDB_MEMORY_LIMIT": config.get_str("transform.duckdb_memory_limit"),
        "LEADS_DBT_THREADS": str(dbt_threads(config)),
        "LEADS_DUCKDB_TEMP_DIRECTORY": f"{dbt_target_dir(config)}/duckdb_tmp",
        "LEADS_DUCKLAKE_CATALOG": ducklake_catalog_connection(lake.catalog),
        "LEADS_DUCKLAKE_METADATA_SCHEMA": lake.catalog.metadata_schema,
        "LEADS_LAKE_DATA_PATH": lake.storage.data_path,
        "LEADS_S3_ENDPOINT": lake.storage.endpoint,
        "LEADS_S3_REGION": lake.storage.region,
        "LEADS_S3_URL_STYLE": lake.storage.url_style,
        "LEADS_S3_USE_SSL": "true" if lake.storage.use_ssl else "false",
        "LEADS_S3_KEY_ID": lake.storage.key_id,
        "LEADS_S3_SECRET": lake.storage.secret,
    }


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
        "LEADS_DBT_DUCKDB_PATH": f"{location_dir}/dagster.duckdb",
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
