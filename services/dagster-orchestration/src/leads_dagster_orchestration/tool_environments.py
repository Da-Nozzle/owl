from leads_config.configuration import Configuration
from leads_ingest.settings import PipelineMode, load_pipeline_settings, raw_archive_path
from leads_lake.settings import LakeCatalog, load_lake_settings


def dagster_storage_environment(config: Configuration) -> dict[str, str]:
    """Values that the instance's dagster.yaml reads from the environment."""
    return {
        "DAGSTER_PG_HOST": config.get_str("postgres.host"),
        "DAGSTER_PG_PORT": str(config.get_int("postgres.port")),
        "DAGSTER_PG_USER": config.get_str("postgres.user"),
        "DAGSTER_PG_PASSWORD": config.get_str("postgres.password"),
        "DAGSTER_PG_DB": config.get_str("postgres.databases.dagster"),
    }


def dbt_environment(config: Configuration, mode: PipelineMode) -> dict[str, str]:
    """Values that the dbt project reads through env_var(): the mode's lake and the archive it
    reads."""
    pipeline = load_pipeline_settings(config)
    lake = load_lake_settings(config, replay=mode is PipelineMode.REPLAY)
    return {
        "LEADS_ARCHIVE_PATH": raw_archive_path(mode, pipeline, lake.storage),
        "DBT_PROJECT_DIR": config.get_str("transform.project_dir"),
        "DBT_PROFILES_DIR": config.get_str("transform.project_dir"),
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


def dagster_dbt_environment(config: Configuration, mode: PipelineMode) -> dict[str, str]:
    """dbt under a code location keeps its artifacts in that location's folder, target/<mode>/:
    the parse made at load, its partial parse and each run's folder (dagster-dbt reads
    DBT_TARGET_PATH for the last two). The two code servers parse the same project with each
    lake's values, so sharing target/ each would overwrite the other's parse and re-parse.

    Its DuckDB main database is a file there instead of memory. After a build, dagster-dbt
    reopens that database read-only to read each table's columns and row count, which it cannot
    do for `:memory:`. The file holds nothing (models build into the lake); run by hand, dbt
    stays in memory, so it never waits on the file's lock."""
    location_dir = dbt_location_dir(config, mode)
    return {
        "DBT_TARGET_PATH": location_dir,
        "LEADS_DBT_DUCKDB_PATH": f"{location_dir}/dagster.duckdb",
    }


def dbt_dev_environment(config: Configuration, mode: PipelineMode) -> dict[str, str]:
    """dbt on the dev target (leads-dbt-dev), for iterating on models without running the
    pipeline: it builds into a DuckDB file of its own and reads the mode's lake read-only. Every
    model it does not build resolves to the lake's table, as that location's last build named it
    (dbt's defer). Its artifacts are kept apart from the location's, so neither invalidates the
    other's partial parse, and each lake has its own dev copy, so one never shadows the other's
    tables."""
    dev_dir = f"{dbt_location_dir(config, mode)}/dev"
    return {
        "DBT_TARGET": "dev",
        "DBT_TARGET_PATH": dev_dir,
        "LEADS_DBT_DEV_PATH": f"{dev_dir}/dev.duckdb",
        "DBT_DEFER": "true",
        "DBT_STATE": dbt_main_state_dir(config, mode),
    }


def dbt_main_state_dir(config: Configuration, mode: PipelineMode) -> str:
    """Where a code location keeps the manifest of its last dbt build: the state the dev target
    defers to."""
    return f"{dbt_location_dir(config, mode)}/main"


def dbt_location_dir(config: Configuration, mode: PipelineMode) -> str:
    return f"{dbt_target_dir(config)}/{mode.value}"


def dbt_target_dir(config: Configuration) -> str:
    return f"{config.get_str('transform.project_dir').rstrip('/')}/target"


def ducklake_catalog_connection(catalog: LakeCatalog) -> str:
    return libpq_connection_string(
        {
            "host": catalog.host,
            "port": str(catalog.port),
            "dbname": catalog.database,
            "user": catalog.user,
            "password": catalog.password,
        }
    )


def libpq_connection_string(parameters: dict[str, str]) -> str:
    return " ".join(f"{key}={quote_libpq_value(value)}" for key, value in parameters.items())


def quote_libpq_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"
