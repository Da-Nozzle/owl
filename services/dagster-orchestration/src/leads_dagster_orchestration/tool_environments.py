from leads_config.configuration import Configuration
from leads_ingest.settings import load_pipeline_settings, raw_archive_path
from leads_ingest.sources.sodra.settings import load_sodra_settings
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


def dbt_environment(config: Configuration) -> dict[str, str]:
    """Values that the dbt project's profiles.yml reads through env_var()."""
    lake = load_lake_settings(config)
    pipeline = load_pipeline_settings(config)
    return {
        "LEADS_ARCHIVE_PATH": raw_archive_path(pipeline, lake.storage),
        "LEADS_SODRA_HISTORY_MONTHS": str(load_sodra_settings(config).history_months),
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


def dagster_dbt_environment(config: Configuration) -> dict[str, str]:
    """dbt under Dagster keeps its DuckDB main database in a file instead of in memory. After a
    build, dagster-dbt reopens that database read-only to read each table's columns and row count,
    which it cannot do for `:memory:`. The file holds nothing (models build into the lake); run by
    hand, dbt stays in memory, so it never waits on the file's lock."""
    return {"LEADS_DBT_DUCKDB_PATH": f"{dbt_target_dir(config)}/dagster.duckdb"}


def dbt_dev_environment(config: Configuration) -> dict[str, str]:
    """dbt on the dev target (leads-dbt-dev), for iterating on models without running the
    pipeline: it builds into a DuckDB file of its own and reads the lake read-only. Every model it
    does not build resolves to the lake's table, as main's last build named it (dbt's defer). Its
    artifacts are kept apart from main's, so neither invalidates the other's partial parse."""
    dev_dir = f"{dbt_target_dir(config)}/dev"
    return {
        "DBT_TARGET": "dev",
        "DBT_TARGET_PATH": dev_dir,
        "LEADS_DBT_DEV_PATH": f"{dev_dir}/dev.duckdb",
        "DBT_DEFER": "true",
        "DBT_STATE": dbt_main_state_dir(config),
    }


def dbt_main_state_dir(config: Configuration) -> str:
    """Where Dagster keeps the manifest of main's last dbt build: the state the dev target
    defers to."""
    return f"{dbt_target_dir(config)}/main"


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
