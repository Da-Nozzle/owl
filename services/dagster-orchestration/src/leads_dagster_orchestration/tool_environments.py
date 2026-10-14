from leads_config.configuration import Configuration
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
    return {
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
