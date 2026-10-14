from leads_config.configuration import Configuration


# TODO consider making proper classes for these config <-> env mappings
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
    return {
        "DBT_PROJECT_DIR": config.get_str("transform.project_dir"),
        "DBT_PROFILES_DIR": config.get_str("transform.project_dir"),
        "LEADS_DUCKLAKE_CATALOG": ducklake_catalog_connection(config),
        "LEADS_LAKE_DATA_PATH": config.get_str("lake.data_path"),
        "LEADS_S3_ENDPOINT": config.get_str("lake.s3.endpoint"),
        "LEADS_S3_REGION": config.get_str("lake.s3.region"),
        "LEADS_S3_URL_STYLE": config.get_str("lake.s3.url_style"),
        "LEADS_S3_USE_SSL": "true" if config.get("lake.s3.use_ssl") is True else "false",
        "LEADS_S3_KEY_ID": config.get_str("lake.s3.key_id"),
        "LEADS_S3_SECRET": config.get_str("lake.s3.secret"),
    }


def ducklake_catalog_connection(config: Configuration) -> str:
    return libpq_connection_string(
        {
            "host": config.get_str("postgres.host"),
            "port": str(config.get_int("postgres.port")),
            "dbname": config.get_str("postgres.databases.ducklake_catalog"),
            "user": config.get_str("postgres.user"),
            "password": config.get_str("postgres.password"),
        }
    )


def libpq_connection_string(parameters: dict[str, str]) -> str:
    return " ".join(f"{key}={quote_libpq_value(value)}" for key, value in parameters.items())


def quote_libpq_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"
