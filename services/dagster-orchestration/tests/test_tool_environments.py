from leads_config.configuration import Configuration
from leads_config.in_memory_secret_store import InMemorySecretStore
from leads_dagster_orchestration.tool_environments import (
    dagster_dbt_environment,
    dagster_storage_environment,
    dbt_dev_environment,
    dbt_environment,
)


def build_local_like_config(password: str) -> Configuration:
    return Configuration(
        values={
            "postgres": {
                "host": "postgres",
                "port": 5432,
                "user": "leads",
                "password": {"secret_store": "memory", "secret_name": "postgres_password"},
                "databases": {"dagster": "dagster", "ducklake_catalog": "ducklake_catalog"},
            },
            "lake": {
                "data_path": "s3://leads-lake/",
                "archive_path": "s3://leads-lake/archive/",
                "metadata_schema": "ducklake",
                "s3": {
                    "endpoint": "s3mock:9090",
                    "region": "eu-central-1",
                    "url_style": "path",
                    "use_ssl": False,
                    "key_id": "s3mock",
                    "secret": "s3mock",
                },
            },
            "pipeline": {
                "mode": "replay",
                "first_month": "2026-09",
                "run_day": 21,
                "run_hour": 6,
                "timezone": "Europe/Vilnius",
                "user_agent": "test",
                "replay_path": "/workspace/replay",
            },
            "sodra": {
                "download_url_template": "https://sodra.test/{year}",
                "history_months": 13,
            },
            "transform": {"project_dir": "/workspace/services/transform"},
        },
        secret_stores={"memory": InMemorySecretStore({"postgres_password": password})},
    )


def test_dagster_gets_the_resolved_postgres_password() -> None:
    # Arrange
    config = build_local_like_config(password="s3cret")

    # Act
    environment = dagster_storage_environment(config)

    # Assert
    assert environment["DAGSTER_PG_PASSWORD"] == "s3cret"
    assert environment["DAGSTER_PG_DB"] == "dagster"


def test_ducklake_catalog_connection_quotes_awkward_passwords() -> None:
    # Arrange
    config = build_local_like_config(password="it's a pass")

    # Act
    catalog = dbt_environment(config)["LEADS_DUCKLAKE_CATALOG"]

    # Assert
    assert catalog == (
        "host='postgres' port='5432' dbname='ducklake_catalog' user='leads' "
        "password='it\\'s a pass'"
    )


def test_dbt_reads_ssl_flag_as_lowercase_text() -> None:
    # Arrange
    config = build_local_like_config(password="s3cret")

    # Act
    use_ssl = dbt_environment(config)["LEADS_S3_USE_SSL"]

    # Assert
    assert use_ssl == "false"


def test_dbt_reads_the_replay_archive_in_replay_mode() -> None:
    # Arrange
    config = build_local_like_config(password="s3cret")

    # Act
    environment = dbt_environment(config)

    # Assert
    assert environment["LEADS_ARCHIVE_PATH"] == "/workspace/replay"
    assert environment["LEADS_SODRA_HISTORY_MONTHS"] == "13"


def test_dagster_keeps_dbts_duckdb_in_the_target_directory() -> None:
    # Arrange
    config = build_local_like_config(password="s3cret")

    # Act
    environment = dagster_dbt_environment(config)

    # Assert
    assert environment == {
        "LEADS_DBT_DUCKDB_PATH": "/workspace/services/transform/target/dagster.duckdb"
    }


def test_dbt_dev_builds_into_its_own_file_and_defers_to_mains_state() -> None:
    # Arrange
    config = build_local_like_config(password="s3cret")

    # Act
    environment = dbt_dev_environment(config)

    # Assert
    assert environment == {
        "DBT_TARGET": "dev",
        "DBT_TARGET_PATH": "/workspace/services/transform/target/dev",
        "LEADS_DBT_DEV_PATH": "/workspace/services/transform/target/dev/dev.duckdb",
        "DBT_DEFER": "true",
        "DBT_STATE": "/workspace/services/transform/target/main",
    }
