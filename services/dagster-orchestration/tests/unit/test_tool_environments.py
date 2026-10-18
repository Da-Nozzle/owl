import duckdb
import pytest
from hypothesis import given
from hypothesis import strategies as st
from psycopg2.extensions import parse_dsn

from leads_config.configuration import Configuration
from leads_dagster_orchestration.dbt.environment import dagster_dbt_environment, dbt_environment
from leads_dagster_orchestration.dev_tools.dagster_dev import (
    dagster_storage_environment,
    dagster_webserver_url,
)
from leads_dagster_orchestration.dev_tools.dbt_cli import (
    dbt_dev_environment,
    vscode_dbt_environment,
)
from leads_dagster_orchestration.dev_tools.mcp_servers import lake_init_sql
from leads_ingest.settings import PipelineMode
from leads_lake.connection import libpq_connection_string
from leads_testing.config import build_config, secret_reference


def config_with_password(password: str) -> Configuration:
    return build_config(
        {"postgres.password": secret_reference("postgres_password")},
        secrets={"postgres_password": password},
    )


def test_dagster_gets_the_resolved_postgres_password() -> None:
    # Arrange
    config = config_with_password("s3cret")

    # Act
    environment = dagster_storage_environment(config)

    # Assert
    assert environment["DAGSTER_PG_PASSWORD"] == "s3cret"
    assert environment["DAGSTER_PG_DB"] == "dagster"


def test_ducklake_catalog_connection_quotes_awkward_passwords() -> None:
    # Arrange
    config = config_with_password("it's a pass")

    # Act
    catalog = dbt_environment(config, PipelineMode.LIVE)["LEADS_DUCKLAKE_CATALOG"]

    # Assert
    assert catalog == (
        "host='postgres' port='5432' dbname='ducklake_catalog' user='leads' "
        "password='it\\'s a pass'"
    )


@given(
    st.dictionaries(
        keys=st.sampled_from(["host", "port", "dbname", "user", "password"]),
        values=st.text(alphabet=st.characters(codec="utf-8", exclude_characters="\x00")),
    )
)
def test_libpq_reads_back_every_value_as_written(parameters: dict[str, str]) -> None:
    # Act
    parsed = parse_dsn(libpq_connection_string(parameters))

    # Assert
    assert parsed == parameters


@pytest.mark.parametrize(("use_ssl", "text"), [(False, "false"), (True, "true")])
def test_dbt_reads_ssl_flag_as_lowercase_text(use_ssl: bool, text: str) -> None:
    # Arrange
    config = build_config({"lake.s3.use_ssl": use_ssl})

    # Act
    environment = dbt_environment(config, PipelineMode.LIVE)

    # Assert
    assert environment["LEADS_S3_USE_SSL"] == text


def test_dbt_reads_the_replay_archive_into_the_replay_lake_in_replay_mode() -> None:
    # Arrange
    config = build_config(
        {
            "pipeline.replay_path": "/workspace/replay",
            "lake.replay.metadata_schema": "ducklake_replay",
            "lake.replay.data_path": "s3://leads-lake/replay/",
        }
    )

    # Act
    environment = dbt_environment(config, PipelineMode.REPLAY)

    # Assert
    assert environment["LEADS_ARCHIVE_PATH"] == "/workspace/replay"
    assert environment["LEADS_DUCKLAKE_METADATA_SCHEMA"] == "ducklake_replay"
    assert environment["LEADS_LAKE_DATA_PATH"] == "s3://leads-lake/replay/"


def test_dbt_reads_the_lakes_archive_into_the_live_lake_in_live_mode() -> None:
    # Arrange
    config = build_config(
        {
            "lake.archive_path": "s3://leads-lake/archive/",
            "lake.metadata_schema": "ducklake",
            "lake.data_path": "s3://leads-lake/",
        }
    )

    # Act
    environment = dbt_environment(config, PipelineMode.LIVE)

    # Assert
    assert environment["LEADS_ARCHIVE_PATH"] == "s3://leads-lake/archive/"
    assert environment["LEADS_DUCKLAKE_METADATA_SCHEMA"] == "ducklake"
    assert environment["LEADS_LAKE_DATA_PATH"] == "s3://leads-lake/"


@pytest.mark.parametrize("project_dir", ["/project", "/project/"])
@pytest.mark.parametrize("mode", list(PipelineMode))
def test_each_code_location_keeps_dbts_artifacts_in_a_folder_of_its_own(
    project_dir: str, mode: PipelineMode
) -> None:
    # Arrange
    config = build_config({"transform.project_dir": project_dir})

    # Act
    environment = dagster_dbt_environment(config, mode)

    # Assert
    assert environment == {
        "DBT_TARGET_PATH": f"/project/target/{mode.value}",
        "LEADS_DBT_DUCKDB_PATH": f"/project/target/{mode.value}/dagster.duckdb",
    }


@pytest.mark.parametrize("mode", list(PipelineMode))
def test_dbt_dev_builds_into_its_lakes_own_file_and_defers_to_its_lakes_state(
    mode: PipelineMode,
) -> None:
    # Arrange
    config = build_config({"transform.project_dir": "/project"})

    # Act
    environment = dbt_dev_environment(config, mode)

    # Assert
    assert environment == {
        "DBT_TARGET": "dev",
        "DBT_TARGET_PATH": f"/project/target/{mode.value}/dev",
        "LEADS_DBT_DEV_PATH": f"/project/target/{mode.value}/dev/dev.duckdb",
        "DBT_DEFER": "true",
        "DBT_STATE": f"/project/target/{mode.value}/main",
    }


@pytest.mark.parametrize("mode", list(PipelineMode))
def test_the_vscode_extension_parses_into_a_folder_and_duckdb_file_of_its_own(
    mode: PipelineMode,
) -> None:
    # Arrange
    config = build_config({"transform.project_dir": "/project"})

    # Act
    environment = vscode_dbt_environment(config, mode)

    # Assert
    assert environment == {
        "DBT_TARGET_PATH": f"/project/target/{mode.value}/vscode",
        "LEADS_DBT_DEV_PATH": f"/project/target/{mode.value}/vscode/editor.duckdb",
    }


def test_dbt_caps_duckdbs_memory_and_spills_under_the_projects_target() -> None:
    # Arrange
    config = build_config(
        {"transform.project_dir": "/project", "transform.duckdb_memory_limit": "1GB"}
    )

    # Act
    environment = dbt_environment(config, PipelineMode.LIVE)

    # Assert
    assert environment["LEADS_DUCKDB_MEMORY_LIMIT"] == "1GB"
    assert environment["LEADS_DUCKDB_TEMP_DIRECTORY"] == "/project/target/duckdb_tmp"


def test_dbt_builds_as_many_models_at_once_as_configured() -> None:
    # Arrange
    config = build_config({"transform.dbt_threads": 2})

    # Act
    environment = dbt_environment(config, PipelineMode.LIVE)

    # Assert
    assert environment["LEADS_DBT_THREADS"] == "2"


def test_dbt_needs_at_least_one_thread() -> None:
    # Arrange
    config = build_config({"transform.dbt_threads": 0})

    # Act / Assert
    with pytest.raises(ValueError, match="dbt_threads"):
        dbt_environment(config, PipelineMode.LIVE)


def test_the_lake_mcp_server_attaches_both_lakes_under_their_names() -> None:
    # Arrange
    config = build_config(
        {
            "lake.metadata_schema": "ducklake",
            "lake.data_path": "s3://leads-lake/",
            "lake.replay.metadata_schema": "ducklake_replay",
            "lake.replay.data_path": "s3://leads-lake/replay/",
        }
    )

    # Act
    sql = lake_init_sql(config)

    # Assert
    assert " AS live (DATA_PATH 's3://leads-lake/', METADATA_SCHEMA 'ducklake')" in sql
    assert (
        " AS replay (DATA_PATH 's3://leads-lake/replay/', METADATA_SCHEMA 'ducklake_replay')" in sql
    )


def test_the_lake_mcp_server_caps_duckdbs_memory_as_dbt_does() -> None:
    # Arrange
    config = build_config(
        {"transform.project_dir": "/project", "transform.duckdb_memory_limit": "1GB"}
    )

    # Act
    sql = lake_init_sql(config)

    # Assert
    assert "SET memory_limit = '1GB';" in sql
    assert "SET temp_directory = '/project/target/duckdb_tmp/mcp';" in sql


def test_the_lake_mcp_servers_startup_sql_parses_with_awkward_credentials() -> None:
    # Arrange
    config = build_config(
        {"postgres.password": secret_reference("postgres_password"), "lake.s3.secret": "a'b"},
        secrets={"postgres_password": "it's a pass"},
    )

    # Act
    statements = duckdb.extract_statements(lake_init_sql(config))

    # Assert
    assert [statement.type.name for statement in statements][-3:] == ["CREATE", "ATTACH", "ATTACH"]


@pytest.mark.parametrize(
    ("host", "url"),
    [
        ("0.0.0.0", "http://localhost:3000"),
        ("::", "http://localhost:3000"),
        ("dagster-webserver", "http://dagster-webserver:3000"),
    ],
)
def test_the_dagster_mcp_server_connects_to_the_webservers_address(host: str, url: str) -> None:
    # Arrange
    config = build_config({"dagster.webserver.host": host, "dagster.webserver.port": 3000})

    # Act / Assert
    assert dagster_webserver_url(config) == url
