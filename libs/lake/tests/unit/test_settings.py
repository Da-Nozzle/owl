import pytest

from leads_lake.settings import load_lake_settings
from leads_testing.config import build_config, secret_reference


def test_catalog_lives_in_the_configured_postgres_database_and_schema() -> None:
    # Arrange
    config = build_config(
        {"postgres.databases.ducklake_catalog": "catalog_db", "lake.metadata_schema": "lake_meta"}
    )

    # Act
    catalog = load_lake_settings(config).catalog

    # Assert
    assert (catalog.database, catalog.metadata_schema) == ("catalog_db", "lake_meta")


def test_catalog_password_may_be_a_secret() -> None:
    # Arrange
    config = build_config(
        {"postgres.password": secret_reference("postgres_password")},
        secrets={"postgres_password": "s3cret"},
    )

    # Act
    catalog = load_lake_settings(config).catalog

    # Assert
    assert catalog.password == "s3cret"


def test_ssl_flag_written_as_text_is_rejected() -> None:
    # Arrange
    config = build_config({"lake.s3.use_ssl": "false"})

    # Act / Assert
    with pytest.raises(TypeError, match="use_ssl"):
        load_lake_settings(config)


def test_replay_lake_has_its_own_catalog_schema_and_data_path() -> None:
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
    live = load_lake_settings(config)
    replay = load_lake_settings(config, replay=True)

    # Assert
    assert (live.catalog.metadata_schema, live.storage.data_path) == (
        "ducklake",
        "s3://leads-lake/",
    )
    assert (replay.catalog.metadata_schema, replay.storage.data_path) == (
        "ducklake_replay",
        "s3://leads-lake/replay/",
    )
    assert replay.storage.archive_path == live.storage.archive_path
