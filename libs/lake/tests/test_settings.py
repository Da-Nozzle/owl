import pytest

from leads_config.configuration import Configuration
from leads_lake.settings import load_lake_settings


def build_config(*, use_ssl: object) -> Configuration:
    return Configuration(
        values={
            "postgres": {
                "host": "postgres",
                "port": 5432,
                "user": "leads",
                "password": "leads",
                "databases": {"ducklake_catalog": "ducklake_catalog"},
            },
            "lake": {
                "data_path": "s3://leads-lake/",
                "metadata_schema": "ducklake",
                "s3": {
                    "endpoint": "s3mock:9090",
                    "region": "eu-central-1",
                    "url_style": "path",
                    "use_ssl": use_ssl,
                    "key_id": "s3mock",
                    "secret": "s3mock",
                },
            },
        },
        secret_stores={},
    )


def test_catalog_lives_in_the_configured_postgres_database_and_schema() -> None:
    # Arrange
    config = build_config(use_ssl=False)

    # Act
    catalog = load_lake_settings(config).catalog

    # Assert
    assert (catalog.database, catalog.metadata_schema) == ("ducklake_catalog", "ducklake")


def test_ssl_flag_written_as_text_is_rejected() -> None:
    # Arrange
    config = build_config(use_ssl="false")

    # Act / Assert
    with pytest.raises(TypeError, match="use_ssl"):
        load_lake_settings(config)
