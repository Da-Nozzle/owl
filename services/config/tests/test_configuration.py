import pytest

from leads_config.configuration import Configuration
from leads_config.in_memory_secret_store import InMemorySecretStore


def test_plain_setting_is_found_by_dotted_key() -> None:
    # Arrange
    config = Configuration(values={"postgres": {"host": "postgres"}}, secret_stores={})

    # Act
    host = config.get_str("postgres.host")

    # Assert
    assert host == "postgres"


def test_secret_is_fetched_from_the_store_the_config_names() -> None:
    # Arrange
    store = InMemorySecretStore({"google-places-api-key": "abc123"})
    config = Configuration(
        values={
            "google": {"api_key": {"secret_store": "vault", "secret_name": "google-places-api-key"}}
        },
        secret_stores={"vault": store},
    )

    # Act
    api_key = config.get_str("google.api_key")

    # Assert
    assert api_key == "abc123"


def test_secret_is_fetched_once_per_process() -> None:
    # Arrange
    store = InMemorySecretStore({"postgres_password": "s3cret"})
    config = Configuration(
        values={
            "postgres": {"password": {"secret_store": "local", "secret_name": "postgres_password"}}
        },
        secret_stores={"local": store},
    )

    # Act
    config.get_str("postgres.password")
    config.get_str("postgres.password")

    # Assert
    assert store.requested_names == ["postgres_password"]


def test_reference_to_unregistered_store_fails_when_config_loads() -> None:
    # Arrange
    values = {"google": {"api_key": {"secret_store": "aws", "secret_name": "key"}}}

    # Act / Assert
    with pytest.raises(ValueError, match="aws"):
        Configuration(values=values, secret_stores={})


def test_missing_key_names_the_key() -> None:
    # Arrange
    config = Configuration(values={"postgres": {}}, secret_stores={})

    # Act / Assert
    with pytest.raises(KeyError, match=r"postgres\.port"):
        config.get_int("postgres.port")
