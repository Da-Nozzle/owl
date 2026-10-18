import pytest

from leads_config.configuration import Configuration
from leads_testing.config import InMemorySecretStore


def test_plain_setting_is_found_by_dotted_key() -> None:
    # Arrange
    config = Configuration(values={"postgres": {"host": "postgres"}})

    # Act
    host = config.get_str("postgres.host")

    # Assert
    assert host == "postgres"


def test_secret_is_fetched_from_the_secret_store() -> None:
    # Arrange
    store = InMemorySecretStore({"google-places-api-key": "abc123"})
    config = Configuration(
        values={"google": {"api_key": {"secret_name": "google-places-api-key"}}},
        secret_store=store,
    )

    # Act
    api_key = config.get_str("google.api_key")

    # Assert
    assert api_key == "abc123"


def test_secret_is_fetched_once_per_process() -> None:
    # Arrange
    store = InMemorySecretStore({"postgres_password": "s3cret"})
    config = Configuration(
        values={"postgres": {"password": {"secret_name": "postgres_password"}}},
        secret_store=store,
    )

    # Act
    config.get_str("postgres.password")
    config.get_str("postgres.password")

    # Assert
    assert store.requested_names == ["postgres_password"]


def test_secret_reference_without_a_secret_store_fails_when_config_loads() -> None:
    # Arrange
    values = {"google": {"api_key": {"secret_name": "key"}}}

    # Act / Assert
    with pytest.raises(ValueError, match=r"No secret store .*'google\.api_key'"):
        Configuration(values=values)


def test_secret_name_that_is_not_a_string_is_rejected() -> None:
    # Arrange
    config = Configuration(
        values={"postgres": {"password": {"secret_name": 5}}},
        secret_store=InMemorySecretStore({}),
    )

    # Act / Assert
    with pytest.raises(TypeError, match="secret_name"):
        config.get_str("postgres.password")


def test_missing_key_names_the_key() -> None:
    # Arrange
    config = Configuration(values={"postgres": {}})

    # Act / Assert
    with pytest.raises(KeyError, match=r"postgres\.port"):
        config.get_int("postgres.port")


def test_a_bool_is_not_an_int() -> None:
    # Arrange
    config = Configuration(values={"postgres": {"port": True}})

    # Act / Assert
    with pytest.raises(TypeError, match=r"'postgres\.port' is bool, expected int"):
        config.get_int("postgres.port")


@pytest.mark.parametrize("value", ["false", 0])
def test_a_string_or_an_int_is_not_a_bool(value: object) -> None:
    # Arrange
    config = Configuration(values={"lake": {"s3": {"use_ssl": value}}})

    # Act / Assert
    with pytest.raises(TypeError, match=r"'lake\.s3\.use_ssl' is .*, expected bool"):
        config.get_bool("lake.s3.use_ssl")


def test_a_table_is_found_by_dotted_key() -> None:
    # Arrange
    config = Configuration(values={"logging": {"root": {"level": "INFO"}}})

    # Act
    table = config.get_table("logging.root")

    # Assert
    assert table == {"level": "INFO"}


def test_a_secret_reference_is_not_a_table() -> None:
    # Arrange
    config = Configuration(
        values={"google": {"api_key": {"secret_name": "key"}}},
        secret_store=InMemorySecretStore({"key": "abc123"}),
    )

    # Act / Assert
    with pytest.raises(TypeError, match="expected a table"):
        config.get_table("google.api_key")


@pytest.mark.parametrize(("value", "number"), [(30, 30.0), (0.5, 0.5)])
def test_a_number_is_an_int_or_a_float(value: float, number: float) -> None:
    # Arrange
    config = Configuration(values={"sodra": {"http": {"timeout_seconds": value}}})

    # Act
    found = config.get_float("sodra.http.timeout_seconds")

    # Assert
    assert (found, type(found)) == (number, float)


@pytest.mark.parametrize("value", [True, "30"])
def test_a_bool_or_a_string_is_not_a_number(value: object) -> None:
    # Arrange
    config = Configuration(values={"sodra": {"http": {"timeout_seconds": value}}})

    # Act / Assert
    with pytest.raises(TypeError, match="expected a number"):
        config.get_float("sodra.http.timeout_seconds")
