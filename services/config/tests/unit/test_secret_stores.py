from pathlib import Path

import pytest

from owl_config.secret_stores import FileSecretStore, build_secret_store


def test_a_file_store_reads_from_its_directory() -> None:
    # Arrange
    values = {"secret_store": {"kind": "file", "directory": "/etc/owl/secrets"}}

    # Act
    store = build_secret_store(values)

    # Assert
    assert store == FileSecretStore(Path("/etc/owl/secrets"))


def test_config_without_a_secret_store_has_none() -> None:
    # Act / Assert
    assert build_secret_store({"postgres": {"host": "postgres"}}) is None


def test_a_store_of_unknown_kind_is_rejected() -> None:
    # Arrange
    values = {"secret_store": {"kind": "hashicorp"}}

    # Act / Assert
    with pytest.raises(ValueError, match="unknown kind 'hashicorp'"):
        build_secret_store(values)


def test_a_file_store_without_a_directory_is_rejected() -> None:
    # Arrange
    values = {"secret_store": {"kind": "file"}}

    # Act / Assert
    with pytest.raises(TypeError, match="needs a 'directory'"):
        build_secret_store(values)
