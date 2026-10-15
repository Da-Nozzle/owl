from pathlib import Path

import pytest

from leads_config.secret_stores import FileSecretStore, build_secret_stores


def test_a_file_store_reads_from_its_directory() -> None:
    # Arrange
    values = {"secret_stores": {"local": {"kind": "file", "directory": "/etc/leads/secrets"}}}

    # Act
    stores = build_secret_stores(values)

    # Assert
    assert stores == {"local": FileSecretStore(Path("/etc/leads/secrets"))}


def test_config_without_secret_stores_has_none() -> None:
    # Act / Assert
    assert build_secret_stores({"postgres": {"host": "postgres"}}) == {}


def test_a_store_of_unknown_kind_is_rejected() -> None:
    # Arrange
    values = {"secret_stores": {"vault": {"kind": "hashicorp"}}}

    # Act / Assert
    with pytest.raises(ValueError, match="'vault' has unknown kind 'hashicorp'"):
        build_secret_stores(values)


def test_a_file_store_without_a_directory_is_rejected() -> None:
    # Arrange
    values = {"secret_stores": {"local": {"kind": "file"}}}

    # Act / Assert
    with pytest.raises(TypeError, match="'local' needs a 'directory'"):
        build_secret_stores(values)
