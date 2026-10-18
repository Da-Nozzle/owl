from pathlib import Path

import pytest

from leads_config.loading import (
    create_log_file_directories,
    load_configuration,
    read_config_directory,
)
from leads_config.secret_stores import FileSecretStore


def test_files_in_one_directory_merge_into_one_tree(tmp_path: Path) -> None:
    # Arrange
    (tmp_path / "pipeline.toml").write_text('[pipeline]\nmode = "replay"\n')
    (tmp_path / "logging.toml").write_text("[logging]\nversion = 1\n")

    # Act
    values = read_config_directory(tmp_path)

    # Assert
    assert values == {"pipeline": {"mode": "replay"}, "logging": {"version": 1}}


def test_same_top_level_table_in_two_files_is_an_error(tmp_path: Path) -> None:
    # Arrange
    (tmp_path / "a.toml").write_text('[postgres]\nhost = "a"\n')
    (tmp_path / "b.toml").write_text("[postgres]\nport = 5432\n")

    # Act / Assert
    with pytest.raises(ValueError, match=r"\[postgres\] is defined in both a.toml and b.toml"):
        read_config_directory(tmp_path)


def test_file_store_secret_resolves_from_its_directory(tmp_path: Path) -> None:
    # Arrange
    secrets_directory = tmp_path / "secrets"
    secrets_directory.mkdir()
    (secrets_directory / "postgres_password").write_text("s3cret\n")
    config_directory = tmp_path / "config"
    config_directory.mkdir()
    (config_directory / "pipeline.toml").write_text(
        "[postgres]\n"
        'password = { secret_name = "postgres_password" }\n'
        "\n"
        "[secret_store]\n"
        'kind = "file"\n'
        f'directory = "{secrets_directory.as_posix()}"\n'
    )

    # Act
    password = load_configuration(config_directory).get_str("postgres.password")

    # Assert
    assert password == "s3cret"


def test_a_missing_config_directory_is_named(tmp_path: Path) -> None:
    # Act / Assert
    with pytest.raises(NotADirectoryError, match="absent"):
        read_config_directory(tmp_path / "absent")


def test_an_empty_secret_file_is_rejected(tmp_path: Path) -> None:
    # Arrange
    (tmp_path / "postgres_password").write_text("\n")

    # Act / Assert
    with pytest.raises(ValueError, match="postgres_password is empty"):
        FileSecretStore(tmp_path).fetch("postgres_password", {})


def test_log_file_directory_is_created_before_logging_starts(tmp_path: Path) -> None:
    # Arrange
    log_file = tmp_path / "logs" / "leads.log"
    settings = {"handlers": {"file": {"filename": str(log_file)}, "console": {}}}

    # Act
    create_log_file_directories(settings)

    # Assert
    assert log_file.parent.is_dir()
