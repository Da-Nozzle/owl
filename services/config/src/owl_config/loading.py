import logging.config
import tomllib
from collections.abc import Mapping
from pathlib import Path

from owl_config.configuration import Configuration
from owl_config.secret_stores import build_secret_store

MOUNTED_CONFIG_DIRECTORY = Path("/etc/owl")


def load_configuration(directory: Path = MOUNTED_CONFIG_DIRECTORY) -> Configuration:
    values = read_config_directory(directory)
    return Configuration(values=values, secret_store=build_secret_store(values))


def read_config_directory(directory: Path) -> dict[str, object]:
    """Merges every TOML file in the directory; a top-level table may live in one file only."""
    if not directory.is_dir():
        raise NotADirectoryError(f"Config directory {directory} does not exist")
    merged: dict[str, object] = {}
    owner_file_by_table: dict[str, Path] = {}
    for config_file in sorted(directory.glob("*.toml")):
        with config_file.open("rb") as handle:
            file_values = tomllib.load(handle)
        for table, value in file_values.items():
            if table in merged:
                raise ValueError(
                    f"[{table}] is defined in both {owner_file_by_table[table].name} "
                    f"and {config_file.name}"
                )
            merged[table] = value
            owner_file_by_table[table] = config_file
    return merged


def configure_logging(config: Configuration) -> None:
    settings = config.get_table("logging")
    create_log_file_directories(settings)
    logging.config.dictConfig(dict(settings))


def create_log_file_directories(settings: Mapping[str, object]) -> None:
    """File handlers fail on a missing directory, and a fresh checkout has none."""
    handlers = settings.get("handlers", {})
    if not isinstance(handlers, Mapping):
        return
    for handler in handlers.values():
        if isinstance(handler, Mapping) and isinstance(handler.get("filename"), str):
            Path(handler["filename"]).parent.mkdir(parents=True, exist_ok=True)
