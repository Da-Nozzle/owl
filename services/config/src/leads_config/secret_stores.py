from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from leads_config.configuration import SecretStore

SECRET_STORES_TABLE = "secret_stores"


@dataclass(frozen=True, slots=True)
class FileSecretStore:
    """Reads each secret from a file named after it, as Docker and Kubernetes mount them."""

    directory: Path

    def fetch(self, name: str, args: Mapping[str, str]) -> str:
        secret_path = self.directory / name
        secret = secret_path.read_text().strip()
        if not secret:
            raise ValueError(f"Secret file {secret_path} is empty")
        return secret


def build_secret_stores(values: Mapping[str, object]) -> dict[str, SecretStore]:
    declared_stores = values.get(SECRET_STORES_TABLE, {})
    if not isinstance(declared_stores, Mapping):
        raise TypeError(f"[{SECRET_STORES_TABLE}] must be a table of stores")
    return {name: build_secret_store(name, settings) for name, settings in declared_stores.items()}


def build_secret_store(name: str, settings: object) -> SecretStore:
    if not isinstance(settings, Mapping):
        raise TypeError(f"[{SECRET_STORES_TABLE}.{name}] must be a table")
    match settings.get("kind"):
        case "file":
            return build_file_secret_store(name, settings)
        case unknown_kind:
            raise ValueError(f"Secret store {name!r} has unknown kind {unknown_kind!r}")


def build_file_secret_store(name: str, settings: Mapping[str, object]) -> FileSecretStore:
    directory = settings.get("directory")
    if not isinstance(directory, str):
        raise TypeError(f"File secret store {name!r} needs a 'directory' string")
    return FileSecretStore(directory=Path(directory))
