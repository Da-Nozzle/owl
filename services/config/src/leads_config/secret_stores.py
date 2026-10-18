from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from leads_config.configuration import SecretStore

SECRET_STORE_TABLE = "secret_store"


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


def build_secret_store(values: Mapping[str, object]) -> SecretStore | None:
    settings = values.get(SECRET_STORE_TABLE)
    if settings is None:
        return None
    if not isinstance(settings, Mapping):
        raise TypeError(f"[{SECRET_STORE_TABLE}] must be a table")
    match settings.get("kind"):
        case "file":
            return build_file_secret_store(settings)
        case unknown_kind:
            raise ValueError(f"[{SECRET_STORE_TABLE}] has unknown kind {unknown_kind!r}")


def build_file_secret_store(settings: Mapping[str, object]) -> FileSecretStore:
    directory = settings.get("directory")
    if not isinstance(directory, str):
        raise TypeError(f"A file [{SECRET_STORE_TABLE}] needs a 'directory' string")
    return FileSecretStore(directory=Path(directory))
