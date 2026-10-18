from collections.abc import Iterator, Mapping
from typing import Protocol

SECRET_NAME_KEY = "secret_name"
SECRET_ARGS_KEY = "secret_args"


class SecretStore(Protocol):
    def fetch(self, name: str, args: Mapping[str, str]) -> str: ...


class Configuration:
    """Settings looked up by dotted key; a secret reference resolves on first request."""

    def __init__(
        self, values: Mapping[str, object], secret_store: SecretStore | None = None
    ) -> None:
        secret_keys = sorted(secret_reference_keys(values))
        if secret_keys and secret_store is None:
            raise ValueError(f"No secret store declared for the secrets at {secret_keys}")
        self._values = values
        self._secret_store = secret_store
        self._resolved_secrets: dict[str, str] = {}

    def get(self, key: str) -> object:
        value = self._lookup(key)
        if is_secret_reference(value):
            return self._resolve_secret(key, value)
        return value

    def get_str(self, key: str) -> str:
        value = self.get(key)
        if not isinstance(value, str):
            raise TypeError(f"{key!r} is {type(value).__name__}, expected str")
        return value

    def get_int(self, key: str) -> int:
        value = self.get(key)
        if not isinstance(value, int) or isinstance(value, bool):
            raise TypeError(f"{key!r} is {type(value).__name__}, expected int")
        return value

    def get_bool(self, key: str) -> bool:
        value = self.get(key)
        if not isinstance(value, bool):
            raise TypeError(f"{key!r} is {type(value).__name__}, expected bool")
        return value

    def get_float(self, key: str) -> float:
        """TOML writes a whole number as an int, so either is accepted."""
        value = self.get(key)
        if not isinstance(value, int | float) or isinstance(value, bool):
            raise TypeError(f"{key!r} is {type(value).__name__}, expected a number")
        return float(value)

    def get_table(self, key: str) -> Mapping[str, object]:
        value = self._lookup(key)
        if not isinstance(value, Mapping) or is_secret_reference(value):
            raise TypeError(f"{key!r} is {type(value).__name__}, expected a table")
        return value

    def _lookup(self, key: str) -> object:
        value: object = self._values
        for part in key.split("."):
            if not isinstance(value, Mapping) or part not in value:
                raise KeyError(key)
            value = value[part]
        return value

    def _resolve_secret(self, key: str, reference: Mapping[str, object]) -> str:
        if key not in self._resolved_secrets:
            name = reference[SECRET_NAME_KEY]
            if not isinstance(name, str):
                raise TypeError(f"{SECRET_NAME_KEY!r} at {key!r} must be a string")
            args = reference.get(SECRET_ARGS_KEY, {})
            if not isinstance(args, Mapping):
                raise TypeError(f"{SECRET_ARGS_KEY!r} at {key!r} must be a table")
            if self._secret_store is None:
                raise ValueError(f"No secret store declared for the secret at {key!r}")
            self._resolved_secrets[key] = self._secret_store.fetch(name, args)
        return self._resolved_secrets[key]


def is_secret_reference(value: object) -> bool:
    return isinstance(value, Mapping) and SECRET_NAME_KEY in value


def secret_reference_keys(values: Mapping[str, object], prefix: str = "") -> Iterator[str]:
    for name, value in values.items():
        key = f"{prefix}{name}"
        if is_secret_reference(value):
            yield key
        elif isinstance(value, Mapping):
            yield from secret_reference_keys(value, prefix=f"{key}.")
