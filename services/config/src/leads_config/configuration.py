from collections.abc import Iterator, Mapping
from typing import Protocol

SECRET_STORE_KEY = "secret_store"  # TODO maybe convert to string enums?
SECRET_NAME_KEY = "secret_name"
SECRET_ARGS_KEY = "secret_args"


class SecretStore(Protocol):
    def fetch(self, name: str, args: Mapping[str, str]) -> str: ...


class Configuration:
    """Settings looked up by dotted key; secret references resolve on first request."""

    def __init__(
        self, values: Mapping[str, object], secret_stores: Mapping[str, SecretStore]
    ) -> None:
        referenced_stores = {
            str(reference[SECRET_STORE_KEY]) for reference in secret_references(values)
        }
        unknown_stores = referenced_stores - secret_stores.keys()
        if unknown_stores:
            raise ValueError(f"No secret store registered for {sorted(unknown_stores)}")
        self._values = values
        self._secret_stores = secret_stores
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
            store = self._secret_stores[str(reference[SECRET_STORE_KEY])]
            name = reference.get(SECRET_NAME_KEY)
            if not isinstance(name, str):
                raise ValueError(f"Secret reference at {key!r} has no {SECRET_NAME_KEY!r}")
            args = reference.get(SECRET_ARGS_KEY, {})
            if not isinstance(args, Mapping):
                raise TypeError(f"{SECRET_ARGS_KEY!r} at {key!r} must be a table")
            self._resolved_secrets[key] = store.fetch(name, args)
        return self._resolved_secrets[key]


def is_secret_reference(value: object) -> bool:
    return isinstance(value, Mapping) and SECRET_STORE_KEY in value


def secret_references(values: Mapping[str, object]) -> Iterator[Mapping[str, object]]:
    for value in values.values():
        if is_secret_reference(value):
            yield value
        elif isinstance(value, Mapping):
            yield from secret_references(value)
