from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from leads_config.configuration import SECRET_NAME_KEY, SECRET_STORE_KEY, Configuration

MEMORY_STORE = "memory"

# Every setting the packages load, valued like config/local.
LOCAL_LIKE_VALUES: Mapping[str, Any] = {
    "postgres": {
        "host": "postgres",
        "port": 5432,
        "user": "leads",
        "password": "leads",
        "databases": {"dagster": "dagster", "ducklake_catalog": "ducklake_catalog"},
    },
    "lake": {
        "data_path": "s3://leads-lake/",
        "archive_path": "s3://leads-lake/archive/",
        "metadata_schema": "ducklake",
        "replay": {
            "metadata_schema": "ducklake_replay",
            "data_path": "s3://leads-lake/replay/",
        },
        "s3": {
            "endpoint": "s3mock:9090",
            "region": "eu-central-1",
            "url_style": "path",
            "use_ssl": False,
            "key_id": "s3mock",
            "secret": "s3mock",
        },
    },
    "pipeline": {
        "timezone": "Europe/Vilnius",
        "user_agent": "leads-test",
        "replay_path": "/workspace/replay",
    },
    "sodra": {
        "download_url_template": "https://sodra.test/{year}/monthly-{year}.csv.zip",
        "history_months": 13,
        "snapshot_schedule": "0 6 21 * *",
    },
    "transform": {"project_dir": "/workspace/services/transform"},
}


@dataclass(frozen=True, slots=True)
class InMemorySecretStore:
    """A SecretStore that records which names were requested."""

    secrets: Mapping[str, str]
    requested_names: list[str] = field(default_factory=list)

    def fetch(self, name: str, args: Mapping[str, str]) -> str:
        self.requested_names.append(name)
        return self.secrets[name]


def build_config(
    overrides: Mapping[str, object] | None = None, *, secrets: Mapping[str, str] | None = None
) -> Configuration:
    """A configuration like config/local with each dotted key of `overrides` replaced, e.g.
    `{"sodra.history_months": 1}`. A key that is not a setting fails, so a typo cannot pass
    unnoticed. `secrets` are in the store that `secret_reference` names."""
    values: dict[str, Any] = deepcopy(dict(LOCAL_LIKE_VALUES))
    for key, value in (overrides or {}).items():
        *parents, leaf = key.split(".")
        table = values
        for part in parents:
            table = table[part]
        if leaf not in table:
            raise KeyError(f"{key!r} is not a setting of the local-like configuration")
        table[leaf] = value
    return Configuration(
        values=values, secret_stores={MEMORY_STORE: InMemorySecretStore(secrets or {})}
    )


def secret_reference(name: str) -> dict[str, str]:
    """A setting's value that `build_config` resolves from its `secrets`."""
    return {SECRET_STORE_KEY: MEMORY_STORE, SECRET_NAME_KEY: name}
