from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from leads_config.configuration import SECRET_NAME_KEY, SECRET_STORE_KEY, Configuration

MEMORY_STORE = "memory"


def http_table(
    timeout: float = 300,
    attempts: int = 5,
    backoff: float = 1,
    max_wait: float = 300,
    interval: float = 0,
) -> dict[str, float]:
    """A source's `<source>.http` table; a new one each time, so that overriding one source's
    leaves the others' as they were."""
    return {
        "timeout_seconds": timeout,
        "attempts": attempts,
        "backoff_seconds": backoff,
        "max_wait_seconds": max_wait,
        "min_interval_seconds": interval,
    }


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
        "http": http_table(),
    },
    "jar": {
        "download_url_template": "https://jar.test/{dataset}/:format/csv",
        "snapshot_schedule": "0 6 21 * *",
        "http": http_table(),
    },
    "address_register": {
        "download_url_template": "https://ar.test/{dataset}/:format/csv",
        "snapshot_schedule": "0 6 21 * *",
        "http": http_table(),
    },
    "vmi": {
        "register_url": "https://vmi.test/MokesciuMoketojas",
        "activities_url": "https://vmi.test/EkonominesVeiklosRusis",
        "daily_changes_schedule": "0 6 * * *",
        "first_daily_changes": "2026-09-01",
        "monthly_register_schedule": "0 6 21 * *",
        "daily_changes_http": http_table(timeout=30, interval=1),
        "monthly_register_http": http_table(),
    },
    "google_places": {
        "api_key": {SECRET_STORE_KEY: MEMORY_STORE, SECRET_NAME_KEY: "google_places_api_key"},
        "search_text_url": "https://places.test/v1/places:searchText",
        "snapshot_schedule": "0 6 21 * *",
        "queries": ["kavinė", "kirpykla", "autoservisas"],
        "language_code": "lt",
        "region_code": "LT",
        "max_tile_meters": 1000,
        "min_tile_meters": 100,
        "max_paid_requests": 1500,
        "workers": 4,
        "http": http_table(timeout=30, attempts=10, backoff=2, max_wait=60),
    },
    "transform": {"project_dir": "/workspace/services/transform", "duckdb_memory_limit": "2GB"},
    "dagster": {"webserver": {"host": "0.0.0.0", "port": 3000}},
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
