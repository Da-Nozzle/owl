from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from owl_config.configuration import SECRET_NAME_KEY, Configuration


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
        "user": "owl",
        "password": "owl",
        "databases": {"dagster": "dagster", "ducklake_catalog": "ducklake_catalog"},
    },
    "lake": {
        "data_path": "s3://owl-lake/",
        "archive_path": "s3://owl-lake/archive/",
        "metadata_schema": "ducklake",
        "replay": {
            "metadata_schema": "ducklake_replay",
            "data_path": "s3://owl-lake/replay/",
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
        "user_agent": "owl-test",
        "replay_path": "/workspace/replay",
    },
    "lineage": {
        "url": "http://marquez:5000",
        "endpoint": "api/v1/lineage",
        "namespace": "owl",
        "sensor_interval_seconds": 30,
        "events_per_tick": 500,
        "http": http_table(timeout=10, attempts=3, max_wait=5),
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
    "jar_financial_statements": {
        "download_url_template": "https://jar.test/{dataset}/:format/csv",
        "snapshot_schedule": "0 6 21 3,6,9,12 *",
        "page_rows": 500_000,
        "fetch_max_runtime_seconds": 14_400,
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
        "changelog_page_size": 10_000,
        "register_export_attempts": 5,
        "register_export_wait_seconds": 65,
        "daily_changes_http": http_table(timeout=30, interval=1),
        "monthly_register_http": http_table(),
    },
    "vmi_tax_arrears": {
        "model_url": "https://vmi.test/NepriemokosSuma",
        "snapshot_schedule": "0 6 21 * *",
        "first_snapshot": "2024-11",
        "changelog_page_size": 20_000,
        "http": http_table(timeout=30, interval=1),
    },
    "vmi_taxes_paid": {
        "model_url": "https://vmi.test/Moketojas",
        "snapshot_schedule": "0 6 21 * *",
        "first_snapshot": "2025-06",
        "changelog_page_size": 20_000,
        "http": http_table(timeout=60, interval=1),
    },
    "google_places": {
        "api_key": {SECRET_NAME_KEY: "google_places_api_key"},
        "search_text_url": "https://places.test/v1/places:searchText",
        "snapshot_schedule": "0 6 21 * *",
        "queries": ["kavinė", "kirpykla", "autoservisas"],
        "language_code": "lt",
        "region_code": "LT",
        "max_tile_meters": 1000,
        "min_tile_meters": 100,
        "max_paid_requests": 1500,
        "workers": 4,
        "fetch_max_runtime_seconds": 10_800,
        "free_paid_requests_a_month": 1_000,
        "usd_per_paid_request": 0.035,
        "http": http_table(timeout=30, attempts=10, backoff=2, max_wait=60),
    },
    "uzt_vacancies": {
        "vacancies_url": "https://uzt.test/Vieta",
        "snapshot_schedule": "0 6 21 * *",
        "export_attempts": 6,
        "export_wait_seconds": 300,
        "page_rows": 400_000,
        "fetch_max_runtime_seconds": 7_200,
        "http": http_table(),
    },
    "archive_index": {"sensor_interval_seconds": 60},
    "transform": {
        "project_dir": "/workspace/services/transform",
        "duckdb_memory_limit": "2GB",
        "dbt_threads": 4,
    },
    "dagster": {
        "workspace": "/workspace/.devcontainer/dagster/workspace.yaml",
        "webserver": {"host": "0.0.0.0", "port": 3000},
    },
    "mcp": {
        "dbt": {"package": "dbt-mcp@2.7.0", "command_timeout_seconds": 3600},
        "lake": {"package": "mcp-server-motherduck@1.1.0"},
        "dagster": {"package": "dagster-mcp@0.12.3"},
    },
    "vscode": {"machine_settings": "/home/dev/.vscode-server/data/Machine/settings.json"},
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
    """LOCAL_LIKE_VALUES with each dotted key of `overrides` replaced, e.g.
    `{"sodra.history_months": 1}`; a key that is not a setting fails, so a typo cannot pass."""
    values = with_overrides(LOCAL_LIKE_VALUES, overrides or {})
    return Configuration(values=values, secret_store=InMemorySecretStore(secrets or {}))


def with_overrides(values: Mapping[str, Any], overrides: Mapping[str, object]) -> dict[str, Any]:
    """A copy of `values` with each dotted key of `overrides` replaced; a key that is not a
    setting fails."""
    copied: dict[str, Any] = deepcopy(dict(values))
    for key, value in overrides.items():
        *parents, leaf = key.split(".")
        table = copied
        for part in parents:
            table = table[part]
        if leaf not in table:
            raise KeyError(f"{key!r} is not a setting of the configuration")
        table[leaf] = value
    return copied


def secret_reference(name: str) -> dict[str, str]:
    """A setting's value that `build_config` resolves from its `secrets`."""
    return {SECRET_NAME_KEY: name}
