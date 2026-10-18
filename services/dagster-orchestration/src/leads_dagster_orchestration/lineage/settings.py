from dataclasses import dataclass

from leads_config.configuration import Configuration
from leads_http.settings import HttpClientSettings, load_http_client_settings


@dataclass(frozen=True, slots=True)
class LineageSettings:
    url: str
    endpoint: str
    namespace: str
    sensor_interval_seconds: int
    events_per_tick: int
    http: HttpClientSettings

    @property
    def endpoint_url(self) -> str:
        return f"{self.url.rstrip('/')}/{self.endpoint.lstrip('/')}"


def load_lineage_settings(config: Configuration) -> LineageSettings:
    return LineageSettings(
        url=config.get_str("lineage.url"),
        endpoint=config.get_str("lineage.endpoint"),
        namespace=config.get_str("lineage.namespace"),
        sensor_interval_seconds=positive(config, "lineage.sensor_interval_seconds"),
        events_per_tick=positive(config, "lineage.events_per_tick"),
        http=load_http_client_settings(config, "lineage.http"),
    )


def positive(config: Configuration, key: str) -> int:
    value = config.get_int(key)
    if value < 1:
        raise ValueError(f"{key!r} must be at least 1: {value}")
    return value
