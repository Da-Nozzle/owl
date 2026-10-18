from dataclasses import dataclass

from owl_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class VmiSettings:
    # Spinta model URLs: the CSV export and the changelog are below them.
    register_url: str
    activities_url: str
    daily_changes_schedule: str
    first_daily_changes: str
    monthly_register_schedule: str
    changelog_page_size: int
    register_export_attempts: int
    register_export_wait_seconds: float


def load_vmi_settings(config: Configuration) -> VmiSettings:
    export_wait = config.get_float("vmi.register_export_wait_seconds")
    if export_wait < 0:
        raise ValueError(f"'vmi.register_export_wait_seconds' is negative: {export_wait}")
    return VmiSettings(
        register_url=config.get_str("vmi.register_url"),
        activities_url=config.get_str("vmi.activities_url"),
        daily_changes_schedule=config.get_str("vmi.daily_changes_schedule"),
        first_daily_changes=config.get_str("vmi.first_daily_changes"),
        monthly_register_schedule=config.get_str("vmi.monthly_register_schedule"),
        changelog_page_size=positive_int(config, "vmi.changelog_page_size"),
        register_export_attempts=positive_int(config, "vmi.register_export_attempts"),
        register_export_wait_seconds=export_wait,
    )


def positive_int(config: Configuration, key: str) -> int:
    value = config.get_int(key)
    if value < 1:
        raise ValueError(f"{key!r} must be at least 1: {value}")
    return value
