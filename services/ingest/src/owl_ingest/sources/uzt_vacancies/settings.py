from dataclasses import dataclass

from owl_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class UztVacanciesSettings:
    # The Spinta model: its CSV export and its changelog are below it.
    vacancies_url: str
    snapshot_schedule: str
    export_attempts: int
    export_wait_seconds: float
    # Rows asked for in each request: the export is fetched in pages of this many.
    page_rows: int
    fetch_max_runtime_seconds: int


def load_uzt_vacancies_settings(config: Configuration) -> UztVacanciesSettings:
    attempts = config.get_int("uzt_vacancies.export_attempts")
    if attempts < 1:
        raise ValueError(f"'uzt_vacancies.export_attempts' must be at least 1: {attempts}")
    export_wait = config.get_float("uzt_vacancies.export_wait_seconds")
    if export_wait < 0:
        raise ValueError(f"'uzt_vacancies.export_wait_seconds' is negative: {export_wait}")
    settings = UztVacanciesSettings(
        vacancies_url=config.get_str("uzt_vacancies.vacancies_url"),
        snapshot_schedule=config.get_str("uzt_vacancies.snapshot_schedule"),
        export_attempts=attempts,
        export_wait_seconds=export_wait,
        page_rows=config.get_int("uzt_vacancies.page_rows"),
        fetch_max_runtime_seconds=config.get_int("uzt_vacancies.fetch_max_runtime_seconds"),
    )
    for key in ("page_rows", "fetch_max_runtime_seconds"):
        if getattr(settings, key) < 1:
            raise ValueError(f"'uzt_vacancies.{key}' must be at least 1: {getattr(settings, key)}")
    return settings
