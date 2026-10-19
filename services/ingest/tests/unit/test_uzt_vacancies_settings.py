import pytest

from owl_ingest.sources.uzt_vacancies.settings import (
    UztVacanciesSettings,
    load_uzt_vacancies_settings,
)
from owl_testing.config import build_config


def test_uzt_vacancies_settings_are_read_from_config() -> None:
    # Arrange
    config = build_config(
        {
            "uzt_vacancies.vacancies_url": "https://uzt.example/Vieta",
            "uzt_vacancies.export_wait_seconds": 65.5,
        }
    )

    # Act
    settings = load_uzt_vacancies_settings(config)

    # Assert
    assert settings == UztVacanciesSettings(
        vacancies_url="https://uzt.example/Vieta",
        snapshot_schedule="0 6 21 * *",
        export_attempts=6,
        export_wait_seconds=65.5,
        page_rows=400_000,
        fetch_max_runtime_seconds=7_200,
    )


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("uzt_vacancies.export_attempts", 0),
        ("uzt_vacancies.export_wait_seconds", -1),
        ("uzt_vacancies.page_rows", 0),
        ("uzt_vacancies.fetch_max_runtime_seconds", 0),
    ],
)
def test_an_export_policy_that_cannot_work_is_rejected(key: str, value: int) -> None:
    # Arrange
    config = build_config({key: value})

    # Act / Assert
    with pytest.raises(ValueError, match=key):
        load_uzt_vacancies_settings(config)
