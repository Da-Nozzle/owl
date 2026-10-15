import pytest

from leads_config.configuration import Configuration
from leads_ingest.settings import load_pipeline_settings


def pipeline_config(run_day: int) -> Configuration:
    return Configuration(
        values={
            "pipeline": {
                "mode": "replay",
                "first_month": "2026-08",
                "run_day": run_day,
                "run_hour": 6,
                "timezone": "Europe/Vilnius",
                "user_agent": "test",
                "replay_path": "/replay",
            }
        },
        secret_stores={},
    )


def test_run_day_is_read_from_config() -> None:
    # Act
    settings = load_pipeline_settings(pipeline_config(run_day=21))

    # Assert
    assert settings.run_day == 21


def test_a_run_day_dagster_cannot_split_on_is_rejected() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match=r"pipeline\.run_day"):
        load_pipeline_settings(pipeline_config(run_day=29))
