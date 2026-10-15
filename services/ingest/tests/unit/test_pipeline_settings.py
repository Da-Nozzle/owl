from pathlib import Path

import pytest

from leads_ingest.settings import PipelineMode, load_pipeline_settings, raw_archive_path
from leads_lake.settings import load_lake_settings
from leads_testing.config import build_config


@pytest.mark.parametrize("run_day", [1, 28])
def test_run_day_is_read_from_config(run_day: int) -> None:
    # Act
    settings = load_pipeline_settings(build_config({"pipeline.run_day": run_day}))

    # Assert
    assert settings.run_day == run_day


@pytest.mark.parametrize("run_day", [0, 29])
def test_a_run_day_dagster_cannot_split_on_is_rejected(run_day: int) -> None:
    # Act / Assert
    with pytest.raises(ValueError, match=r"pipeline\.run_day"):
        load_pipeline_settings(build_config({"pipeline.run_day": run_day}))


@pytest.mark.parametrize("run_hour", [-1, 24])
def test_a_run_hour_outside_the_day_is_rejected(run_hour: int) -> None:
    # Act / Assert
    with pytest.raises(ValueError, match=r"pipeline\.run_hour"):
        load_pipeline_settings(build_config({"pipeline.run_hour": run_hour}))


def test_an_unknown_mode_is_rejected() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="'batch'"):
        load_pipeline_settings(build_config({"pipeline.mode": "batch"}))


@pytest.mark.parametrize(
    ("mode", "archive_path"),
    [(PipelineMode.LIVE, "s3://leads-lake/archive/"), (PipelineMode.REPLAY, "/workspace/replay")],
)
def test_live_runs_use_the_lakes_archive_and_replay_runs_the_sample(
    mode: PipelineMode, archive_path: str
) -> None:
    # Arrange
    config = build_config(
        {
            "pipeline.mode": mode.value,
            "pipeline.replay_path": "/workspace/replay",
            "lake.archive_path": "s3://leads-lake/archive/",
        }
    )
    settings = load_pipeline_settings(config)

    # Act
    path = raw_archive_path(settings, load_lake_settings(config).storage)

    # Assert
    assert (settings.mode, settings.replay_path, path) == (
        mode,
        Path("/workspace/replay"),
        archive_path,
    )
