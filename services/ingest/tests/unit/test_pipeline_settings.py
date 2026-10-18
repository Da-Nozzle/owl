from pathlib import Path

import pytest

from owl_ingest.settings import PipelineMode, load_pipeline_settings, raw_archive_path
from owl_lake.settings import load_lake_settings
from owl_testing.config import build_config


@pytest.mark.parametrize(
    ("mode", "archive_path"),
    [(PipelineMode.LIVE, "s3://owl-lake/archive/"), (PipelineMode.REPLAY, "/workspace/replay")],
)
def test_live_runs_use_the_lakes_archive_and_replay_runs_the_sample(
    mode: PipelineMode, archive_path: str
) -> None:
    # Arrange
    config = build_config(
        {
            "pipeline.replay_path": "/workspace/replay",
            "lake.archive_path": "s3://owl-lake/archive/",
        }
    )
    settings = load_pipeline_settings(config)

    # Act
    path = raw_archive_path(mode, settings, load_lake_settings(config).storage)

    # Assert
    assert (settings.replay_path, path) == (Path("/workspace/replay"), archive_path)
