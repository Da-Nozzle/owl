from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from leads_config.configuration import Configuration
from leads_ingest.months import Month
from leads_lake.settings import LakeStorage

MAX_RUN_DAY = 28
MAX_RUN_HOUR = 23


class PipelineMode(StrEnum):
    LIVE = "live"
    REPLAY = "replay"


@dataclass(frozen=True, slots=True)
class PipelineSettings:
    """How sources run: live calls the sources and archives what they return, replay reads
    `replay_path` instead and calls nothing. `first_month`, `run_day` and `run_hour` set the live
    run months; replay runs the months the sample holds."""

    mode: PipelineMode
    first_month: Month
    run_day: int
    run_hour: int
    timezone: str
    user_agent: str
    replay_path: Path


def raw_archive_path(settings: PipelineSettings, storage: LakeStorage) -> str:
    """Live runs write to and read from the lake's archive; replay runs read replay_path."""
    if settings.mode is PipelineMode.LIVE:
        return storage.archive_path
    return str(settings.replay_path)


def load_pipeline_settings(config: Configuration) -> PipelineSettings:
    # Dagster splits monthly partitions on days 1 to 28 only.
    run_day = config.get_int("pipeline.run_day")
    if not 1 <= run_day <= MAX_RUN_DAY:
        raise ValueError(f"'pipeline.run_day' is {run_day}, expected 1 to {MAX_RUN_DAY}")
    run_hour = config.get_int("pipeline.run_hour")
    if not 0 <= run_hour <= MAX_RUN_HOUR:
        raise ValueError(f"'pipeline.run_hour' is {run_hour}, expected 0 to {MAX_RUN_HOUR}")
    return PipelineSettings(
        mode=PipelineMode(config.get_str("pipeline.mode")),
        first_month=Month.parse(config.get_str("pipeline.first_month")),
        run_day=run_day,
        run_hour=run_hour,
        timezone=config.get_str("pipeline.timezone"),
        user_agent=config.get_str("pipeline.user_agent"),
        replay_path=Path(config.get_str("pipeline.replay_path")),
    )
