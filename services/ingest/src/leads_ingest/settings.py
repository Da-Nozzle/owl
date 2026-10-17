from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from leads_config.configuration import Configuration
from leads_lake.settings import LakeStorage


class PipelineMode(StrEnum):
    """Which lake a code location builds. Live calls the sources and archives what they return;
    replay reads the committed sample in `replay_path` and calls nothing. Each mode is a code
    location of its own, so the mode is code, not configuration."""

    LIVE = "live"
    REPLAY = "replay"


@dataclass(frozen=True, slots=True)
class PipelineSettings:
    """What every source shares: the timezone their fetch schedules are in, the user agent they
    fetch as, and where replay reads the committed sample. When each source is fetched is the
    source's own setting."""

    timezone: str
    user_agent: str
    replay_path: Path


def raw_archive_path(mode: PipelineMode, settings: PipelineSettings, storage: LakeStorage) -> str:
    """Live runs write to and read from the lake's archive; replay runs read replay_path."""
    if mode is PipelineMode.LIVE:
        return storage.archive_path
    return str(settings.replay_path)


def load_pipeline_settings(config: Configuration) -> PipelineSettings:
    return PipelineSettings(
        timezone=config.get_str("pipeline.timezone"),
        user_agent=config.get_str("pipeline.user_agent"),
        replay_path=Path(config.get_str("pipeline.replay_path")),
    )
