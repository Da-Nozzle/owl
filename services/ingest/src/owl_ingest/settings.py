from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from owl_config.configuration import Configuration
from owl_lake.settings import LakeStorage


class PipelineMode(StrEnum):
    """Which lake a code location builds: live fetches the sources, replay reads the committed
    sample in `replay_path`. Each is a code location of its own, so this is code, not config."""

    LIVE = "live"
    REPLAY = "replay"


@dataclass(frozen=True, slots=True)
class PipelineSettings:
    # Every source's snapshot_schedule is in it.
    timezone: str
    user_agent: str
    replay_path: Path


def raw_archive_path(mode: PipelineMode, settings: PipelineSettings, storage: LakeStorage) -> str:
    if mode is PipelineMode.LIVE:
        return storage.archive_path
    return str(settings.replay_path)


def load_pipeline_settings(config: Configuration) -> PipelineSettings:
    return PipelineSettings(
        timezone=config.get_str("pipeline.timezone"),
        user_agent=config.get_str("pipeline.user_agent"),
        replay_path=Path(config.get_str("pipeline.replay_path")),
    )
