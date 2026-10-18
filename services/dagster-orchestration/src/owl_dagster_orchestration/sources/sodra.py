from functools import partial

from owl_config.configuration import Configuration
from owl_dagster_orchestration.snapshots import ArchivedSource, SnapshotCadence
from owl_dagster_orchestration.sources.adapters import LiveAdapters
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.sources.sodra.files import (
    SNAPSHOT_KEY_FORMAT,
    SOURCE_NAME,
    archive_yearly_files,
)
from owl_ingest.sources.sodra.settings import load_sodra_settings


def sodra_source(
    config: Configuration, timezone: str, archive: RawArchive, live: LiveAdapters | None
) -> ArchivedSource:
    settings = load_sodra_settings(config)
    fetch = None
    if live is not None:
        fetch = partial(
            archive_yearly_files,
            archive=archive,
            settings=settings,
            downloader=live.downloader("sodra.http"),
        )
    return ArchivedSource(
        name=SOURCE_NAME,
        cadence=SnapshotCadence(
            schedule=settings.snapshot_schedule, key_format=SNAPSHOT_KEY_FORMAT, timezone=timezone
        ),
        fetch=fetch,
    )
