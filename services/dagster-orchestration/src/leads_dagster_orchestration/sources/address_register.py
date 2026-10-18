from functools import partial

from leads_config.configuration import Configuration
from leads_dagster_orchestration.snapshots import ArchivedSource, SnapshotCadence
from leads_dagster_orchestration.sources.adapters import LiveAdapters
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.sources.address_register.files import (
    SNAPSHOT_KEY_FORMAT,
    SOURCE_NAME,
    archive_register_files,
)
from leads_ingest.sources.address_register.settings import load_address_register_settings


def address_register_source(
    config: Configuration, timezone: str, archive: RawArchive, live: LiveAdapters | None
) -> ArchivedSource:
    settings = load_address_register_settings(config)
    fetch = None
    if live is not None:
        fetch = partial(
            archive_register_files,
            archive=archive,
            settings=settings,
            downloader=live.downloader("address_register.http"),
        )
    return ArchivedSource(
        name=SOURCE_NAME,
        cadence=SnapshotCadence(
            schedule=settings.snapshot_schedule, key_format=SNAPSHOT_KEY_FORMAT, timezone=timezone
        ),
        fetch=fetch,
    )
