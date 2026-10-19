from collections.abc import Callable

from owl_config.configuration import Configuration
from owl_dagster_orchestration.snapshots import ArchivedSource, SnapshotCadence
from owl_dagster_orchestration.sources.adapters import LiveAdapters
from owl_ingest.ports.downloader import Download
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.sources.spinta.changelog import Changelog
from owl_ingest.sources.vmi_tax_arrears.changes import (
    SNAPSHOT_KEY_FORMAT,
    SOURCE_NAME,
    archive_changes,
)
from owl_ingest.sources.vmi_tax_arrears.settings import load_vmi_tax_arrears_settings


def vmi_tax_arrears_source(
    config: Configuration, timezone: str, archive: RawArchive, live: LiveAdapters | None
) -> ArchivedSource:
    settings = load_vmi_tax_arrears_settings(config)
    # Monthly by choice: the changelog keeps every change, so a daily schedule would give the list
    # as of each day, at the cost of more requests and partitions.
    cadence = SnapshotCadence(
        schedule=settings.snapshot_schedule, key_format=SNAPSHOT_KEY_FORMAT, timezone=timezone
    )
    fetch = None
    if live is not None:
        changelog = Changelog(
            settings.model_url,
            live.downloader("vmi_tax_arrears.http"),
            settings.changelog_page_size,
        )
        fetch = changes_fetch(cadence, archive, changelog)
    return ArchivedSource(
        name=SOURCE_NAME,
        cadence=cadence,
        fetch=fetch,
        history_start=settings.first_snapshot,
    )


def changes_fetch(
    cadence: SnapshotCadence, archive: RawArchive, changelog: Changelog
) -> Callable[[str], dict[str, Download]]:
    """A snapshot holds the changes made in the month before its tick. One changelog per process,
    so the consecutive months a backfill fetches are read in one pass."""

    def fetch(snapshot: str) -> dict[str, Download]:
        made_from, made_until = cadence.window_before(snapshot)
        return archive_changes(snapshot, made_from, made_until, archive, changelog)

    return fetch
