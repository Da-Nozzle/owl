from collections.abc import Callable
from functools import partial

from owl_config.configuration import Configuration
from owl_dagster_orchestration.snapshots import ArchivedSource, SnapshotCadence
from owl_dagster_orchestration.sources.adapters import LiveAdapters
from owl_ingest.ports.downloader import Download
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.sources.spinta.changelog import Changelog
from owl_ingest.sources.vmi import daily_changes, monthly_register
from owl_ingest.sources.vmi.settings import load_vmi_settings

# The register's daily changes and the monthly baseline they apply to are drawn in one box, with
# the staging models that combine them (models/staging/vmi/).
VMI_GROUP = "vmi"


def vmi_sources(
    config: Configuration, timezone: str, archive: RawArchive, live: LiveAdapters | None
) -> list[ArchivedSource]:
    settings = load_vmi_settings(config)
    changes_cadence = SnapshotCadence(
        schedule=settings.daily_changes_schedule,
        key_format=daily_changes.SNAPSHOT_KEY_FORMAT,
        timezone=timezone,
    )
    fetch_changes = fetch_register = None
    if live is not None:
        changelog = Changelog(
            settings.register_url,
            live.downloader("vmi.daily_changes_http"),
            settings.changelog_page_size,
        )
        fetch_changes = changes_fetch(changes_cadence, archive, changelog)
        fetch_register = partial(
            monthly_register.archive_register,
            archive=archive,
            settings=settings,
            downloader=live.downloader("vmi.monthly_register_http"),
        )
    return [
        ArchivedSource(
            name=daily_changes.SOURCE_NAME,
            cadence=changes_cadence,
            fetch=fetch_changes,
            history_start=settings.first_daily_changes,
            group=f"{VMI_GROUP}/daily_changes",
        ),
        ArchivedSource(
            name=monthly_register.SOURCE_NAME,
            cadence=SnapshotCadence(
                schedule=settings.monthly_register_schedule,
                key_format=monthly_register.SNAPSHOT_KEY_FORMAT,
                timezone=timezone,
            ),
            fetch=fetch_register,
            group=f"{VMI_GROUP}/monthly_register",
        ),
    ]


def changes_fetch(
    cadence: SnapshotCadence, archive: RawArchive, changelog: Changelog
) -> Callable[[str], dict[str, Download]]:
    """A snapshot holds the changes made in the period before its own. One changelog per process,
    so the consecutive days a backfill fetches are read in one pass."""

    def fetch(snapshot: str) -> dict[str, Download]:
        made_from, made_until = cadence.window_before(snapshot)
        return daily_changes.archive_changes(snapshot, made_from, made_until, archive, changelog)

    return fetch
