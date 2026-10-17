from datetime import datetime

from leads_ingest.ports.downloader import Download
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.sources import spinta

SOURCE_NAME = "vmi_daily_changes"
# The register changes most days, so its changes are fetched daily and a snapshot is keyed by
# its day, which its dbt models read from the archive path (_snapshot_day).
SNAPSHOT_KEY_FORMAT = "%Y-%m-%d"


def archive_changes(
    snapshot: str,
    made_from: datetime,
    made_until: datetime,
    archive: RawArchive,
    changelog: spinta.Changelog,
) -> dict[str, Download]:
    """Fetches the register's changelog entries made from `made_from` up to `made_until` into
    the archive, one file per page as the portal sent it, named after the change id it starts
    from. The changelog keeps every past day, so the same window fetched again gives the same
    changes. A day with no changes has no files. Returns each file's download by name;
    completing the snapshot is the caller's."""
    archived = {}
    for position, download in changelog.pages(made_from, made_until):
        name = page_name(position)
        archive.save(SOURCE_NAME, snapshot, name, download)
        archived[name] = download
    return archived


def page_name(position: int) -> str:
    """Zero-padded, so that the pages list in changelog order."""
    return f"changes-{position:012d}.json"
