from datetime import datetime

from owl_ingest.ports.downloader import Download
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.sources.spinta.changelog import Changelog

SOURCE_NAME = "vmi_tax_arrears"
# Its dbt models read the month from the archive path (_snapshot_month).
SNAPSHOT_KEY_FORMAT = "%Y-%m"


def archive_changes(
    snapshot: str,
    made_from: datetime,
    made_until: datetime,
    archive: RawArchive,
    changelog: Changelog,
) -> dict[str, Download]:
    """Saves the changes made to the arrears list in the window, a file per changelog page; none
    for a window without changes. Completing the snapshot is the caller's."""
    archived = {}
    for position, download in changelog.pages(made_from, made_until):
        name = page_name(position)
        archive.save(SOURCE_NAME, snapshot, name, download)
        archived[name] = download
    return archived


def page_name(position: int) -> str:
    # Zero-padded, so the pages list in changelog order.
    return f"changes-{position:012d}.json"
