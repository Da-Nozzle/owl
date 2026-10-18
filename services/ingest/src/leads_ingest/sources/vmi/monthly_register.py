import time
from collections.abc import Callable

from leads_ingest.ports.downloader import Download, Downloader
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.sources.spinta.changelog import last_change
from leads_ingest.sources.spinta.exports import csv_export_url, require_rows_with_ids
from leads_ingest.sources.vmi.settings import VmiSettings

SOURCE_NAME = "vmi_monthly_register"
# Its dbt models read the month from the archive path (_snapshot_month).
SNAPSHOT_KEY_FORMAT = "%Y-%m"
REGISTER_FILE = "MokesciuMoketojas.csv"
ACTIVITIES_FILE = "EkonominesVeiklosRusis.csv"
# The changelog's newest change when the register was exported: the daily changes after it apply.
LAST_CHANGE_FILE = "last-change.json"


def archive_register(
    snapshot: str,
    archive: RawArchive,
    settings: VmiSettings,
    downloader: Downloader,
    *,
    wait: Callable[[float], None] = time.sleep,
) -> dict[str, Download]:
    """Saves the whole register, the changelog's newest change it matches, and the EVRK 2
    classifier its activities refer to. Completing the snapshot is the caller's."""
    register, last_change_download = matching_export(settings, downloader, wait)
    activities = downloader.download(csv_export_url(settings.activities_url))
    require_rows_with_ids(ACTIVITIES_FILE, activities.body)
    archived = {
        REGISTER_FILE: register,
        ACTIVITIES_FILE: activities,
        LAST_CHANGE_FILE: last_change_download,
    }
    for name, download in archived.items():
        archive.save(SOURCE_NAME, snapshot, name, download)
    return archived


def matching_export(
    settings: VmiSettings, downloader: Downloader, wait: Callable[[float], None]
) -> tuple[Download, Download]:
    """The register's export and the changelog's newest change, once the export's ETag is that
    change's revision; otherwise the cache kept it from before that change, or it changed under."""
    revisions = []
    for attempt in range(settings.register_export_attempts):
        if attempt:
            wait(settings.register_export_wait_seconds)
        register = downloader.download(csv_export_url(settings.register_url))
        require_rows_with_ids(REGISTER_FILE, register.body)
        last_download, last = last_change(settings.register_url, downloader)
        if last is not None and unquoted(register.etag) == last.revision:
            return register, last_download
        revisions.append((register.etag, last.revision if last else None))
    raise ValueError(
        f"The register's export never matched its changelog's newest change "
        f"(ETag, newest revision): {revisions}"
    )


def unquoted(etag: str | None) -> str | None:
    """Without the weak marker and the quotes HTTP allows around it."""
    return etag.removeprefix("W/").strip('"') if etag else None
