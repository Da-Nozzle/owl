import time
from collections.abc import Callable

from leads_ingest.ports.downloader import Download, Downloader
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.sources import spinta
from leads_ingest.sources.vmi.settings import VmiSettings

SOURCE_NAME = "vmi_monthly_register"
# The whole register is fetched once a month, as the baseline its daily changes are applied to,
# so a snapshot is keyed by its month (_snapshot_month).
SNAPSHOT_KEY_FORMAT = "%Y-%m"
REGISTER_FILE = "MokesciuMoketojas.csv"
ACTIVITIES_FILE = "EkonominesVeiklosRusis.csv"
# The changelog's newest change when the register was exported: where in the changelog the
# export stands, so that only the changes after it are applied to it.
LAST_CHANGE_FILE = "last-change.json"
# The portal's cache serves an export for up to a minute after the register changed
# (cache-control: max-age=60), so an export that does not match the changelog is asked for
# again after that long.
EXPORT_ATTEMPTS = 5
CACHE_SECONDS = 65


def archive_register(
    snapshot: str,
    archive: RawArchive,
    settings: VmiSettings,
    downloader: Downloader,
    *,
    wait: Callable[[float], None] = time.sleep,
) -> dict[str, Download]:
    """Fetches the whole register as get.data.gov.lt exports it, with the changelog's newest
    change at that moment and the EVRK 2 classifier its activities refer to. The export's ETag is
    the revision of the last change it holds, so it is fetched again until that is the
    changelog's newest: an export the cache kept from before the register's last change, or one
    the register changed under, would place it wrong in the changelog. Returns each file's
    download by name; completing the snapshot is the caller's."""
    register, last_change = matching_export(settings, downloader, wait)
    activities = downloader.download(spinta.csv_export_url(settings.activities_url))
    spinta.require_rows_with_ids(ACTIVITIES_FILE, activities.body)
    archived = {REGISTER_FILE: register, ACTIVITIES_FILE: activities, LAST_CHANGE_FILE: last_change}
    for name, download in archived.items():
        archive.save(SOURCE_NAME, snapshot, name, download)
    return archived


def matching_export(
    settings: VmiSettings, downloader: Downloader, wait: Callable[[float], None]
) -> tuple[Download, Download]:
    """The register's export and the changelog's newest change, once they agree."""
    revisions = []
    for attempt in range(EXPORT_ATTEMPTS):
        if attempt:
            wait(CACHE_SECONDS)
        register = downloader.download(spinta.csv_export_url(settings.register_url))
        spinta.require_rows_with_ids(REGISTER_FILE, register.body)
        last_download, last = spinta.last_change(settings.register_url, downloader)
        if last is not None and unquoted(register.etag) == last.revision:
            return register, last_download
        revisions.append((register.etag, last.revision if last else None))
    raise ValueError(
        f"The register's export never matched its changelog's newest change "
        f"(ETag, newest revision): {revisions}"
    )


def unquoted(etag: str | None) -> str | None:
    """An ETag as compared: without the weak marker and quotes HTTP allows around it."""
    return etag.removeprefix("W/").strip('"') if etag else None
