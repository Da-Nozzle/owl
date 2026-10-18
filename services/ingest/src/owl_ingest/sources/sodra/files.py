import io
import zipfile

from owl_ingest.months import Month
from owl_ingest.ports.downloader import Download, Downloader
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.sources.sodra.settings import SodraSettings

SOURCE_NAME = "sodra"
# Its dbt models read the month from the archive path (_snapshot_month).
SNAPSHOT_KEY_FORMAT = "%Y-%m"


def archive_yearly_files(
    snapshot: str,
    archive: RawArchive,
    settings: SodraSettings,
    downloader: Downloader,
) -> dict[str, Download]:
    """Saves the whole yearly files that cover the snapshot's window. Completing the snapshot is
    the caller's."""
    archived = {}
    for year in window_years(Month.parse(snapshot), settings.history_months):
        name = file_name(year)
        download = downloader.download(settings.download_url_template.format(year=year))
        require_single_csv(download.body)
        archive.save(SOURCE_NAME, snapshot, name, download)
        archived[name] = download
    return archived


def window_years(snapshot: Month, history_months: int) -> list[int]:
    """The years of the `history_months` before the snapshot's month."""
    return list(range(snapshot.minus(history_months).year, snapshot.minus(1).year + 1))


def file_name(year: int) -> str:
    return f"monthly-{year}.csv.zip"


def require_single_csv(zipped: bytes) -> None:
    """dbt reads every `*.csv` in the zip, so a second CSV would be read as more rows."""
    with zipfile.ZipFile(io.BytesIO(zipped)) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
    if len(members) != 1:
        raise ValueError(f"Expected one CSV in the Sodra archive, found {members}")
