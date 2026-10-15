import io
import zipfile

from leads_ingest.months import Month
from leads_ingest.ports.downloader import Download, Downloader
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.sources.sodra.settings import SodraSettings

SOURCE_NAME = "sodra"


def archive_yearly_files(
    run_month: Month,
    archive: RawArchive,
    settings: SodraSettings,
    downloader: Downloader | None = None,
) -> dict[str, Download]:
    """Puts the yearly files that cover a run's months in the archive, where dbt reads them, all
    of each file: the window only picks which years' files a run fetches.

    With a downloader each file is fetched and archived; without one (replay) the archive must
    already hold them. Returns each file's download by name.
    """
    archived = {}
    for year in window_years(run_month, settings.history_months):
        name = file_name(year)
        if downloader is None:
            # TODO in replay the archive becomes an external asset (nothing fetches into it), so
            # this branch goes away; live always downloads. Reusing archived files instead of
            # fetching is done by materializing only the assets downstream of the archive.
            if not archive.exists(SOURCE_NAME, run_month, name):
                raise FileNotFoundError(f"{SOURCE_NAME}/{run_month.key}/{name} is not archived")
            archived[name] = archive.load(SOURCE_NAME, run_month, name)
            continue
        download = downloader.download(settings.download_url_template.format(year=year))
        require_single_csv(download.body)
        archive.save(SOURCE_NAME, run_month, name, download)
        archived[name] = download
    return archived


def window_years(run_month: Month, history_months: int) -> list[int]:
    """Years of the files a run fetches: those holding the `history_months` before the run
    month."""
    return list(range(run_month.minus(history_months).year, run_month.minus(1).year + 1))


def file_name(year: int) -> str:
    return f"monthly-{year}.csv.zip"


def require_single_csv(zipped: bytes) -> None:
    """Sodra publishes each yearly file as a zip holding one CSV; dbt reads `*.csv` inside it,
    so a second CSV would be read as more rows."""
    with zipfile.ZipFile(io.BytesIO(zipped)) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
    if len(members) != 1:
        raise ValueError(f"Expected one CSV in the Sodra archive, found {members}")
