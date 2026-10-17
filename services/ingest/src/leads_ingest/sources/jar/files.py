import csv

from leads_ingest.ports.downloader import Download, Downloader
from leads_ingest.ports.raw_archive import RawArchive
from leads_ingest.sources.jar.settings import JarSettings

SOURCE_NAME = "jar"
# The register is fetched once a month, so a snapshot is keyed by its month, which its dbt
# models read from the archive path (_snapshot_month).
SNAPSHOT_KEY_FORMAT = "%Y-%m"
# The register's datasets a snapshot fetches, each by the file name its dbt model reads: the
# registered entities, and the classifiers their legal form and status refer to by `_id`.
DATASETS = {
    "JuridinisAsmuo.csv": "iregistruoti/JuridinisAsmuo",
    "Forma.csv": "formos_statusai/Forma",
    "Statusas.csv": "formos_statusai/Statusas",
}
ID_COLUMN = "_id"
# Enough of a file to hold its header and the start of its first row.
HEAD_BYTES = 1 << 16


def archive_register_files(
    snapshot: str,
    archive: RawArchive,
    settings: JarSettings,
    downloader: Downloader,
) -> dict[str, Download]:
    """Fetches each of the register's datasets whole, as get.data.gov.lt exports it (one CSV),
    into the archive, where dbt reads them. Returns each file's download by name; completing the
    snapshot is the caller's."""
    archived = {}
    for name, dataset in DATASETS.items():
        download = downloader.download(settings.download_url_template.format(dataset=dataset))
        require_rows_with_ids(name, download.body)
        archive.save(SOURCE_NAME, snapshot, name, download)
        archived[name] = download
    return archived


def require_rows_with_ids(name: str, body: bytes) -> None:
    """The portal's front proxy sometimes answers with an HTML error page; the downloader retries
    those sent as errors, and this stops one sent as a success, or an empty export, from
    completing a snapshot. Staging joins the files on `_id`, so each must have that column and
    a row."""
    header, _, rows = body[:HEAD_BYTES].partition(b"\n")
    columns = next(csv.reader([header.decode("utf-8", errors="replace")]))
    if ID_COLUMN not in columns or not rows.strip():
        raise ValueError(f"{name} is not a CSV export with an {ID_COLUMN} column and a row")
