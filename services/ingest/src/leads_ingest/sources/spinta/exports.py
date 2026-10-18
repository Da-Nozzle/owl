import csv
from collections.abc import Mapping

from leads_ingest.ports.downloader import Download, Downloader
from leads_ingest.ports.raw_archive import RawArchive

ID_COLUMN = "_id"
# Enough of a file to hold its header and the start of its first row.
HEAD_BYTES = 1 << 16


def csv_export_url(model_url: str) -> str:
    return f"{model_url}/:format/csv"


def archive_exports(
    source: str,
    snapshot: str,
    urls: Mapping[str, str],
    archive: RawArchive,
    downloader: Downloader,
) -> dict[str, Download]:
    """Saves each whole CSV export in `urls`, by file name, to the snapshot. Completing the
    snapshot is the caller's."""
    archived = {}
    for name, url in urls.items():
        download = downloader.download(url)
        require_rows_with_ids(name, download.body)
        archive.save(source, snapshot, name, download)
        archived[name] = download
    return archived


def require_rows_with_ids(name: str, body: bytes) -> None:
    """Staging joins the exports on `_id`. This also stops an empty export, or an error page the
    portal's proxy sends as a success, from completing a snapshot."""
    header, _, rows = body[:HEAD_BYTES].partition(b"\n")
    columns = next(csv.reader([header.decode("utf-8", errors="replace")]))
    if ID_COLUMN not in columns or not rows.strip():
        raise ValueError(f"{name} is not a CSV export with an {ID_COLUMN} column and a row")
