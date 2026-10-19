import csv
from collections.abc import Iterator, Mapping
from dataclasses import replace
from functools import partial

from owl_ingest.ports.downloader import (
    Download,
    Downloader,
    OpenedDownload,
    StreamedDownload,
    StreamingDownloader,
)
from owl_ingest.ports.raw_archive import RawArchive

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


def archive_streamed_exports(
    source: str,
    snapshot: str,
    urls: Mapping[str, str],
    archive: RawArchive,
    downloader: StreamingDownloader,
) -> dict[str, StreamedDownload]:
    """As `archive_exports`, for exports too big to hold in memory: each is streamed into the
    archive as it arrives (gzip-compressed if its name ends in `.gz`), its start checked as
    `archive_exports` checks a whole one."""
    archived = {}
    for name, url in urls.items():
        save = partial(save_checked_stream, source, snapshot, name, archive)
        archived[name] = downloader.stream(url, save)
    return archived


def save_checked_stream(
    source: str, snapshot: str, name: str, archive: RawArchive, download: OpenedDownload
) -> StreamedDownload:
    """Saves the stream once its start holds a header with an `_id` column and a row."""
    checked = replace(download, chunks=checked_head(name, download.chunks))
    return archive.save_stream(source, snapshot, name, checked)


def checked_head(name: str, chunks: Iterator[bytes]) -> Iterator[bytes]:
    """The chunks, once their first HEAD_BYTES pass `require_rows_with_ids`."""
    head = b""
    for chunk in chunks:
        head += chunk
        if len(head) >= HEAD_BYTES:
            break
    require_rows_with_ids(name, head)
    yield head
    yield from chunks


def require_rows_with_ids(name: str, body: bytes) -> None:
    """Staging joins the exports on `_id`. This also stops an empty export, or an error page the
    portal's proxy sends as a success, from completing a snapshot."""
    header, _, rows = body[:HEAD_BYTES].partition(b"\n")
    columns = next(csv.reader([header.decode("utf-8", errors="replace")]))
    if ID_COLUMN not in columns or not rows.strip():
        raise ValueError(f"{name} is not a CSV export with an {ID_COLUMN} column and a row")


def unquoted(etag: str | None) -> str | None:
    """Without the weak marker and the quotes HTTP allows around it."""
    return etag.removeprefix("W/").strip('"') if etag else None
