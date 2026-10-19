import csv
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, replace

from owl_ingest.ports.downloader import OpenedDownload, StreamedDownload, StreamingDownloader
from owl_ingest.ports.raw_archive import RawArchive
from owl_ingest.sources.spinta.exports import HEAD_BYTES, save_checked_stream, unquoted

FILE_SUFFIX = ".csv.gz"
# Spinta fills it on a response's last row only: the token of the page after that row.
PAGE_COLUMN = "_page.next"
# What select() names to get PAGE_COLUMN: `_page.next` itself fails the response midway.
PAGE_SELECTION = "_page"
# A row is found by the type it starts with, so a selection starts with it.
TYPE_COLUMN = "_type"


class IncompleteExportError(ValueError):
    pass


class ChangedExportError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class SavedPage:
    download: StreamedDownload
    rows: int
    # The last row's _page.next; None if the page was cut off before its end.
    next_page: str | None


def archive_pages(
    source: str,
    snapshot: str,
    archive: RawArchive,
    name: str,
    export_url: str,
    page_rows: int,
    downloader: StreamingDownloader,
    *,
    columns: Sequence[str] = (),
    version: str | None = None,
) -> dict[str, StreamedDownload]:
    """Saves the export page by page, in the model's page order, each page a file
    (`<name>.csv.gz`, then `<name>.2.csv.gz`, ...), until the pages hold as many rows as the
    export counts. Completing the snapshot is the caller's.

    The portal drops a response after about 25 minutes, and its cache keeps a dropped response as
    if it were whole: on 2026-10-18 it sent JAR's income statements cut off at 3.18 M of their
    6.49 M rows, ending on a whole row, on every request, and the export asked for past the cache
    (sent at 0.6 to 1.6 MB/s) was dropped after about 2.4 GB on each of five tries. A page small
    enough to take minutes is safe; one dropped midway is asked for again whole by the
    downloader. Each page must be whole: as many rows as asked for (fewer only on the last), its
    last row with the next page's token, and of the version (ETag) that was counted.

    With `columns`, each page selects them alone (`select()`, with paging); they start with `_type`
    and hold `_page`, which the checks read. With `version`, a count of any other version fails as
    a page of another version does, before any page is asked for."""
    selection = page_selection(columns)
    rows, counted = downloader.stream(f"{export_url}?count()", read_count)
    if version is not None and counted != version:
        raise ChangedExportError(f"{name} was counted at version {counted}, not {version}")
    if rows == 0:
        raise IncompleteExportError(f"{name} counts no rows")
    pages: dict[str, StreamedDownload] = {}
    saved, next_page = 0, None
    while saved < rows:
        file_name = page_file_name(name, len(pages) + 1)
        query = selection + f"limit({page_rows})" + (f'&page("{next_page}")' if next_page else "")
        page = downloader.stream(
            f"{export_url}?{query}", page_saver(source, snapshot, file_name, archive)
        )
        if unquoted(page.download.etag) != counted:
            raise ChangedExportError(
                f"{file_name} is version {page.download.etag}, but {counted} was counted: the "
                "dataset was reloaded during the fetch, or the portal's cache sent an older page"
            )
        expected = min(page_rows, rows - saved)
        if page.rows != expected or page.next_page is None:
            raise IncompleteExportError(
                f"{file_name} was cut off: {page.rows} rows of {expected}, its last row "
                f"{'with' if page.next_page else 'without'} {PAGE_COLUMN}"
            )
        pages[file_name] = page.download
        saved += page.rows
        next_page = page.next_page
    return pages


def page_selection(columns: Sequence[str]) -> str:
    """The select() a page's query starts with, if any."""
    if not columns:
        return ""
    if columns[0] != TYPE_COLUMN or PAGE_SELECTION not in columns:
        raise ValueError(f"A paged selection starts with {TYPE_COLUMN} and holds {PAGE_SELECTION}")
    return f"select({','.join(columns)})&"


def page_file_name(name: str, page: int) -> str:
    """The first page under the dataset's own name, as a dataset of one page is kept."""
    return f"{name}{FILE_SUFFIX}" if page == 1 else f"{name}.{page}{FILE_SUFFIX}"


def read_count(download: OpenedDownload) -> tuple[int, str | None]:
    """The rows `?count()` counts (a CSV of one column and one row), and the version counted."""
    body = b"".join(download.chunks)[:HEAD_BYTES].decode("utf-8", errors="replace")
    rows = list(csv.reader(body.splitlines()))
    if len(rows) != 2 or rows[0] != ["count()"] or not rows[1] or not rows[1][0].isdigit():
        raise ValueError(f"{download.url} is not a count: {body[:200]!r}")
    return int(rows[1][0]), unquoted(download.etag)


def page_saver(
    source: str, snapshot: str, file_name: str, archive: RawArchive
) -> Callable[[OpenedDownload], SavedPage]:
    """As `save_checked_stream`, also counting the page's rows and reading its last row's token.
    A retried download calls it again, so each call counts afresh."""

    def save(download: OpenedDownload) -> SavedPage:
        scan = PageScan()
        scanned = replace(download, chunks=scan.scanning(download.chunks))
        stored = save_checked_stream(source, snapshot, file_name, archive, scanned)
        return SavedPage(download=stored, rows=scan.rows, next_page=scan.next_page())

    return save


@dataclass
class PageScan:
    """Counts a Spinta CSV's rows as its chunks pass: a row starts a line with the first row's
    _type, which a line of a field spanning lines (a balance sheet form's name) never does. Keeps
    the start, for the header, and the last HEAD_BYTES, for the last row's token."""

    head: bytes = b""
    tail: bytes = b""
    rows: int = 0
    marker: bytes | None = None

    def scanning(self, chunks: Iterator[bytes]) -> Iterator[bytes]:
        for chunk in chunks:
            if self.marker is None and len(self.head) < HEAD_BYTES:
                self.head += chunk
                self.marker = row_marker(self.head)
                if self.marker is not None:
                    self.rows = self.head.count(self.marker)
                    self.tail = self.head[-HEAD_BYTES:]
            elif self.marker is not None:
                # The window joins the chunk to the last one, so a marker split by them counts.
                window = self.tail[-(len(self.marker) - 1) :] + chunk
                self.rows += window.count(self.marker)
                self.tail = (self.tail + chunk)[-HEAD_BYTES:]
            yield chunk

    def next_page(self) -> str | None:
        """The last row's _page.next; None unless the page ends with a whole row holding one."""
        header = self.head.partition(b"\n")[0].decode("utf-8", errors="replace")
        columns = next(csv.reader([header]), [])
        if self.marker is None or PAGE_COLUMN not in columns or not self.tail.endswith(b"\n"):
            return None
        # Spinta's own columns, the token's included, come first and are never quoted.
        position = columns.index(PAGE_COLUMN)
        fields = self.tail.rpartition(self.marker)[2].split(b",", position)
        token = fields[position - 1].decode() if len(fields) > position else ""
        return token or None


def row_marker(head: bytes) -> bytes | None:
    """A line's end and the first row's _type with its comma, once `head` holds them."""
    header_end = head.find(b"\n")
    type_end = head.find(b",", header_end + 1) if header_end >= 0 else -1
    if type_end < 0:
        return None
    return head[header_end : type_end + 1]
