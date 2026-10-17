"""get.data.gov.lt serves every dataset through Spinta: a dataset's model at
`https://get.data.gov.lt/datasets/gov/<...>/<Model>` exports whole as CSV, and keeps a changelog
of every insert, patch and delete made to it, each with its id (`_cid`) and when it was made
(`_created`, UTC). Query filters are ignored on the changelog, so it is read by id."""

import csv
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from leads_ingest.ports.downloader import Download, Downloader
from leads_ingest.ports.raw_archive import RawArchive

ID_COLUMN = "_id"
# Enough of a file to hold its header and the start of its first row.
HEAD_BYTES = 1 << 16
# Changes per changelog page: about 3 MB of JSON, well within what DuckDB reads as one object.
CHANGES_PER_PAGE = 10_000
# The portal publishes no rate limit, but stops answering changelog reads for a while after a
# burst: after about 2,000 in an hour, at up to 10 a second, on 2026-10-17. So one is started at
# most every second, and a backfill reads its days in one pass (Changelog).
REQUEST_INTERVAL_SECONDS = 1.0


@dataclass(frozen=True, slots=True)
class Change:
    """A changelog entry's place in the changelog: its id and when it was made."""

    id: int
    made_at: datetime
    revision: str


def csv_export_url(model_url: str) -> str:
    return f"{model_url}/:format/csv"


def archive_exports(
    source: str,
    snapshot: str,
    urls: Mapping[str, str],
    archive: RawArchive,
    downloader: Downloader,
) -> dict[str, Download]:
    """Fetches each dataset whole, as the portal exports it (one CSV), into the source's
    snapshot under its file name (`urls` maps each name to its export's URL), where dbt reads
    them. Returns each file's download by name; completing the snapshot is the caller's."""
    archived = {}
    for name, url in urls.items():
        download = downloader.download(url)
        require_rows_with_ids(name, download.body)
        archive.save(source, snapshot, name, download)
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


def last_change_url(model_url: str) -> str:
    return f"{model_url}/:changes/-1?select(_cid,_created,_revision)"


def last_change(model_url: str, downloader: Downloader) -> tuple[Download, Change | None]:
    """The changelog's newest entry, and the response it was read from."""
    download = downloader.download(last_change_url(model_url))
    entries = page_entries(download.body)
    return download, (as_change(entries[-1]) if entries else None)


@dataclass
class Changelog:
    """A model's changelog, read by the time its changes were made. A window's changes are read
    forward from its start, page by page, until the first change made at or after its end: that
    change's id is where the next window starts, so consecutive windows read in one process (a
    backfill's days) cost one search for the first start in all. That change is remembered too,
    so a window that ends before it, a day with no change, costs no request. The changelog is
    only appended to, so a window's boundaries never move once its end has passed."""

    model_url: str
    downloader: Downloader
    _boundaries: dict[datetime, int] = field(default_factory=dict)
    # The first change from a boundary on, where it is known.
    _first_changes: dict[int, Change] = field(default_factory=dict)

    def pages(self, made_from: datetime, made_until: datetime) -> Iterator[tuple[int, Download]]:
        """The changes made from `made_from` up to `made_until`, page by page, each page with the
        id it starts from and as the portal sent it, holding that window's changes only: the page
        that reaches past the window's end is asked for again with the count that fits."""
        position = self.boundary(made_from)
        first = self._first_changes.get(position)
        if first is not None and first.made_at >= made_until:
            self._boundaries[made_until] = position
            return
        while True:
            download = self.downloader.download(
                changes_url(self.model_url, position, CHANGES_PER_PAGE)
            )
            entries = page_entries(download.body)
            inside = [entry for entry in entries if as_change(entry).made_at < made_until]
            if len(inside) == len(entries) == CHANGES_PER_PAGE:
                yield position, download
                position = as_change(entries[-1]).id + 1
                continue
            if len(inside) < len(entries):
                after = as_change(entries[len(inside)])
                self._boundaries[made_until] = after.id
                self._first_changes[after.id] = after
                if inside:
                    download = self.downloader.download(
                        changes_url(self.model_url, position, len(inside))
                    )
            else:
                # The changelog ends inside the window.
                self._boundaries[made_until] = (
                    as_change(entries[-1]).id + 1 if entries else position
                )
            if inside:
                yield position, download
            return

    def boundary(self, moment: datetime) -> int:
        """The id from which changes were made at or after `moment`: every change with a smaller
        id was made before it. Known if a window read ended there; otherwise found by bisection,
        one entry per request (about 25 for a changelog of millions), as ids grow with time,
        with gaps."""
        if moment in self._boundaries:
            return self._boundaries[moment]
        _, newest = last_change(self.model_url, self.downloader)
        if newest is None or newest.made_at < moment:
            return newest.id + 1 if newest else 1
        # The first change from `high` on, `first`, was made at or after `moment`.
        low, high, first = 1, newest.id, newest
        while low < high:
            middle = (low + high) // 2
            change = first_change_from(self.model_url, middle, self.downloader)
            if change is not None and change.made_at < moment:
                low = change.id + 1
            else:
                high, first = middle, change or first
        self._boundaries[moment] = low
        self._first_changes[low] = first
        return low


def first_change_from(model_url: str, position: int, downloader: Downloader) -> Change | None:
    url = f"{model_url}/:changes/{position}?select(_cid,_created,_revision)&limit(1)"
    entries = page_entries(downloader.download(url).body)
    return as_change(entries[0]) if entries else None


def changes_url(model_url: str, position: int, count: int) -> str:
    return f"{model_url}/:changes/{position}?limit({count})"


def page_entries(body: bytes) -> list[Mapping[str, object]]:
    document = json.loads(body)
    entries = document.get("_data") if isinstance(document, Mapping) else None
    if not isinstance(entries, list):
        raise ValueError(f"Not a Spinta page: {body[:200]!r}")
    return entries


def as_change(entry: Mapping[str, object]) -> Change:
    return Change(
        id=int(str(entry["_cid"])),
        made_at=datetime.fromisoformat(str(entry["_created"])).replace(tzinfo=UTC),
        revision=str(entry["_revision"]),
    )
