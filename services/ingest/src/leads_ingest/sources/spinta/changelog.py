import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from leads_ingest.ports.downloader import Download, Downloader


@dataclass(frozen=True, slots=True)
class Change:
    id: int
    made_at: datetime
    revision: str


@dataclass
class Changelog:
    """A Spinta model's changelog, read by when its changes were made. Spinta ignores query
    filters on the changelog, so it is read by change id."""

    model_url: str
    downloader: Downloader
    page_size: int
    # Where each window read so far ended, so consecutive windows (a backfill's days) search the
    # changelog once. It is only appended to, so a boundary never moves once its moment has passed.
    _boundaries: dict[datetime, int] = field(default_factory=dict)
    # The first change from a boundary on, so a window that ends before it costs no request.
    _first_changes: dict[int, Change] = field(default_factory=dict)

    def pages(self, made_from: datetime, made_until: datetime) -> Iterator[tuple[int, Download]]:
        """The window's changes, page by page, each with the id it starts from. The page that
        reaches past the window's end is asked for again with the count that fits."""
        position = self.boundary(made_from)
        first = self._first_changes.get(position)
        if first is not None and first.made_at >= made_until:
            self._boundaries[made_until] = position
            return
        while True:
            download = self.downloader.download(
                changes_url(self.model_url, position, self.page_size)
            )
            entries = page_entries(download.body)
            inside = [entry for entry in entries if as_change(entry).made_at < made_until]
            if len(inside) == len(entries) == self.page_size:
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
        """The id from which changes were made at or after `moment`. Unless known, found by
        bisection, one entry per request, as ids grow with time (with gaps)."""
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


def last_change(model_url: str, downloader: Downloader) -> tuple[Download, Change | None]:
    """The changelog's newest entry, and the response it was read from."""
    download = downloader.download(f"{model_url}/:changes/-1?select(_cid,_created,_revision)")
    entries = page_entries(download.body)
    return download, (as_change(entries[-1]) if entries else None)


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
        # UTC, written without an offset.
        made_at=datetime.fromisoformat(str(entry["_created"])).replace(tzinfo=UTC),
        revision=str(entry["_revision"]),
    )
