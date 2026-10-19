import hashlib
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True, slots=True)
class Download:
    """One fetched file, with what is needed to reload it later as if it were fetched again."""

    url: str
    body: bytes
    fetched_at: datetime
    last_modified: str | None
    etag: str | None = None
    # What was asked for beyond the URL (a search's body and field mask), never credentials.
    request: Mapping[str, object] | None = None

    @property
    def size(self) -> int:
        return len(self.body)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.body).hexdigest()


class Downloader(Protocol):
    def download(self, url: str) -> Download: ...


@dataclass(frozen=True, slots=True)
class OpenedDownload:
    """A file too big to hold in memory, as its response starts: what it is, with its body still
    to be read, once, in `chunks`."""

    url: str
    fetched_at: datetime
    last_modified: str | None
    etag: str | None
    chunks: Iterator[bytes]


@dataclass(frozen=True, slots=True)
class StreamedDownload:
    """A file written to the archive as it arrived: a Download's record without its body. `size`
    and `sha256` are of the body as sent, before any compression of the archived file, so two
    fetches of one version compare equal however it is stored."""

    url: str
    fetched_at: datetime
    last_modified: str | None
    etag: str | None
    size: int
    sha256: str


# A file a fetch archived, held in memory or streamed.
FetchedFile = Download | StreamedDownload


class StreamingDownloader(Protocol):
    def stream[T](self, url: str, write: Callable[[OpenedDownload], T]) -> T:
        """Calls `write` with the download, its body unread, and returns what it returns; `write`
        may also leave the body unread. A transient failure, before the body or while `write`
        reads it, downloads it again from its start and calls `write` again, as the
        downloader's retry policy allows, so `write` must start its file over each time."""
        ...
