import hashlib
from collections.abc import Mapping
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
