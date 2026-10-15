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


class Downloader(Protocol):
    def download(self, url: str) -> Download: ...
