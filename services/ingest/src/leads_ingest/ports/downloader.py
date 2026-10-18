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
    # The server's version of what it sent (its ETag header), as sent. get.data.gov.lt sets it
    # to the revision of the dataset's last change, which places an export in its changelog.
    etag: str | None = None
    # What was asked for beyond the URL, for a response that a GET of the URL would not give
    # again (a search's body and field mask), never its credentials. None for a plain GET.
    request: Mapping[str, object] | None = None

    @property
    def size(self) -> int:
        return len(self.body)

    @property
    def sha256(self) -> str:
        """Equal for two downloads of the same version of a file, which `last_modified` alone
        does not prove."""
        return hashlib.sha256(self.body).hexdigest()


class Downloader(Protocol):
    def download(self, url: str) -> Download: ...
