from dataclasses import dataclass, field
from datetime import UTC, datetime

from leads_ingest.months import Month
from leads_ingest.ports.downloader import Download

FETCHED_AT = datetime(2026, 9, 29, 11, 23, tzinfo=UTC)


def build_download(
    body: bytes = b"PK",
    *,
    url: str = "https://source.test/file.zip",
    fetched_at: datetime = FETCHED_AT,
    last_modified: str | None = None,
) -> Download:
    return Download(url=url, body=body, fetched_at=fetched_at, last_modified=last_modified)


@dataclass
class FakeDownloader:
    """A Downloader that answers every URL with the same body and records the URLs asked for."""

    body: bytes
    requested: list[str] = field(default_factory=list)

    def download(self, url: str) -> Download:
        self.requested.append(url)
        return build_download(self.body, url=url)


@dataclass
class InMemoryRawArchive:
    """A RawArchive in a dict, keyed (source, run month, file name)."""

    downloads: dict[tuple[str, Month, str], Download] = field(default_factory=dict)

    def save(self, source: str, month: Month, name: str, download: Download) -> None:
        self.downloads[(source, month, name)] = download

    def load(self, source: str, month: Month, name: str) -> Download:
        if not self.exists(source, month, name):
            raise FileNotFoundError(f"{source}/{month.key}/{name} is not in the raw archive")
        return self.downloads[(source, month, name)]

    def exists(self, source: str, month: Month, name: str) -> bool:
        return (source, month, name) in self.downloads

    def months(self, source: str) -> list[Month]:
        return sorted({month for saved, month, _ in self.downloads if saved == source})

    def version_id(self, source: str, month: Month, name: str) -> str | None:
        return None
