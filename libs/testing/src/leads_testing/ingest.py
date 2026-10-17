from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from leads_ingest.ports.downloader import Download
from leads_ingest.ports.raw_archive import SnapshotManifest, file_record

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
    """A RawArchive in dicts: downloads keyed (source, snapshot, file name), and the manifests of
    complete snapshots keyed (source, snapshot), each with how often it was completed as its
    version."""

    downloads: dict[tuple[str, str, str], Download] = field(default_factory=dict)
    manifests: dict[tuple[str, str], tuple[SnapshotManifest, int]] = field(default_factory=dict)

    def save(self, source: str, snapshot: str, name: str, download: Download) -> None:
        self.downloads[(source, snapshot, name)] = download

    def complete(
        self,
        source: str,
        snapshot: str,
        downloads: Mapping[str, Download],
        completed_at: datetime,
    ) -> None:
        files = {name: file_record(download) for name, download in downloads.items()}
        _, version = self.manifests.get((source, snapshot), (None, 0))
        self.manifests[(source, snapshot)] = (SnapshotManifest(completed_at, files), version + 1)

    def snapshots(self, source: str) -> dict[str, str]:
        return {
            snapshot: str(version)
            for (saved, snapshot), (_, version) in self.manifests.items()
            if saved == source
        }

    def manifest(self, source: str, snapshot: str) -> SnapshotManifest:
        return self.manifests[(source, snapshot)][0]

    def version_id(self, source: str, snapshot: str, name: str) -> str | None:
        return None
