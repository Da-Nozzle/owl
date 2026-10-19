from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from owl_ingest.ports.downloader import Download, FetchedFile, OpenedDownload, StreamedDownload


@dataclass(frozen=True, slots=True)
class SnapshotManifest:
    completed_at: datetime
    files: Mapping[str, Mapping[str, object]]


class RawArchive(Protocol):
    """Raw downloads by source and snapshot, the period a fetch was made in. A fetch saves its
    files, then completes the snapshot by writing its manifest: nothing reads a snapshot without
    one."""

    def save(self, source: str, snapshot: str, name: str, download: Download) -> None: ...

    def save_stream(
        self, source: str, snapshot: str, name: str, download: OpenedDownload
    ) -> StreamedDownload:
        """Writes the body as its chunks arrive, never holding it whole, from the file's start
        each time; gzip-compressed if `name` ends in `.gz`. The file is replaced only once the
        body has arrived whole: a body that breaks off leaves the file as it was."""
        ...

    def complete(
        self,
        source: str,
        snapshot: str,
        downloads: Mapping[str, FetchedFile],
        completed_at: datetime,
    ) -> None: ...

    def snapshots(self, source: str) -> dict[str, str]:
        """The complete snapshots, each with a version that changes when it is completed again."""
        ...

    def manifest(self, source: str, snapshot: str) -> SnapshotManifest: ...


def file_record(download: FetchedFile) -> dict[str, object]:
    """A file's fetch as the archive records it, in the snapshot's manifest and beside the file."""
    return {
        "url": download.url,
        "fetched_at": download.fetched_at.isoformat(),
        "last_modified": download.last_modified,
        "etag": download.etag,
        "size": download.size,
        "sha256": download.sha256,
    }
