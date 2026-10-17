from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from leads_ingest.ports.downloader import Download


@dataclass(frozen=True, slots=True)
class SnapshotManifest:
    """What a fetch captured: when it completed the snapshot, and each file's fetch by name."""

    completed_at: datetime
    files: Mapping[str, Mapping[str, object]]


class RawArchive(Protocol):
    """Raw downloads per source and snapshot. A snapshot is what one fetch of a source captured,
    keyed by the period it was taken in (YYYY-MM for a source fetched monthly, YYYY-MM-DD for one
    fetched daily). A fetch saves its files, then completes the snapshot, which writes its
    manifest last: a snapshot with no manifest is a fetch that has not finished (or failed
    midway), and nothing reads it. The complete snapshots are what the lake is built from."""

    def save(self, source: str, snapshot: str, name: str, download: Download) -> None: ...

    def complete(
        self,
        source: str,
        snapshot: str,
        downloads: Mapping[str, Download],
        completed_at: datetime,
    ) -> None: ...

    def snapshots(self, source: str) -> dict[str, str]:
        """The source's complete snapshots, each with a version of its manifest that changes
        when the snapshot is completed again, as by a second fetch in the same period."""
        ...

    def manifest(self, source: str, snapshot: str) -> SnapshotManifest: ...

    def version_id(self, source: str, snapshot: str, name: str) -> str | None:
        """The storage version of the file as it is now, for the audit trail; None where the
        storage keeps no versions (a local directory)."""
        ...


def file_record(download: Download) -> dict[str, object]:
    """A file's fetch as the archive records it, beside the file and in its snapshot's manifest:
    two snapshots whose files have the same sha256 hold the same version of them."""
    return {
        "url": download.url,
        "fetched_at": download.fetched_at.isoformat(),
        "last_modified": download.last_modified,
        "etag": download.etag,
        "size": download.size,
        "sha256": download.sha256,
    }
