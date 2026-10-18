import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.storages.fsspec_filesystem import fsspec_from_config
from fsspec import AbstractFileSystem

from leads_ingest.ports.downloader import Download
from leads_ingest.ports.raw_archive import SnapshotManifest, file_record, sidecar_record

META_SUFFIX = ".meta.json"
# A source's manifests are one folder, <source>/_snapshots/<snapshot>.json, beside its snapshot
# folders: finding the complete snapshots is one listing, not a walk through every file fetched.
MANIFESTS = "_snapshots"


@dataclass(frozen=True, slots=True)
class FsspecRawArchive:
    """The raw archive on any fsspec filesystem (S3 for live runs, a local directory for
    replay): `<root>/<source>/<snapshot>/<name>`, each file with a `.meta.json` sidecar, and each
    complete snapshot's manifest at `<root>/<source>/_snapshots/<snapshot>.json`."""

    # TODO the live archive moves to its own S3 bucket with versioning enabled, outside the
    # lake's data_path. A rerun then overwrites the current object and S3 keeps the old one as a
    # version, so this class needs no write-once logic of its own.

    filesystem: AbstractFileSystem
    root: str

    def save(self, source: str, snapshot: str, name: str, download: Download) -> None:
        # TODO the sidecar is a separate object, so its versions are not tied to the file's.
        # Either store fetched_at and Last-Modified as S3 object metadata (versioned with the
        # file; the local replay sample would still need sidecars) or rely on the VersionIds
        # the fetch records.
        path = f"{self._source_path(source)}/{snapshot}/{name}"
        self.filesystem.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
        self.filesystem.pipe_file(path, download.body)
        self._write_json(path + META_SUFFIX, sidecar_record(download))

    def complete(
        self,
        source: str,
        snapshot: str,
        downloads: Mapping[str, Download],
        completed_at: datetime,
    ) -> None:
        manifest = {
            "source": source,
            "snapshot": snapshot,
            "completed_at": completed_at.isoformat(),
            "files": {name: file_record(download) for name, download in downloads.items()},
        }
        self.filesystem.makedirs(self._manifests_path(source), exist_ok=True)
        self._write_json(self._manifest_path(source, snapshot), manifest)

    def snapshots(self, source: str) -> dict[str, str]:
        manifests_path = self._manifests_path(source)
        if not self.filesystem.isdir(manifests_path):
            return {}
        return {
            entry["name"].rsplit("/", 1)[-1].removesuffix(".json"): self._version(entry)
            for entry in self.filesystem.ls(manifests_path, detail=True, refresh=True)
            if entry["type"] == "file" and entry["name"].endswith(".json")
        }

    def manifest(self, source: str, snapshot: str) -> SnapshotManifest:
        values = json.loads(self.filesystem.cat_file(self._manifest_path(source, snapshot)))
        return SnapshotManifest(
            completed_at=datetime.fromisoformat(values["completed_at"]), files=values["files"]
        )

    def version_id(self, source: str, snapshot: str, name: str) -> str | None:
        # TODO open the S3 filesystem with version_aware=True (s3fs) and return
        # filesystem.info(path)["VersionId"]; None for a local directory.
        raise NotImplementedError

    def _source_path(self, source: str) -> str:
        return f"{self.root.rstrip('/')}/{source}"

    def _manifests_path(self, source: str) -> str:
        return f"{self._source_path(source)}/{MANIFESTS}"

    def _manifest_path(self, source: str, snapshot: str) -> str:
        return f"{self._manifests_path(source)}/{snapshot}.json"

    def _version(self, entry: Mapping[str, object]) -> str:
        """S3 lists each object's ETag, which changes with its content. A local directory lists
        a modification time too coarse to tell two writes in a row apart, so there the manifest
        is read; it is small, and a local archive is the replay sample."""
        etag = entry.get("ETag")
        if etag:
            return str(etag)
        return hashlib.sha256(self.filesystem.cat_file(str(entry["name"]))).hexdigest()

    def _write_json(self, path: str, values: Mapping[str, object]) -> None:
        self.filesystem.pipe_file(path, json.dumps(values, indent=2).encode())


def open_fsspec_archive(location: FilesystemConfiguration) -> FsspecRawArchive:
    filesystem, root = fsspec_from_config(location)
    return FsspecRawArchive(filesystem=filesystem, root=root)
