import json
from dataclasses import dataclass
from datetime import datetime

from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.storages.fsspec_filesystem import fsspec_from_config
from fsspec import AbstractFileSystem

from leads_ingest.months import MONTH_KEY, Month
from leads_ingest.ports.downloader import Download

META_SUFFIX = ".meta.json"


@dataclass(frozen=True, slots=True)
class FsspecRawArchive:
    """The raw archive on any fsspec filesystem (S3 for live runs, a local directory for
    replay): `<root>/<source>/<YYYY-MM>/<name>`, each file with a `.meta.json` sidecar."""

    # TODO the live archive moves to its own S3 bucket with versioning enabled, outside the
    # lake's data_path. A rerun then overwrites the current object and S3 keeps the old one as a
    # version, so this class needs no write-once logic of its own.

    filesystem: AbstractFileSystem
    root: str

    def save(self, source: str, month: Month, name: str, download: Download) -> None:
        # TODO the sidecar is a separate object, so its versions are not tied to the file's.
        # Either store fetched_at and Last-Modified as S3 object metadata (versioned with the
        # file; the local replay sample would still need sidecars) or rely on the VersionIds
        # the archive asset records.
        path = self._path(source, month, name)
        self.filesystem.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
        self.filesystem.pipe_file(path, download.body)
        meta = {
            "url": download.url,
            "fetched_at": download.fetched_at.isoformat(),
            "last_modified": download.last_modified,
            "size": download.size,
            "sha256": download.sha256,
        }
        self.filesystem.pipe_file(path + META_SUFFIX, json.dumps(meta, indent=2).encode())

    def load(self, source: str, month: Month, name: str) -> Download:
        path = self._path(source, month, name)
        if not self.filesystem.exists(path):
            raise FileNotFoundError(f"{path} is not in the raw archive")
        meta = json.loads(self.filesystem.cat_file(path + META_SUFFIX))
        return Download(
            url=meta["url"],
            body=self.filesystem.cat_file(path),
            fetched_at=datetime.fromisoformat(meta["fetched_at"]),
            last_modified=meta["last_modified"],
        )

    def exists(self, source: str, month: Month, name: str) -> bool:
        return bool(self.filesystem.exists(self._path(source, month, name)))

    def months(self, source: str) -> list[Month]:
        """The run months that hold any of the source's files, oldest first."""
        source_path = f"{self.root.rstrip('/')}/{source}"
        if not self.filesystem.isdir(source_path):
            return []
        names = (
            entry["name"].rstrip("/").rsplit("/", 1)[-1]
            for entry in self.filesystem.ls(source_path, detail=True)
            if entry["type"] == "directory"
        )
        return sorted(Month.parse(name) for name in names if MONTH_KEY.match(name))

    def version_id(self, source: str, month: Month, name: str) -> str | None:
        # TODO open the S3 filesystem with version_aware=True (s3fs) and return
        # filesystem.info(path)["VersionId"]; None for a local directory.
        raise NotImplementedError

    def _path(self, source: str, month: Month, name: str) -> str:
        return f"{self.root.rstrip('/')}/{source}/{month.key}/{name}"


def open_fsspec_archive(location: FilesystemConfiguration) -> FsspecRawArchive:
    filesystem, root = fsspec_from_config(location)
    return FsspecRawArchive(filesystem=filesystem, root=root)
