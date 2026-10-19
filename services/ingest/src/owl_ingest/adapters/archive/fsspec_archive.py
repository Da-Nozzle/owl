import gzip
import hashlib
import json
from collections.abc import Mapping
from contextlib import AbstractContextManager, nullcontext, suppress
from dataclasses import dataclass
from datetime import datetime
from typing import IO

from fsspec import AbstractFileSystem
from fsspec.core import url_to_fs

from owl_ingest.ports.downloader import Download, FetchedFile, OpenedDownload, StreamedDownload
from owl_ingest.ports.raw_archive import SnapshotManifest, file_record

META_SUFFIX = ".meta.json"
# One folder of manifests per source, so finding the complete snapshots is a single listing.
MANIFESTS = "_snapshots"
# A name the archive compresses as it writes it, for exports too big to keep as sent.
GZIP_SUFFIX = ".gz"
# A streamed file until its body has arrived whole. The sources' globs end in their extension, so
# none matches it.
PARTIAL_SUFFIX = ".partial"
# zlib's default: Spinta's CSV exports to about a fifth, at about 40 MB/s; 9 is slower for little
# less.
GZIP_LEVEL = 6


@dataclass(frozen=True, slots=True)
class FsspecRawArchive:
    """`<root>/<source>/<snapshot>/<name>`, each file with a `.meta.json` sidecar, and each
    complete snapshot's manifest at `<root>/<source>/_snapshots/<snapshot>.json`."""

    filesystem: AbstractFileSystem
    root: str

    def save(self, source: str, snapshot: str, name: str, download: Download) -> None:
        path = f"{self._source_path(source)}/{snapshot}/{name}"
        self.filesystem.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
        self.filesystem.pipe_file(path, download.body)
        self._write_json(path + META_SUFFIX, sidecar_record(download))

    def save_stream(
        self, source: str, snapshot: str, name: str, download: OpenedDownload
    ) -> StreamedDownload:
        path = f"{self._source_path(source)}/{snapshot}/{name}"
        self.filesystem.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
        digest, size = hashlib.sha256(), 0
        # The body is written beside the file and moved into place once read whole: closing an
        # S3 file completes its upload even when the body broke off, which would replace a
        # complete file with the part that arrived. (Aborting the upload instead would need no
        # move, but s3mock's abort deletes the object already at that key.)
        partial = path + PARTIAL_SUFFIX
        try:
            # On S3 the file is uploaded in parts as it is written (s3fs's blocks), so only a
            # block is held at a time.
            with self.filesystem.open(partial, "wb") as stored, compressing(stored, name) as file:
                for chunk in download.chunks:
                    digest.update(chunk)
                    size += len(chunk)
                    file.write(chunk)
        except BaseException:
            # The download's own error is what the caller retries on, so a failed cleanup must
            # not replace it; a partial file left over is overwritten by the next attempt.
            with suppress(OSError):
                self.filesystem.rm_file(partial)
            raise
        self.filesystem.mv(partial, path)
        streamed = StreamedDownload(
            url=download.url,
            fetched_at=download.fetched_at,
            last_modified=download.last_modified,
            etag=download.etag,
            size=size,
            sha256=digest.hexdigest(),
        )
        self._write_json(path + META_SUFFIX, sidecar_record(streamed))
        return streamed

    def complete(
        self,
        source: str,
        snapshot: str,
        downloads: Mapping[str, FetchedFile],
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

    def _source_path(self, source: str) -> str:
        return f"{self.root.rstrip('/')}/{source}"

    def _manifests_path(self, source: str) -> str:
        return f"{self._source_path(source)}/{MANIFESTS}"

    def _manifest_path(self, source: str, snapshot: str) -> str:
        return f"{self._manifests_path(source)}/{snapshot}.json"

    def _version(self, entry: Mapping[str, object]) -> str:
        """S3's ETag; a local directory's modification time is too coarse to tell two writes in a
        row apart, so there the (small) manifest is hashed."""
        etag = entry.get("ETag")
        if etag:
            return str(etag)
        return hashlib.sha256(self.filesystem.cat_file(str(entry["name"]))).hexdigest()

    def _write_json(self, path: str, values: Mapping[str, object]) -> None:
        self.filesystem.pipe_file(path, json.dumps(values, indent=2).encode())


def compressing(file: IO[bytes], name: str) -> AbstractContextManager[IO[bytes]]:
    """`file`, gzip-compressed as it is written if `name` ends in `.gz`; closing the result leaves
    `file` open."""
    if name.endswith(GZIP_SUFFIX):
        return gzip.GzipFile(fileobj=file, mode="wb", compresslevel=GZIP_LEVEL)
    return nullcontext(file)


def sidecar_record(download: FetchedFile) -> dict[str, object]:
    """The file's record, with the request it answered if any. Only the sidecar holds the request:
    a search's is larger than the rest of its record, and a manifest lists a whole sweep's pages."""
    record = file_record(download)
    if isinstance(download, Download) and download.request is not None:
        record["request"] = dict(download.request)
    return record


def open_fsspec_archive(url: str, options: Mapping[str, object] | None = None) -> FsspecRawArchive:
    """The archive at `url` (`s3://...` or a local path), its filesystem opened with `options`."""
    filesystem, root = url_to_fs(url, **(options or {}))
    return FsspecRawArchive(filesystem=filesystem, root=root)
