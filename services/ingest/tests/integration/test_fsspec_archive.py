import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dlt.common.storages.configuration import FilesystemConfiguration

from leads_ingest.adapters.archive.fsspec_archive import FsspecRawArchive, open_fsspec_archive
from leads_ingest.ports.raw_archive import SnapshotManifest, file_record
from leads_testing.ingest import build_download

COMPLETED_AT = datetime(2026, 10, 21, 3, 5, tzinfo=UTC)


@pytest.fixture
def archive(tmp_path: Path) -> FsspecRawArchive:
    return open_fsspec_archive(FilesystemConfiguration(bucket_url=str(tmp_path)))


def test_a_saved_download_is_kept_byte_for_byte_with_its_fetch_beside_it(
    archive: FsspecRawArchive, tmp_path: Path
) -> None:
    # Arrange
    download = build_download(
        b"PK\x03\x04 raw bytes",
        fetched_at=datetime(2026, 10, 14, 17, 34, tzinfo=UTC),
        last_modified="Tue, 29 Sep 2026 11:23:05 GMT",
    )

    # Act
    archive.save("sodra", "2026-09", "monthly-2026.csv.zip", download)

    # Assert
    path = tmp_path / "sodra" / "2026-09" / "monthly-2026.csv.zip"
    assert path.read_bytes() == b"PK\x03\x04 raw bytes"
    sidecar = json.loads(path.with_name("monthly-2026.csv.zip.meta.json").read_text())
    assert sidecar == file_record(download)
    assert sidecar["sha256"] == download.sha256


def test_a_snapshot_is_complete_only_once_its_manifest_is_written(
    archive: FsspecRawArchive,
) -> None:
    # Arrange
    download = build_download(b"first")
    archive.save("sodra", "2026-09", "monthly-2026.csv.zip", download)
    before = archive.snapshots("sodra")

    # Act
    archive.complete("sodra", "2026-09", {"monthly-2026.csv.zip": download}, COMPLETED_AT)

    # Assert
    assert before == {}
    assert list(archive.snapshots("sodra")) == ["2026-09"]
    assert archive.manifest("sodra", "2026-09") == SnapshotManifest(
        completed_at=COMPLETED_AT, files={"monthly-2026.csv.zip": file_record(download)}
    )


def test_completing_a_snapshot_again_changes_its_version(archive: FsspecRawArchive) -> None:
    # Arrange
    archive.complete("sodra", "2026-09", {"f.zip": build_download(b"first")}, COMPLETED_AT)
    first = archive.snapshots("sodra")["2026-09"]

    # Act
    later = datetime(2026, 10, 25, tzinfo=UTC)
    archive.complete("sodra", "2026-09", {"f.zip": build_download(b"second")}, later)

    # Assert
    assert archive.snapshots("sodra")["2026-09"] != first
    assert archive.manifest("sodra", "2026-09").completed_at == later


def test_snapshots_are_listed_per_source_with_any_key(archive: FsspecRawArchive) -> None:
    # Arrange
    for snapshot in ["2026-09", "2026-08"]:
        archive.complete("sodra", snapshot, {}, COMPLETED_AT)
    archive.complete("google", "2026-10-17", {}, COMPLETED_AT)

    # Act
    sodra, google = archive.snapshots("sodra"), archive.snapshots("google")

    # Assert
    assert sorted(sodra) == ["2026-08", "2026-09"]
    assert list(google) == ["2026-10-17"]


def test_a_source_never_completed_has_no_snapshots(archive: FsspecRawArchive) -> None:
    # Act / Assert
    assert archive.snapshots("sodra") == {}
