import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dlt.common.storages.configuration import FilesystemConfiguration

from leads_ingest.adapters.archive.fsspec_archive import open_fsspec_archive
from leads_ingest.months import Month
from leads_ingest.ports.downloader import Download


def test_a_saved_download_loads_back_with_its_metadata(tmp_path: Path) -> None:
    # Arrange
    archive = open_fsspec_archive(FilesystemConfiguration(bucket_url=str(tmp_path)))
    download = Download(
        url="https://source.test/file.zip",
        body=b"PK\x03\x04 raw bytes",
        fetched_at=datetime(2026, 10, 14, 17, 34, tzinfo=UTC),
        last_modified="Tue, 29 Sep 2026 11:23:05 GMT",
    )

    # Act
    archive.save("sodra", Month(2026, 9), "file.zip", download)
    loaded = archive.load("sodra", Month(2026, 9), "file.zip")

    # Assert
    assert loaded == download
    meta = json.loads((tmp_path / "sodra" / "2026-09" / "file.zip.meta.json").read_text())
    assert (meta["size"], meta["sha256"]) == (download.size, download.sha256)


def test_exists_finds_only_saved_files(tmp_path: Path) -> None:
    # Arrange
    archive = open_fsspec_archive(FilesystemConfiguration(bucket_url=str(tmp_path)))
    download = Download("https://source.test/file.zip", b"PK", datetime.now(UTC), None)
    archive.save("sodra", Month(2026, 9), "file.zip", download)

    # Act / Assert
    assert archive.exists("sodra", Month(2026, 9), "file.zip")
    assert not archive.exists("sodra", Month(2026, 10), "file.zip")


def test_a_missing_file_names_its_archive_path(tmp_path: Path) -> None:
    # Arrange
    archive = open_fsspec_archive(FilesystemConfiguration(bucket_url=str(tmp_path)))

    # Act / Assert
    with pytest.raises(FileNotFoundError, match=r"sodra/2026-09/missing.zip"):
        archive.load("sodra", Month(2026, 9), "missing.zip")


def test_months_lists_a_sources_run_months_oldest_first(tmp_path: Path) -> None:
    # Arrange
    archive = open_fsspec_archive(FilesystemConfiguration(bucket_url=str(tmp_path)))
    download = Download("https://source.test/file.zip", b"PK", datetime.now(UTC), None)
    for month in [Month(2026, 10), Month(2025, 12), Month(2026, 1)]:
        archive.save("sodra", month, "file.zip", download)
    archive.save("google", Month(2026, 11), "file.json", download)
    (tmp_path / "sodra" / "notes").mkdir()

    # Act
    months = archive.months("sodra")

    # Assert
    assert months == [Month(2025, 12), Month(2026, 1), Month(2026, 10)]


def test_a_source_with_nothing_archived_has_no_months(tmp_path: Path) -> None:
    # Arrange
    archive = open_fsspec_archive(FilesystemConfiguration(bucket_url=str(tmp_path)))

    # Act / Assert
    assert archive.months("sodra") == []


def test_a_downloads_size_and_sha256_are_of_its_bytes() -> None:
    # Arrange
    download = Download("https://source.test/file.zip", b"PK raw", datetime.now(UTC), None)

    # Act / Assert
    assert download.size == len(b"PK raw")
    assert download.sha256 == hashlib.sha256(b"PK raw").hexdigest()
