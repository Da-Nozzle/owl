import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from dlt.common.storages.configuration import FilesystemConfiguration

from leads_ingest.adapters.archive.fsspec_archive import FsspecRawArchive, open_fsspec_archive
from leads_ingest.months import Month
from leads_testing.ingest import build_download


@pytest.fixture
def archive(tmp_path: Path) -> FsspecRawArchive:
    return open_fsspec_archive(FilesystemConfiguration(bucket_url=str(tmp_path)))


def test_a_saved_download_loads_back_with_its_metadata(
    archive: FsspecRawArchive, tmp_path: Path
) -> None:
    # Arrange
    download = build_download(
        b"PK\x03\x04 raw bytes",
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


def test_saving_again_replaces_the_file_and_its_metadata(archive: FsspecRawArchive) -> None:
    # Arrange
    archive.save("sodra", Month(2026, 9), "file.zip", build_download(b"first"))
    rerun = build_download(b"second", last_modified="Wed, 30 Sep 2026 08:00:00 GMT")

    # Act
    archive.save("sodra", Month(2026, 9), "file.zip", rerun)

    # Assert
    assert archive.load("sodra", Month(2026, 9), "file.zip") == rerun


def test_exists_finds_only_saved_files(archive: FsspecRawArchive) -> None:
    # Arrange
    archive.save("sodra", Month(2026, 9), "file.zip", build_download())

    # Act / Assert
    assert archive.exists("sodra", Month(2026, 9), "file.zip")
    assert not archive.exists("sodra", Month(2026, 10), "file.zip")


def test_a_missing_file_names_its_archive_path(archive: FsspecRawArchive) -> None:
    # Act / Assert
    with pytest.raises(FileNotFoundError, match=r"sodra/2026-09/missing.zip"):
        archive.load("sodra", Month(2026, 9), "missing.zip")


def test_months_lists_a_sources_run_months_oldest_first(
    archive: FsspecRawArchive, tmp_path: Path
) -> None:
    # Arrange
    for month in [Month(2026, 10), Month(2025, 12), Month(2026, 1)]:
        archive.save("sodra", month, "file.zip", build_download())
    archive.save("google", Month(2026, 11), "file.json", build_download())
    (tmp_path / "sodra" / "notes").mkdir()

    # Act
    months = archive.months("sodra")

    # Assert
    assert months == [Month(2025, 12), Month(2026, 1), Month(2026, 10)]


def test_a_source_with_nothing_archived_has_no_months(archive: FsspecRawArchive) -> None:
    # Act / Assert
    assert archive.months("sodra") == []
