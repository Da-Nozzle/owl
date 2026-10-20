import gzip
import json
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from owl_config.configuration import Configuration
from owl_ingest.adapters.archive.fsspec_archive import FsspecRawArchive, open_fsspec_archive
from owl_ingest.adapters.s3.configuration import s3fs_options
from owl_ingest.ports.downloader import OpenedDownload
from owl_ingest.ports.raw_archive import SnapshotManifest, file_record
from owl_lake.settings import load_lake_storage
from owl_testing.ingest import build_download

COMPLETED_AT = datetime(2026, 10, 21, 3, 5, tzinfo=UTC)


@pytest.fixture(params=["local", "s3"])
def archive(request: pytest.FixtureRequest, tmp_path: Path) -> FsspecRawArchive:
    """In a folder, as replay reads its sample, and on s3mock, as live keeps its archive (in the
    test's own folder of a bucket of the session's)."""
    if request.param == "local":
        return open_fsspec_archive(str(tmp_path))
    config: Configuration = request.getfixturevalue("local_config")
    prefix: str = request.getfixturevalue("s3_prefix")
    return open_fsspec_archive(f"{prefix}/archive/", s3fs_options(load_lake_storage(config)))


def stored(archive: FsspecRawArchive, path: str) -> bytes:
    """The file at `path` below the archive's root."""
    return archive.filesystem.cat_file(f"{archive.root.rstrip('/')}/{path}")


def folder_of(archive: FsspecRawArchive, path: str) -> dict[str, bytes]:
    """Each file in the folder at `path` below the archive's root, by name."""
    folder = f"{archive.root.rstrip('/')}/{path}"
    return {
        name.rsplit("/", 1)[-1]: archive.filesystem.cat_file(name)
        for name in archive.filesystem.ls(folder, detail=False)
    }


def test_a_saved_download_is_kept_byte_for_byte_with_its_fetch_beside_it(
    archive: FsspecRawArchive,
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
    assert stored(archive, "sodra/2026-09/monthly-2026.csv.zip") == b"PK\x03\x04 raw bytes"
    sidecar = json.loads(stored(archive, "sodra/2026-09/monthly-2026.csv.zip.meta.json"))
    assert sidecar == file_record(download)
    assert sidecar["sha256"] == download.sha256


def opened(chunks: Iterable[bytes]) -> OpenedDownload:
    """A download of `chunks`, as `build_download` would record their body whole."""
    recorded = build_download(last_modified="Sun, 18 Oct 2026 02:40:18 GMT")
    return OpenedDownload(
        url=recorded.url,
        fetched_at=recorded.fetched_at,
        last_modified=recorded.last_modified,
        etag=recorded.etag,
        chunks=iter(chunks),
    )


def broken_off(chunks: list[bytes]) -> Iterator[bytes]:
    """`chunks`, then the connection dropped, as get.data.gov.lt drops a long export."""
    yield from chunks
    raise httpx.RemoteProtocolError("peer closed connection without sending complete message")


def test_a_streamed_download_is_kept_as_it_arrived_and_recorded_as_a_saved_one(
    archive: FsspecRawArchive,
) -> None:
    # Arrange
    chunks = [b"_id,darbo_vietos_id\n", b"v1,DV-59-", b"996710668\n"]
    whole = build_download(b"".join(chunks), last_modified="Sun, 18 Oct 2026 02:40:18 GMT")

    # Act
    streamed = archive.save_stream("uzt_vacancies", "2026-09", "Vieta.csv", opened(chunks))
    archive.complete("uzt_vacancies", "2026-09", {"Vieta.csv": streamed}, COMPLETED_AT)

    # Assert
    assert stored(archive, "uzt_vacancies/2026-09/Vieta.csv") == whole.body
    sidecar = json.loads(stored(archive, "uzt_vacancies/2026-09/Vieta.csv.meta.json"))
    assert sidecar == file_record(streamed) == file_record(whole)
    assert archive.manifest("uzt_vacancies", "2026-09").files == {"Vieta.csv": sidecar}


def test_a_streamed_gz_file_is_compressed_and_recorded_as_sent(
    archive: FsspecRawArchive,
) -> None:
    # Arrange
    chunks = [b"_id,kodas\n", b"r1,310\n" * 1000]
    whole = build_download(b"".join(chunks), last_modified="Sun, 18 Oct 2026 02:40:18 GMT")

    # Act
    streamed = archive.save_stream("jar", "2026-09", "Finansai.csv.gz", opened(chunks))

    # Assert
    compressed = stored(archive, "jar/2026-09/Finansai.csv.gz")
    assert gzip.decompress(compressed) == whole.body
    assert len(compressed) < whole.size
    assert file_record(streamed) == file_record(whole)


def test_a_stream_saved_again_starts_its_file_over(archive: FsspecRawArchive) -> None:
    # Arrange
    archive.save_stream("jar", "2026-09", "Finansai.csv.gz", opened([b"_id\n", b"r1\n" * 50]))

    # Act
    archive.save_stream("jar", "2026-09", "Finansai.csv.gz", opened([b"_id\nr2\n"]))

    # Assert
    assert gzip.decompress(stored(archive, "jar/2026-09/Finansai.csv.gz")) == b"_id\nr2\n"


def test_a_stream_is_moved_into_place_only_once_it_arrived_whole(
    archive: FsspecRawArchive,
) -> None:
    # Act
    archive.save_stream("uzt_vacancies", "2026-09", "Vieta.csv", opened([b"_id\n", b"v1\n"]))

    # Assert
    assert sorted(folder_of(archive, "uzt_vacancies/2026-09")) == [
        "Vieta.csv",
        "Vieta.csv.meta.json",
    ]


def test_a_stream_that_broke_off_leaves_the_file_it_would_have_replaced(
    archive: FsspecRawArchive,
) -> None:
    # Arrange
    complete = archive.save_stream("jar", "2026-09", "Finansai.csv.gz", opened([b"_id\nr1\n"]))
    before = folder_of(archive, "jar/2026-09")

    # Act
    with pytest.raises(httpx.RemoteProtocolError):
        archive.save_stream("jar", "2026-09", "Finansai.csv.gz", opened(broken_off([b"_id\n"])))

    # Assert
    assert folder_of(archive, "jar/2026-09") == before
    assert gzip.decompress(before["Finansai.csv.gz"]) == b"_id\nr1\n"
    assert json.loads(before["Finansai.csv.gz.meta.json"]) == file_record(complete)


def test_the_request_a_file_answered_is_kept_beside_it_and_not_in_the_manifest(
    archive: FsspecRawArchive,
) -> None:
    # Arrange
    request = {"field_mask": "places.id", "body": {"textQuery": "kavinė", "pageToken": "t" * 1000}}
    download = build_download(b'{"places": []}', request=request)

    # Act
    archive.save("google_places", "2026-09", "places-kavine-r-1.json", download)
    archive.complete("google_places", "2026-09", {"places-kavine-r-1.json": download}, COMPLETED_AT)

    # Assert
    sidecar = stored(archive, "google_places/2026-09/places-kavine-r-1.json.meta.json")
    assert json.loads(sidecar) == {**file_record(download), "request": request}
    manifest = archive.manifest("google_places", "2026-09")
    assert manifest.files == {"places-kavine-r-1.json": file_record(download)}
    assert "request" not in file_record(download)


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
