import pytest

from owl_ingest.sources.spinta.exports import archive_streamed_exports, require_rows_with_ids
from owl_testing.ingest import FakeStreamingDownloader, InMemoryRawArchive, build_download

EXPORT = b'_type,_id,ja_kodas\nJuridinisAsmuo,a1,"110000291"\n'


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"_type,_id,kodas\n", id="header only"),
        pytest.param(b"_type,_id,kodas\n\n", id="header and a blank line"),
        pytest.param(b"_type,kodas\nForma,310\n", id="no _id column"),
        pytest.param(b"", id="empty"),
        pytest.param(b"<html><body>500 Internal Server Error</body></html>\n", id="error page"),
    ],
)
def test_an_export_without_ids_or_rows_is_rejected(body: bytes) -> None:
    # Act / Assert
    with pytest.raises(ValueError, match=r"Forma\.csv"):
        require_rows_with_ids("Forma.csv", body)


def test_an_export_with_ids_and_a_row_is_accepted() -> None:
    # Act / Assert
    require_rows_with_ids("JuridinisAsmuo.csv", b'_type,_id,ja_kodas\nJuridinisAsmuo,a1,"1"\n')


def test_streamed_exports_are_archived_by_name_and_left_to_complete() -> None:
    # Arrange
    urls = {"Finansai.csv.gz": "https://jar.test/finansai/:format/csv"}
    downloader = FakeStreamingDownloader({urls["Finansai.csv.gz"]: [build_download(EXPORT)]})
    archive = InMemoryRawArchive()

    # Act
    archived = archive_streamed_exports("jar", "2026-09", urls, archive, downloader)

    # Assert
    assert (list(archived), archived["Finansai.csv.gz"].size) == (["Finansai.csv.gz"], len(EXPORT))
    assert archive.downloads[("jar", "2026-09", "Finansai.csv.gz")].body == EXPORT
    assert archive.snapshots("jar") == {}


def test_a_streamed_error_page_is_not_archived() -> None:
    # Arrange
    url = "https://jar.test/finansai/:format/csv"
    error_page = build_download(b"<html><body>500 Internal Server Error</body></html>\n")
    archive = InMemoryRawArchive()

    # Act / Assert
    with pytest.raises(ValueError, match=r"Finansai\.csv\.gz"):
        archive_streamed_exports(
            "jar",
            "2026-09",
            {"Finansai.csv.gz": url},
            archive,
            FakeStreamingDownloader({url: [error_page]}),
        )
    assert archive.downloads == {}
