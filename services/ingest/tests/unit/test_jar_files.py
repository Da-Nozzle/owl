import pytest

from leads_ingest.sources.jar.files import archive_register_files, require_rows_with_ids
from leads_ingest.sources.jar.settings import JarSettings
from leads_testing.ingest import FakeDownloader, InMemoryRawArchive

URL_TEMPLATE = "https://jar.test/{dataset}/:format/csv"
SETTINGS = JarSettings(download_url_template=URL_TEMPLATE, snapshot_schedule="0 6 21 * *")
EXPORT = b'_type,_id,_revision,ja_kodas\nJuridinisAsmuo,a1,r1,"110000291"\n'


def test_a_fetch_archives_each_dataset_by_name_and_leaves_completing_it() -> None:
    # Arrange
    downloader = FakeDownloader(EXPORT)
    archive = InMemoryRawArchive()

    # Act
    archived = archive_register_files("2026-09", archive, SETTINGS, downloader)

    # Assert
    names = list(archived)
    assert names == ["JuridinisAsmuo.csv", "Forma.csv", "Statusas.csv"]
    assert downloader.requested == [
        URL_TEMPLATE.format(dataset="iregistruoti/JuridinisAsmuo"),
        URL_TEMPLATE.format(dataset="formos_statusai/Forma"),
        URL_TEMPLATE.format(dataset="formos_statusai/Statusas"),
    ]
    assert set(archive.downloads) == {("jar", "2026-09", name) for name in names}
    assert archive.snapshots("jar") == {}


def test_an_error_page_sent_as_a_success_is_not_archived() -> None:
    # Arrange
    downloader = FakeDownloader(b"<html><body>500 Internal Server Error</body></html>\n")
    archive = InMemoryRawArchive()

    # Act / Assert
    with pytest.raises(ValueError, match=r"JuridinisAsmuo\.csv"):
        archive_register_files("2026-09", archive, SETTINGS, downloader)
    assert archive.downloads == {}


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(b"_type,_id,kodas\n", id="header only"),
        pytest.param(b"_type,_id,kodas\n\n", id="header and a blank line"),
        pytest.param(b"_type,kodas\nForma,310\n", id="no _id column"),
        pytest.param(b"", id="empty"),
    ],
)
def test_an_export_without_ids_or_rows_is_rejected(body: bytes) -> None:
    # Act / Assert
    with pytest.raises(ValueError, match=r"Forma\.csv"):
        require_rows_with_ids("Forma.csv", body)


def test_an_export_with_ids_and_a_row_is_accepted() -> None:
    # Act / Assert
    require_rows_with_ids("JuridinisAsmuo.csv", EXPORT)
