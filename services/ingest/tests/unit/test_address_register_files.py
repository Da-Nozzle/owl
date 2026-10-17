import pytest

from leads_ingest.sources.address_register.files import archive_register_files
from leads_ingest.sources.address_register.settings import AddressRegisterSettings
from leads_testing.ingest import FakeDownloader, InMemoryRawArchive

URL_TEMPLATE = "https://ar.test/{dataset}/:format/csv"
SETTINGS = AddressRegisterSettings(
    download_url_template=URL_TEMPLATE, snapshot_schedule="0 6 21 * *"
)
EXPORT = b'_type,_id,_revision,_page.next,nr\nPastatas,b1,r1,,"23"\n'


def test_a_fetch_archives_each_dataset_by_name_and_leaves_completing_it() -> None:
    # Arrange
    downloader = FakeDownloader(EXPORT)
    archive = InMemoryRawArchive()

    # Act
    archived = archive_register_files("2026-09", archive, SETTINGS, downloader)

    # Assert
    names = list(archived)
    assert names == [
        "Pastatas.csv",
        "Patalpa.csv",
        "Gatve.csv",
        "GyvenamojiVietove.csv",
        "Savivaldybe.csv",
    ]
    assert downloader.requested == [
        URL_TEMPLATE.format(dataset=dataset)
        for dataset in (
            "pastatas/Pastatas",
            "patalpa/Patalpa",
            "gatve/Gatve",
            "gyvenamojivietove/GyvenamojiVietove",
            "savivaldybe/Savivaldybe",
        )
    ]
    assert set(archive.downloads) == {("address_register", "2026-09", name) for name in names}
    assert archive.snapshots("address_register") == {}


def test_an_empty_export_is_not_archived() -> None:
    # Arrange
    downloader = FakeDownloader(b"_type,_id,_revision,_page.next,nr\n")
    archive = InMemoryRawArchive()

    # Act / Assert
    with pytest.raises(ValueError, match=r"Pastatas\.csv"):
        archive_register_files("2026-09", archive, SETTINGS, downloader)
    assert archive.downloads == {}
