import io
import zipfile

import pytest

from owl_ingest.months import Month
from owl_ingest.sources.sodra.files import archive_yearly_files, window_years
from owl_ingest.sources.sodra.settings import SodraSettings
from owl_testing.ingest import FakeDownloader, InMemoryRawArchive

URL_TEMPLATE = "https://sodra.test/{year}/monthly-{year}.csv.zip"
SETTINGS = SodraSettings(
    download_url_template=URL_TEMPLATE, history_months=3, snapshot_schedule="0 6 21 * *"
)


def zipped(*members: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for member in members:
            archive.writestr(member, b"header\n")
    return buffer.getvalue()


def test_a_window_spans_the_years_of_the_months_before_the_snapshots() -> None:
    # Act
    years = window_years(Month(2026, 2), history_months=3)

    # Assert
    assert years == [2025, 2026]


def test_a_january_run_needs_only_the_previous_year() -> None:
    # Act
    years = window_years(Month(2026, 1), history_months=3)

    # Assert
    assert years == [2025]


def test_a_fetch_archives_each_year_of_its_window_and_leaves_completing_it() -> None:
    # Arrange
    downloader = FakeDownloader(zipped("monthly.csv"))
    archive = InMemoryRawArchive()

    # Act
    archived = archive_yearly_files("2026-02", archive, SETTINGS, downloader)

    # Assert
    names = list(archived)
    assert names == ["monthly-2025.csv.zip", "monthly-2026.csv.zip"]
    assert archived["monthly-2026.csv.zip"].url == URL_TEMPLATE.format(year=2026)
    assert downloader.requested == [URL_TEMPLATE.format(year=2025), URL_TEMPLATE.format(year=2026)]
    assert set(archive.downloads) == {("sodra", "2026-02", name) for name in names}
    assert archive.snapshots("sodra") == {}


def test_a_zip_with_more_than_one_csv_is_not_archived() -> None:
    # Arrange
    downloader = FakeDownloader(zipped("monthly.csv", "extra.csv"))
    archive = InMemoryRawArchive()

    # Act / Assert
    with pytest.raises(ValueError, match=r"extra\.csv"):
        archive_yearly_files("2026-02", archive, SETTINGS, downloader)
    assert archive.downloads == {}
