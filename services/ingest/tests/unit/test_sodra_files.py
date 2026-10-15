import io
import zipfile

import pytest

from leads_ingest.months import Month
from leads_ingest.sources.sodra.files import archive_yearly_files, window_years
from leads_ingest.sources.sodra.settings import SodraSettings
from leads_testing.ingest import FakeDownloader, InMemoryRawArchive, build_download

URL_TEMPLATE = "https://sodra.test/{year}/monthly-{year}.csv.zip"
SETTINGS = SodraSettings(download_url_template=URL_TEMPLATE, history_months=3)


def zipped(*members: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for member in members:
            archive.writestr(member, b"header\n")
    return buffer.getvalue()


def test_a_window_spans_the_years_of_the_months_before_the_run_month() -> None:
    # Act
    years = window_years(Month(2026, 2), history_months=3)

    # Assert
    assert years == [2025, 2026]


def test_a_january_run_needs_only_the_previous_year() -> None:
    # Act
    years = window_years(Month(2026, 1), history_months=3)

    # Assert
    assert years == [2025]


def test_live_run_archives_each_year_of_its_window() -> None:
    # Arrange
    downloader = FakeDownloader(zipped("monthly.csv"))
    archive = InMemoryRawArchive()

    # Act
    archived = archive_yearly_files(Month(2026, 2), archive, SETTINGS, downloader)

    # Assert
    names = list(archived)
    assert names == ["monthly-2025.csv.zip", "monthly-2026.csv.zip"]
    assert archived["monthly-2026.csv.zip"].url == URL_TEMPLATE.format(year=2026)
    assert downloader.requested == [URL_TEMPLATE.format(year=2025), URL_TEMPLATE.format(year=2026)]
    assert set(archive.downloads) == {("sodra", Month(2026, 2), name) for name in names}


def test_a_zip_with_more_than_one_csv_is_not_archived() -> None:
    # Arrange
    downloader = FakeDownloader(zipped("monthly.csv", "extra.csv"))
    archive = InMemoryRawArchive()

    # Act / Assert
    with pytest.raises(ValueError, match=r"extra\.csv"):
        archive_yearly_files(Month(2026, 2), archive, SETTINGS, downloader)
    assert archive.downloads == {}


def test_replay_fails_on_a_year_missing_from_the_archive() -> None:
    # Arrange
    archive = InMemoryRawArchive()
    archive.save("sodra", Month(2026, 2), "monthly-2025.csv.zip", build_download())

    # Act / Assert
    with pytest.raises(FileNotFoundError, match=r"sodra/2026-02/monthly-2026.csv.zip"):
        archive_yearly_files(Month(2026, 2), archive, SETTINGS, downloader=None)


def test_replay_returns_the_archived_downloads() -> None:
    # Arrange
    archive = InMemoryRawArchive()
    saved = {
        f"monthly-{year}.csv.zip": build_download(url=URL_TEMPLATE.format(year=year))
        for year in (2025, 2026)
    }
    for name, download in saved.items():
        archive.save("sodra", Month(2026, 2), name, download)

    # Act
    archived = archive_yearly_files(Month(2026, 2), archive, SETTINGS, downloader=None)

    # Assert
    assert archived == saved
