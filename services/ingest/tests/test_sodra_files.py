import io
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest

from leads_ingest.months import Month
from leads_ingest.ports.downloader import Download
from leads_ingest.sources.sodra.files import archive_yearly_files, window_years
from leads_ingest.sources.sodra.settings import SodraSettings

URL_TEMPLATE = "https://sodra.test/{year}/monthly-{year}.csv.zip"
FETCHED_AT = datetime(2026, 9, 29, 11, 23, tzinfo=UTC)
SETTINGS = SodraSettings(download_url_template=URL_TEMPLATE, history_months=3)


def zipped(*members: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for member in members:
            archive.writestr(member, b"header\n")
    return buffer.getvalue()


@dataclass
class FakeDownloader:
    body: bytes
    requested: list[str] = field(default_factory=list)

    def download(self, url: str) -> Download:
        self.requested.append(url)
        return Download(url, self.body, FETCHED_AT, None)


@dataclass
class InMemoryArchive:
    downloads: dict[tuple[str, Month, str], Download] = field(default_factory=dict)

    def save(self, source: str, month: Month, name: str, download: Download) -> None:
        self.downloads[(source, month, name)] = download

    def load(self, source: str, month: Month, name: str) -> Download:
        return self.downloads[(source, month, name)]

    def exists(self, source: str, month: Month, name: str) -> bool:
        return (source, month, name) in self.downloads


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
    archive = InMemoryArchive()

    # Act
    names = archive_yearly_files(Month(2026, 2), archive, SETTINGS, downloader)

    # Assert
    assert names == ["monthly-2025.csv.zip", "monthly-2026.csv.zip"]
    assert downloader.requested == [URL_TEMPLATE.format(year=2025), URL_TEMPLATE.format(year=2026)]
    assert set(archive.downloads) == {("sodra", Month(2026, 2), name) for name in names}


def test_a_zip_with_more_than_one_csv_is_not_archived() -> None:
    # Arrange
    downloader = FakeDownloader(zipped("monthly.csv", "extra.csv"))
    archive = InMemoryArchive()

    # Act / Assert
    with pytest.raises(ValueError, match=r"extra\.csv"):
        archive_yearly_files(Month(2026, 2), archive, SETTINGS, downloader)
    assert archive.downloads == {}


def test_replay_downloads_nothing_and_needs_the_files_archived() -> None:
    # Arrange
    archive = InMemoryArchive()
    archive.save(
        "sodra", Month(2026, 2), "monthly-2025.csv.zip", Download("u", b"", FETCHED_AT, None)
    )

    # Act / Assert
    with pytest.raises(FileNotFoundError, match=r"sodra/2026-02/monthly-2026.csv.zip"):
        archive_yearly_files(Month(2026, 2), archive, SETTINGS, downloader=None)
