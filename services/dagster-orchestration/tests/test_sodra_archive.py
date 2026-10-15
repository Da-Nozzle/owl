from datetime import UTC, datetime

import dagster as dg
import pytest

from leads_dagster_orchestration.assets.sodra import files_metadata, require_current_run_month
from leads_ingest.ports.downloader import Download

# Run month M starts on the 21st of M at 06:00 in Vilnius (UTC+3 in October).
PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2026-05",
    fmt="%Y-%m",
    day_offset=21,
    hour_offset=6,
    timezone="Europe/Vilnius",
    end_offset=1,
)


def test_the_current_run_month_downloads() -> None:
    # Act / Assert
    require_current_run_month(PARTITIONS, "2026-09", datetime(2026, 10, 15, tzinfo=UTC))


def test_a_run_month_that_has_passed_does_not_download() -> None:
    # Act / Assert
    with pytest.raises(dg.Failure, match=r"2026-08 has passed \(the current one is 2026-09\)"):
        require_current_run_month(PARTITIONS, "2026-08", datetime(2026, 10, 15, tzinfo=UTC))


def test_a_run_month_passes_when_the_next_one_starts() -> None:
    # Arrange
    next_run_month_started = datetime(2026, 10, 21, 3, 1, tzinfo=UTC)

    # Act / Assert
    with pytest.raises(dg.Failure, match=r"archive/archive_sodra"):
        require_current_run_month(PARTITIONS, "2026-09", next_run_month_started)


def test_files_metadata_shows_each_files_fetch() -> None:
    # Arrange
    fetched_at = datetime(2026, 10, 15, 19, 5, tzinfo=UTC)
    download = Download("https://sodra.test/2026", b"PK", fetched_at, "Tue, 29 Sep 2026")

    # Act
    metadata = files_metadata({"monthly-2026.csv.zip": download})

    # Assert
    assert metadata == {
        "monthly-2026.csv.zip": {
            "url": "https://sodra.test/2026",
            "fetched_at": "2026-10-15T19:05:00+00:00",
            "last_modified": "Tue, 29 Sep 2026",
            "size": 2,
            "sha256": download.sha256,
        }
    }
