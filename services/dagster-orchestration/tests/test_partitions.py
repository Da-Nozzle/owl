from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from leads_dagster_orchestration.partitions import replay_month_partitions, run_month_partitions
from leads_ingest.months import Month
from leads_ingest.settings import PipelineMode, PipelineSettings

VILNIUS = ZoneInfo("Europe/Vilnius")
SETTINGS = PipelineSettings(
    mode=PipelineMode.REPLAY,
    first_month=Month(2026, 8),
    run_day=21,
    run_hour=6,
    timezone="Europe/Vilnius",
    user_agent="test",
    replay_path=Path("/replay"),
)


def test_a_run_month_has_no_partition_before_its_run_day() -> None:
    # Act
    keys = run_month_partitions(SETTINGS).get_partition_keys(
        current_time=datetime(2026, 10, 21, 5, 59, tzinfo=VILNIUS)
    )

    # Assert
    assert keys == ["2026-08", "2026-09"]


def test_a_run_month_gets_its_partition_at_its_run_hour() -> None:
    # Act
    keys = run_month_partitions(SETTINGS).get_partition_keys(
        current_time=datetime(2026, 10, 21, 6, 0, tzinfo=VILNIUS)
    )

    # Assert
    assert keys == ["2026-08", "2026-09", "2026-10"]


def test_replay_partitions_are_the_sample_months_whatever_the_date() -> None:
    # Arrange
    partitions = replay_month_partitions([Month(2026, 9), Month(2026, 10)], SETTINGS)

    # Act
    keys_by_date = [
        partitions.get_partition_keys(current_time=datetime(2026, 11, 1, tzinfo=VILNIUS)),
        partitions.get_partition_keys(current_time=datetime(2028, 1, 1, tzinfo=VILNIUS)),
    ]

    # Assert
    assert keys_by_date == [["2026-09", "2026-10"], ["2026-09", "2026-10"]]


def test_replay_partitions_ignore_first_month_and_run_day() -> None:
    # Arrange
    partitions = replay_month_partitions([Month(2026, 9)], SETTINGS)

    # Act
    keys = partitions.get_partition_keys(current_time=datetime(2026, 10, 15, tzinfo=VILNIUS))

    # Assert
    assert keys == ["2026-09"]


def test_a_gap_in_the_replay_sample_is_rejected() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match=r"lacks run months \['2026-09'\]"):
        replay_month_partitions([Month(2026, 8), Month(2026, 10)], SETTINGS)


def test_an_empty_replay_sample_is_rejected() -> None:
    # Act / Assert
    with pytest.raises(ValueError, match="holds no run month"):
        replay_month_partitions([], SETTINGS)
