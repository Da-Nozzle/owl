from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from dagster._core.definitions.partitions.context import partition_loading_context

from leads_dagster_orchestration.snapshots import (
    ArchivedSource,
    SnapshotCadence,
    snapshot_partitions,
)
from leads_ingest.settings import PipelineMode

VILNIUS = ZoneInfo("Europe/Vilnius")
MONTHLY = SnapshotCadence(schedule="0 6 21 * *", key_format="%Y-%m", timezone="Europe/Vilnius")
DAILY = SnapshotCadence(schedule="0 3 * * *", key_format="%Y-%m-%d", timezone="Europe/Vilnius")


@pytest.mark.parametrize(
    ("moment", "snapshot"),
    [
        (datetime(2026, 10, 21, 5, 59, tzinfo=VILNIUS), "2026-09"),
        (datetime(2026, 10, 21, 6, 0, tzinfo=VILNIUS), "2026-10"),
        (datetime(2026, 11, 3, 12, 0, tzinfo=VILNIUS), "2026-10"),
    ],
)
def test_a_monthly_snapshot_is_keyed_by_the_month_of_the_tick_it_follows(
    moment: datetime, snapshot: str
) -> None:
    # Act / Assert
    assert MONTHLY.snapshot_at(moment) == snapshot


@pytest.mark.parametrize(
    ("moment", "snapshot"),
    [
        (datetime(2026, 10, 17, 2, 59, tzinfo=VILNIUS), "2026-10-16"),
        (datetime(2026, 10, 17, 3, 0, tzinfo=VILNIUS), "2026-10-17"),
    ],
)
def test_a_daily_snapshot_is_keyed_by_its_day(moment: datetime, snapshot: str) -> None:
    # Act / Assert
    assert DAILY.snapshot_at(moment) == snapshot


def test_each_lake_and_source_has_its_own_partitions() -> None:
    # Act
    names = {
        snapshot_partitions(source, lake).name
        for source in ("sodra", "google")
        for lake in PipelineMode
    }

    # Assert
    assert names == {
        "live_sodra_snapshots",
        "replay_sodra_snapshots",
        "live_google_snapshots",
        "replay_google_snapshots",
    }


@pytest.mark.parametrize(
    ("snapshot", "window"),
    [
        pytest.param(
            "2026-10-17",
            (
                datetime(2026, 10, 16, 3, 0, tzinfo=VILNIUS),
                datetime(2026, 10, 17, 3, 0, tzinfo=VILNIUS),
            ),
            id="a day",
        ),
        pytest.param(
            "2026-10-26",
            (
                datetime(2026, 10, 25, 3, 0, tzinfo=VILNIUS),
                datetime(2026, 10, 26, 3, 0, tzinfo=VILNIUS),
            ),
            id="the day clocks go back",
        ),
    ],
)
def test_a_snapshot_holds_what_was_new_in_the_period_before_its_own(
    snapshot: str, window: tuple[datetime, datetime]
) -> None:
    # Act
    start, end = DAILY.window_before(snapshot)

    # Assert
    assert (start, end) == window
    assert DAILY.snapshot_at(end) == snapshot


def test_a_month_holds_what_was_new_since_the_previous_months_tick() -> None:
    # Act / Assert
    assert MONTHLY.window_before("2026-10") == (
        datetime(2026, 9, 21, 6, 0, tzinfo=VILNIUS),
        datetime(2026, 10, 21, 6, 0, tzinfo=VILNIUS),
    )


def test_a_source_with_history_can_be_fetched_for_every_period_up_to_the_current_one() -> None:
    # Arrange
    source = ArchivedSource(name="vmi", cadence=DAILY, history_start="2026-09-01")

    # Act
    periods = source.fetch_periods()
    assert periods is not None
    with partition_loading_context(effective_dt=datetime(2026, 10, 17, 3, 0, tzinfo=VILNIUS)):
        keys = periods.get_partition_keys()

    # Assert
    assert (keys[0], keys[-1], len(keys)) == ("2026-09-01", "2026-10-17", 47)


def test_a_source_without_history_has_no_periods_to_fetch() -> None:
    # Act / Assert
    assert ArchivedSource(name="sodra", cadence=MONTHLY).fetch_periods() is None
