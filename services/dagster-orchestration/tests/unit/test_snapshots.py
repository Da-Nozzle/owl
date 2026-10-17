from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from leads_dagster_orchestration.snapshots import SnapshotCadence, snapshot_partitions
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
