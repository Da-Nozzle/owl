import pytest

from owl_dagster_orchestration.archive.index import (
    IndexChanges,
    index_changes,
    load_archive_index_settings,
)
from owl_testing.config import build_config


def test_a_new_snapshot_gets_a_partition_and_is_reported() -> None:
    # Act
    changes = index_changes(
        registered={"2026-08"}, seen={"2026-08": "v1"}, current={"2026-08": "v1", "2026-09": "v1"}
    )

    # Assert
    assert changes == IndexChanges(added=["2026-09"], removed=[], completed=["2026-09"])


def test_nothing_changes_while_the_archive_does_not() -> None:
    # Act
    changes = index_changes(
        registered={"2026-09"}, seen={"2026-09": "v1"}, current={"2026-09": "v1"}
    )

    # Assert
    assert changes == IndexChanges(added=[], removed=[], completed=[])


def test_a_snapshot_completed_again_is_reported_again() -> None:
    # Act
    changes = index_changes(
        registered={"2026-09"}, seen={"2026-09": "v1"}, current={"2026-09": "v2"}
    )

    # Assert
    assert changes == IndexChanges(added=[], removed=[], completed=["2026-09"])


def test_a_snapshot_gone_from_the_archive_loses_its_partition() -> None:
    # Act
    changes = index_changes(
        registered={"2026-08", "2026-09"},
        seen={"2026-08": "v1", "2026-09": "v1"},
        current={"2026-09": "v1"},
    )

    # Assert
    assert changes == IndexChanges(added=[], removed=["2026-08"], completed=[])


def test_partitions_lost_with_the_instance_come_back_from_the_archive() -> None:
    # Act
    changes = index_changes(registered=set(), seen={"2026-09": "v1"}, current={"2026-09": "v1"})

    # Assert
    assert changes == IndexChanges(added=["2026-09"], removed=[], completed=["2026-09"])


def test_the_index_must_look_at_the_archive_at_least_every_second() -> None:
    # Arrange
    config = build_config({"archive_index.sensor_interval_seconds": 0})

    # Act / Assert
    with pytest.raises(ValueError, match="sensor_interval_seconds"):
        load_archive_index_settings(config)
