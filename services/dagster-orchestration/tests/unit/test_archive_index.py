from leads_dagster_orchestration.archive_index import IndexChanges, index_changes


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
