from collections.abc import Iterator
from datetime import UTC, datetime

import dagster as dg
import pytest

from leads_dagster_orchestration.archive_index import build_archive_index
from leads_dagster_orchestration.snapshots import (
    ArchivedSource,
    SnapshotCadence,
    snapshot_partitions,
)
from leads_ingest.settings import PipelineMode
from leads_testing.ingest import InMemoryRawArchive, build_download

LAKE = PipelineMode.LIVE
SOURCE = ArchivedSource(
    name="sodra",
    cadence=SnapshotCadence(schedule="0 6 21 * *", key_format="%Y-%m", timezone="Europe/Vilnius"),
)
PARTITIONS = snapshot_partitions("sodra", LAKE)
LABELS = {"leads/mode": "live", "leads/lake": "ducklake"}
ARCHIVE = dg.AssetKey(["live", "archive", "archive_sodra"])
COMPLETED_AT = datetime(2026, 10, 21, 3, 5, tzinfo=UTC)


@pytest.fixture
def instance() -> Iterator[dg.DagsterInstance]:
    with dg.instance_for_test() as instance:
        yield instance


def tick(
    archive: InMemoryRawArchive, instance: dg.DagsterInstance, cursor: str | None = None
) -> dg.SensorResult:
    """Evaluates the index and applies its partition requests, as the sensor daemon does."""
    sensor = build_archive_index([SOURCE], archive, LAKE, {"sodra": PARTITIONS}, LABELS)
    result = sensor(dg.build_sensor_context(instance=instance, cursor=cursor))
    assert isinstance(result, dg.SensorResult)
    for request in result.dynamic_partitions_requests or []:
        if isinstance(request, dg.AddDynamicPartitionsRequest):
            instance.add_dynamic_partitions(request.partitions_def_name, request.partition_keys)
        else:
            for key in request.partition_keys:
                instance.delete_dynamic_partition(request.partitions_def_name, key)
    return result


def reported(result: dg.SensorResult) -> list[str | None]:
    return [event.partition for event in result.asset_events]


def test_a_complete_snapshot_becomes_a_partition_and_an_archive_materialization(
    instance: dg.DagsterInstance,
) -> None:
    # Arrange
    archive = InMemoryRawArchive()
    files = {"monthly-2026.csv.zip": build_download(b"2026")}
    archive.complete("sodra", "2026-09", files, COMPLETED_AT)

    # Act
    result = tick(archive, instance)

    # Assert
    assert instance.get_dynamic_partitions(PARTITIONS.name) == ["2026-09"]
    [event] = result.asset_events
    assert isinstance(event, dg.AssetMaterialization)
    assert (event.asset_key, event.partition) == (ARCHIVE, "2026-09")
    assert event.metadata["completed_at"] == dg.MetadataValue.text(COMPLETED_AT.isoformat())
    assert event.metadata["leads/lake"] == dg.MetadataValue.text("ducklake")


def test_an_unfinished_snapshot_is_not_indexed(instance: dg.DagsterInstance) -> None:
    # Arrange
    archive = InMemoryRawArchive()
    archive.save("sodra", "2026-09", "monthly-2026.csv.zip", build_download())

    # Act
    result = tick(archive, instance)

    # Assert
    assert instance.get_dynamic_partitions(PARTITIONS.name) == []
    assert reported(result) == []


def test_only_what_changed_since_the_last_tick_is_reported(instance: dg.DagsterInstance) -> None:
    # Arrange
    archive = InMemoryRawArchive()
    archive.complete("sodra", "2026-08", {}, COMPLETED_AT)
    archive.complete("sodra", "2026-09", {}, COMPLETED_AT)
    first = tick(archive, instance)

    # Act
    quiet = tick(archive, instance, first.cursor)
    archive.complete("sodra", "2026-09", {}, COMPLETED_AT)
    redone = tick(archive, instance, quiet.cursor)

    # Assert
    assert reported(first) == ["2026-08", "2026-09"]
    assert reported(quiet) == []
    assert reported(redone) == ["2026-09"]
    assert instance.get_dynamic_partitions(PARTITIONS.name) == ["2026-08", "2026-09"]


def test_a_snapshot_gone_from_the_archive_loses_its_partition(
    instance: dg.DagsterInstance,
) -> None:
    # Arrange
    archive = InMemoryRawArchive()
    archive.complete("sodra", "2026-08", {}, COMPLETED_AT)
    archive.complete("sodra", "2026-09", {}, COMPLETED_AT)
    first = tick(archive, instance)

    # Act
    del archive.manifests[("sodra", "2026-08")]
    tick(archive, instance, first.cursor)

    # Assert
    assert instance.get_dynamic_partitions(PARTITIONS.name) == ["2026-09"]
