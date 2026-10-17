from collections.abc import Callable, Mapping
from datetime import UTC, datetime

import dagster as dg

from leads_dagster_orchestration.assets.archive import build_fetch_asset, files_metadata
from leads_dagster_orchestration.snapshots import ArchivedSource, SnapshotCadence
from leads_ingest.ports.downloader import Download
from leads_testing.ingest import InMemoryRawArchive, build_download

CADENCE = SnapshotCadence(schedule="0 6 21 * *", key_format="%Y-%m", timezone="Europe/Vilnius")
LABELS = {"leads/mode": "live", "leads/lake": "ducklake"}
FILES = {
    "monthly-2025.csv.zip": build_download(b"2025", url="https://sodra.test/2025"),
    "monthly-2026.csv.zip": build_download(b"2026", url="https://sodra.test/2026"),
}


def saving(
    archive: InMemoryRawArchive, source: str = "sodra", *, fail_after: int | None = None
) -> Callable[[str], Mapping[str, Download]]:
    """A source's fetch: saves FILES into the archive as the snapshot asked for, or fails after
    saving `fail_after` of them."""

    def fetch(snapshot: str) -> Mapping[str, Download]:
        for index, (name, download) in enumerate(FILES.items()):
            if index == fail_after:
                raise ConnectionError("the source went away")
            archive.save(source, snapshot, name, download)
        return FILES

    return fetch


def materialize(
    archive: InMemoryRawArchive, fetch: Callable[[str], Mapping[str, Download]]
) -> tuple[dg.ExecuteInProcessResult, dg.DagsterInstance]:
    instance = dg.DagsterInstance.ephemeral()
    source = ArchivedSource(name="sodra", cadence=CADENCE, fetch=fetch)
    asset = build_fetch_asset(source, archive, LABELS)
    return dg.materialize([asset], instance=instance, raise_on_error=False), instance


def test_a_fetch_completes_the_current_periods_snapshot() -> None:
    # Arrange
    archive = InMemoryRawArchive()
    current = CADENCE.snapshot_at(datetime.now(UTC))

    # Act
    result, _ = materialize(archive, saving(archive))

    # Assert
    assert result.success
    assert list(archive.snapshots("sodra")) == [current]
    assert archive.manifest("sodra", current).files == files_metadata(FILES)


def test_a_fetch_records_its_snapshot_and_labels_its_run() -> None:
    # Arrange
    archive = InMemoryRawArchive()

    # Act
    result, instance = materialize(archive, saving(archive))

    # Assert
    [event] = result.asset_materializations_for_node("live__fetch__fetch_sodra")
    assert event.metadata["snapshot"] == dg.MetadataValue.text(
        CADENCE.snapshot_at(datetime.now(UTC))
    )
    assert event.metadata["leads/trigger"] == dg.MetadataValue.text("fetch")
    run = instance.get_run_by_id(result.run_id)
    assert run is not None
    assert {**LABELS, "leads/trigger": "fetch"}.items() <= run.tags.items()


def test_a_fetch_that_fails_midway_leaves_no_complete_snapshot() -> None:
    # Arrange
    archive = InMemoryRawArchive()

    # Act
    result, _ = materialize(archive, saving(archive, fail_after=1))

    # Assert
    assert not result.success
    assert len(archive.downloads) == 1
    assert archive.snapshots("sodra") == {}


def test_a_source_with_history_fetches_the_periods_asked_for_in_order_in_one_run() -> None:
    # Arrange
    archive = InMemoryRawArchive()
    fetched: list[str] = []
    save = saving(archive, "vmi")

    def fetch(snapshot: str) -> Mapping[str, Download]:
        fetched.append(snapshot)
        return save(snapshot)

    daily = SnapshotCadence(schedule="0 6 * * *", key_format="%Y-%m-%d", timezone="Europe/Vilnius")
    source = ArchivedSource(name="vmi", cadence=daily, fetch=fetch, history_start="2026-09-01")
    asset = build_fetch_asset(source, archive, LABELS)

    # Act
    result = dg.materialize(
        [asset],
        instance=dg.DagsterInstance.ephemeral(),
        tags={
            "dagster/asset_partition_range_start": "2026-09-14",
            "dagster/asset_partition_range_end": "2026-09-16",
        },
    )

    # Assert
    assert result.success
    assert fetched == ["2026-09-14", "2026-09-15", "2026-09-16"]
    assert sorted(archive.snapshots("vmi")) == fetched
    event = result.asset_materializations_for_node("live__fetch__fetch_vmi")[0]
    assert event.metadata["snapshot"] == dg.MetadataValue.text("2026-09-14 to 2026-09-16")
