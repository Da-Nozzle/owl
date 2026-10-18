from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from unittest import mock

import dagster as dg

from owl_dagster_orchestration.archive import assets as archive_assets
from owl_dagster_orchestration.archive.assets import (
    WITHIN_BUDGET,
    build_fetch_asset,
    build_planned_fetch_asset,
)
from owl_dagster_orchestration.archive.metadata import files_metadata
from owl_dagster_orchestration.snapshots import ArchivedSource, FetchPlan, SnapshotCadence
from owl_ingest.ports.downloader import Download
from owl_testing.ingest import InMemoryRawArchive, build_download

CADENCE = SnapshotCadence(schedule="0 6 21 * *", key_format="%Y-%m", timezone="Europe/Vilnius")
LABELS = {"owl/mode": "live", "owl/lake": "ducklake"}
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
    assert event.metadata["owl/trigger"] == dg.MetadataValue.text("fetch")
    run = instance.get_run_by_id(result.run_id)
    assert run is not None
    assert {**LABELS, "owl/trigger": "fetch"}.items() <= run.tags.items()


def test_a_fetch_of_many_files_records_how_many_and_their_size() -> None:
    # Arrange
    archive = InMemoryRawArchive()
    pages = {f"places-kavine-r-{index}.json": build_download(b"{}") for index in range(101)}

    def fetch(snapshot: str) -> Mapping[str, Download]:
        for name, download in pages.items():
            archive.save("sodra", snapshot, name, download)
        return pages

    # Act
    result, _ = materialize(archive, fetch)

    # Assert
    [event] = result.asset_materializations_for_node("live__fetch__fetch_sodra")
    files = event.metadata["files"]
    assert isinstance(files, dg.JsonMetadataValue)
    assert files.data == {
        "file_count": 101,
        "size": 202,
        "listed_in": "the snapshot's manifest, <archive>/<source>/_snapshots/<snapshot>.json",
    }
    current = CADENCE.snapshot_at(datetime.now(UTC))
    assert len(archive.manifest("sodra", current).files) == 101


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


PLAN = dg.AssetKey(["live", "plan", "plan_google"])
FETCH = dg.AssetKey(["live", "fetch", "fetch_google"])
BUDGET = dg.AssetCheckKey(PLAN, WITHIN_BUDGET)


def planned(
    archive: InMemoryRawArchive, paid_requests: int
) -> tuple[ArchivedSource, list[str], list[str]]:
    """A paid source whose plan pays for `paid_requests` of the most 1,500, by saving FILES; and
    the plans made and the snapshots paid for, as they happen."""
    plans: list[str] = []
    paid: list[str] = []

    def plan() -> FetchPlan:
        made = f"plan {len(plans) + 1}"
        plans.append(made)

        def pay(snapshot: str) -> Mapping[str, Download]:
            paid.append(f"{made} as {snapshot}")
            return saving(archive, "google")(snapshot)

        return FetchPlan(
            paid_requests=paid_requests,
            max_paid_requests=1_500,
            summary=f"The next fetch pays for {paid_requests:,} requests.",
            parts=[
                {"municipality": "Vilniaus miesto sav.", "mode": "fetch", "paid requests": 1_200},
                {"municipality": "Kauno miesto sav.", "mode": "fetch", "paid requests": 400},
            ],
            pay=pay,
        )

    return ArchivedSource(name="google", cadence=CADENCE, plan=plan), plans, paid


def materialize_planned(
    source: ArchivedSource, archive: InMemoryRawArchive, *keys: dg.AssetKey
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [build_planned_fetch_asset(source, archive, LABELS)],
        selection=list(keys),
        instance=dg.DagsterInstance.ephemeral(),
        raise_on_error=False,
    )


def test_a_plan_alone_shows_what_the_next_fetch_would_pay_for_and_pays_nothing() -> None:
    # Arrange
    archive = InMemoryRawArchive()
    source, plans, paid = planned(archive, paid_requests=1_200)

    # Act
    result = materialize_planned(source, archive, PLAN)

    # Assert
    assert result.success
    [event] = result.get_asset_materialization_events()
    assert event.asset_key == PLAN
    metadata = event.materialization.metadata
    assert metadata["paid_requests"] == dg.MetadataValue.int(1_200)
    assert metadata["parts"] == dg.MetadataValue.md(
        "| municipality | mode | paid requests |\n"
        "|---|---|---|\n"
        "| Vilniaus miesto sav. | fetch | 1200 |\n"
        "| Kauno miesto sav. | fetch | 400 |"
    )
    [check] = result.get_asset_check_evaluations()
    assert (check.asset_check_key, check.passed) == (BUDGET, True)
    assert (plans, paid, archive.downloads) == (["plan 1"], [], {})


def test_a_fetch_pays_for_the_plan_made_in_its_own_step() -> None:
    # Arrange
    archive = InMemoryRawArchive()
    source, plans, paid = planned(archive, paid_requests=1_200)

    # Act
    with mock.patch(f"{archive_assets.__name__}.datetime", wraps=datetime) as clock:
        clock.now.return_value = datetime(2026, 10, 22, tzinfo=UTC)
        result = materialize_planned(source, archive, PLAN, FETCH)

    # Assert
    assert result.success
    assert [event.asset_key for event in result.get_asset_materialization_events()] == [PLAN, FETCH]
    assert plans == ["plan 1"]
    assert paid == ["plan 1 as 2026-10"]
    assert set(archive.snapshots("google")) == {"2026-10"}


def test_a_plan_over_the_budget_fails_its_check_and_its_fetch_pays_nothing() -> None:
    # Arrange
    archive = InMemoryRawArchive()
    source, _, paid = planned(archive, paid_requests=1_600)

    # Act
    result = materialize_planned(source, archive, PLAN, FETCH)

    # Assert: the plan and its failed check are recorded; the fetch is not.
    assert not result.success
    assert [event.asset_key for event in result.get_asset_materialization_events()] == [PLAN]
    [check] = result.get_asset_check_evaluations()
    assert (check.asset_check_key, check.passed) == (BUDGET, False)
    assert (paid, archive.downloads) == ([], {})


def test_a_fetch_selected_alone_plans_all_the_same_without_recording_the_plan() -> None:
    # Arrange
    archive = InMemoryRawArchive()
    source, plans, paid = planned(archive, paid_requests=1_200)

    # Act
    result = materialize_planned(source, archive, FETCH)

    # Assert
    assert result.success
    assert [event.asset_key for event in result.get_asset_materialization_events()] == [FETCH]
    assert result.get_asset_check_evaluations() == []
    assert (len(plans), len(paid)) == (1, 1)
