"""Both code locations built as Dagster loads them, from a copy of the dbt project: the parse
writes into the project's target/, which the copy keeps away from the working tree."""

import os
import shutil
import sysconfig
from collections.abc import Iterator
from unittest import mock

import dagster as dg
import pytest
from dagster._core.definitions.partitions.context import partition_loading_context
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from leads_dagster_orchestration.archive_index import ARCHIVE_INDEX
from leads_dagster_orchestration.automation import (
    BUILD_NEW_SNAPSHOTS,
    REBUILD_ON_CHANGE,
    TRANSFORM_AUTOMATION,
)
from leads_dagster_orchestration.entrypoints.definitions import pipeline_definitions
from leads_ingest.settings import PipelineMode
from leads_testing.config import build_config

LIVE, REPLAY = PipelineMode.LIVE, PipelineMode.REPLAY
FETCH = dg.AssetKey(["live", "fetch", "fetch_sodra"])
ARCHIVE = dg.AssetKey(["live", "archive", "archive_sodra"])
RAW = dg.AssetKey(["live", "raw", "raw_sodra__monthly"])
STAGING = dg.AssetKey(["live", "staging", "stg_sodra__monthly"])
REPLAY_ARCHIVE, REPLAY_RAW, REPLAY_STAGING = (
    dg.AssetKey(["replay", *key.path[1:]]) for key in (ARCHIVE, RAW, STAGING)
)
JAR_FETCH = dg.AssetKey(["live", "fetch", "fetch_jar"])
JAR_ARCHIVE = dg.AssetKey(["live", "archive", "archive_jar"])
JAR_RAW = {
    dg.AssetKey(["live", "raw", f"raw_jar__{table}"])
    for table in ("entities", "legal_forms", "statuses")
}
JAR_STAGING = dg.AssetKey(["live", "staging", "stg_jar__entities"])
REPLAY_JAR_ARCHIVE, REPLAY_JAR_STAGING = (
    dg.AssetKey(["replay", *key.path[1:]]) for key in (JAR_ARCHIVE, JAR_STAGING)
)
REPLAY_JAR_RAW = {dg.AssetKey(["replay", *key.path[1:]]) for key in JAR_RAW}


@pytest.fixture(scope="module")
def locations(
    tmp_path_factory: pytest.TempPathFactory, pytestconfig: pytest.Config
) -> Iterator[dict[PipelineMode, dg.Definitions]]:
    """Each mode's definitions. Both load in this one process here, so the second's dbt
    variables replace the first's, which is why Dagster runs them as two code locations."""
    project = tmp_path_factory.mktemp("transform") / "transform"
    shutil.copytree(
        pytestconfig.rootpath / "services" / "transform",
        project,
        ignore=shutil.ignore_patterns("target", "dbt_packages", "logs"),
    )
    config = build_config(
        {
            "transform.project_dir": str(project),
            "pipeline.replay_path": str(pytestconfig.rootpath / "replay"),
        }
    )
    environment = os.environ  # noqa: TID251 - parses the project as `dagster dev` would
    path = os.pathsep.join([sysconfig.get_path("scripts"), environment.get("PATH", "")])
    with mock.patch.dict(environment, {"DAGSTER_IS_DEV_CLI": "1", "PATH": path}):
        yield {mode: pipeline_definitions(mode, config) for mode in PipelineMode}


def job_assets(definitions: dg.Definitions, name: str) -> set[dg.AssetKey]:
    job = next(job for job in definitions.jobs or [] if job.name == name)
    assert isinstance(job, UnresolvedAssetJobDefinition)
    return set(job.selection.resolve(definitions.resolve_asset_graph()))


def test_each_sources_assets_are_a_group_within_their_lakes(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Act
    groups = {
        mode: {key: graph.get(key).group_name for key in graph.get_all_asset_keys()}
        for mode, graph in (
            (mode, definitions.resolve_asset_graph()) for mode, definitions in locations.items()
        )
    }

    # Assert
    assert groups == {
        LIVE: {
            FETCH: "live/sodra",
            ARCHIVE: "live/sodra",
            RAW: "live/sodra",
            STAGING: "live/sodra",
            JAR_FETCH: "live/jar",
            JAR_ARCHIVE: "live/jar",
            **dict.fromkeys(JAR_RAW, "live/jar"),
            JAR_STAGING: "live/jar",
        },
        REPLAY: {
            REPLAY_ARCHIVE: "replay/sodra",
            REPLAY_RAW: "replay/sodra",
            REPLAY_STAGING: "replay/sodra",
            REPLAY_JAR_ARCHIVE: "replay/jar",
            **dict.fromkeys(REPLAY_JAR_RAW, "replay/jar"),
            REPLAY_JAR_STAGING: "replay/jar",
        },
    }


def test_only_a_fetch_writes_the_archive_and_only_live_fetches(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Arrange
    live = locations[LIVE].resolve_asset_graph()
    replay = locations[REPLAY].resolve_asset_graph()

    # Act / Assert
    for fetch, archive, replay_archive in [
        (FETCH, ARCHIVE, REPLAY_ARCHIVE),
        (JAR_FETCH, JAR_ARCHIVE, REPLAY_JAR_ARCHIVE),
    ]:
        assert live.get(fetch).is_executable
        assert live.get(fetch).partitions_def is None
        assert not live.get(archive).is_executable
        assert live.get(archive).parent_keys == {fetch}
        assert not replay.get(replay_archive).is_executable


def partitions_name(definitions: dg.Definitions, key: dg.AssetKey) -> str | None:
    """The name of the asset's dynamic partitions, or None if it is unpartitioned."""
    partitions = definitions.resolve_asset_graph().get(key).partitions_def
    if partitions is None:
        return None
    assert isinstance(partitions, dg.DynamicPartitionsDefinition)
    return partitions.name


def test_raw_is_partitioned_by_its_sources_snapshots_and_staging_is_whole(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Arrange
    tables = {
        LIVE: ({RAW, *JAR_RAW}, {STAGING, JAR_STAGING}),
        REPLAY: ({REPLAY_RAW, *REPLAY_JAR_RAW}, {REPLAY_STAGING, REPLAY_JAR_STAGING}),
    }

    # Act
    partitions = {
        mode: (
            {partitions_name(locations[mode], key) for key in raws},
            {partitions_name(locations[mode], key) for key in stagings},
        )
        for mode, (raws, stagings) in tables.items()
    }

    # Assert
    assert partitions == {
        LIVE: ({"live_sodra_snapshots", "live_jar_snapshots"}, {None}),
        REPLAY: ({"replay_sodra_snapshots", "replay_jar_snapshots"}, {None}),
    }


def test_new_snapshots_and_changed_models_build_themselves(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Arrange
    definitions = locations[LIVE]
    graph = definitions.resolve_asset_graph()

    # Act
    raw = graph.get(RAW).automation_condition
    staging = graph.get(STAGING).automation_condition

    # Assert
    assert raw is not None
    assert raw.get_label() == BUILD_NEW_SNAPSHOTS
    assert staging is not None
    assert staging.get_label() == REBUILD_ON_CHANGE
    assert graph.get(FETCH).automation_condition is None
    for name in (ARCHIVE_INDEX, TRANSFORM_AUTOMATION):
        assert definitions.resolve_sensor_def(name).default_status is dg.DefaultSensorStatus.RUNNING


def requested_after(
    definitions: dg.Definitions, *, snapshot_before_first_evaluation: bool
) -> tuple[int, dict[dg.AssetKey, set[str]]]:
    """What the automation requests once the index has registered and reported snapshot 2026-09,
    either before the automation's first evaluation or between its first and second."""

    def register(instance: dg.DagsterInstance) -> None:
        instance.report_runless_asset_event(dg.AssetMaterialization(ARCHIVE, partition="2026-09"))
        instance.add_dynamic_partitions("live_sodra_snapshots", ["2026-09"])

    with dg.instance_for_test() as instance:
        if snapshot_before_first_evaluation:
            register(instance)
        first = dg.evaluate_automation_conditions(defs=definitions, instance=instance)
        result = first
        if not snapshot_before_first_evaluation:
            register(instance)
            result = dg.evaluate_automation_conditions(
                defs=definitions, instance=instance, cursor=first.cursor
            )
        with partition_loading_context(dynamic_partitions_store=instance):
            requested = {
                evaluation.key: evaluation.true_subset.expensively_compute_partition_keys()
                for evaluation in result.results
                if evaluation.true_subset.size
            }
    return first.total_requested if result is not first else 0, requested


def test_a_snapshot_the_index_reports_is_built_and_nothing_else(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Act
    first, requested = requested_after(locations[LIVE], snapshot_before_first_evaluation=False)

    # Assert
    assert first == 0
    assert requested == {RAW: {"2026-09"}}


def test_a_snapshot_registered_before_the_automation_started_is_built_too(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Act
    _, requested = requested_after(locations[LIVE], snapshot_before_first_evaluation=True)

    # Assert
    assert requested == {RAW: {"2026-09"}}


def test_live_fetches_on_its_schedule_and_rebuilds_without_fetching(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Arrange
    definitions = locations[LIVE]

    # Act
    schedules = [
        definitions.resolve_schedule_def(f"fetch_{source}_schedule") for source in ("sodra", "jar")
    ]

    # Assert
    assert job_assets(definitions, "fetch_sodra") == {FETCH}
    assert job_assets(definitions, "rebuild_sodra_from_archive") == {RAW, STAGING}
    assert job_assets(definitions, "fetch_jar") == {JAR_FETCH}
    assert job_assets(definitions, "rebuild_jar_from_archive") == {*JAR_RAW, JAR_STAGING}
    for schedule in schedules:
        assert (schedule.cron_schedule, schedule.execution_timezone) == (
            "0 6 21 * *",
            "Europe/Vilnius",
        )
        assert schedule.default_status is dg.DefaultScheduleStatus.RUNNING


def test_replay_only_rebuilds_from_its_sample(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Arrange
    definitions = locations[REPLAY]

    # Act
    jobs = {job.name for job in definitions.jobs or []}

    # Assert
    assert jobs == {"rebuild_sodra_from_archive", "rebuild_jar_from_archive"}
    assert job_assets(definitions, "rebuild_sodra_from_archive") == {REPLAY_RAW, REPLAY_STAGING}
    assert job_assets(definitions, "rebuild_jar_from_archive") == {
        *REPLAY_JAR_RAW,
        REPLAY_JAR_STAGING,
    }
    assert not definitions.schedules


def test_each_lakes_steps_run_in_its_own_pools(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Act
    pools = {
        mode: {definitions.resolve_assets_def(key).op.pool for key in keys}
        for mode, definitions, keys in [
            (LIVE, locations[LIVE], (FETCH, RAW, STAGING, JAR_FETCH, *JAR_RAW, JAR_STAGING)),
            (
                REPLAY,
                locations[REPLAY],
                (REPLAY_RAW, REPLAY_STAGING, *REPLAY_JAR_RAW, REPLAY_JAR_STAGING),
            ),
        ]
    }

    # Assert
    assert pools == {
        LIVE: {"live_fetch_sodra", "live_transform_sodra", "live_fetch_jar", "live_transform_jar"},
        REPLAY: {"replay_transform_sodra", "replay_transform_jar"},
    }
