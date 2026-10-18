import os
import shutil
import sysconfig
from collections.abc import Iterator
from datetime import datetime
from unittest import mock
from zoneinfo import ZoneInfo

import dagster as dg
import pytest
from dagster._core.definitions.partitions.context import partition_loading_context
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from owl_dagster_orchestration.archive.assets import WITHIN_BUDGET
from owl_dagster_orchestration.archive.index import ARCHIVE_INDEX
from owl_dagster_orchestration.automation import (
    BUILD_NEW_SNAPSHOTS,
    BUILD_STATIC_DATA,
    PLAN_ON_CHANGE,
    REBUILD_ON_CHANGE,
    TRANSFORM_AUTOMATION,
)
from owl_dagster_orchestration.entrypoints.definitions import pipeline_definitions
from owl_dagster_orchestration.lineage.sensor import LINEAGE
from owl_ingest.settings import PipelineMode
from owl_testing.config import build_config

LIVE, REPLAY = PipelineMode.LIVE, PipelineMode.REPLAY
FETCH = dg.AssetKey(["live", "fetch", "fetch_sodra"])
ARCHIVE = dg.AssetKey(["live", "archive", "archive_sodra"])
RAW = dg.AssetKey(["live", "raw", "raw_sodra__monthly"])
STAGING = dg.AssetKey(["live", "staging", "stg_sodra__monthly"])
# Each source's dbt models, by layer. The VMI sources and Google's are staged in a folder of
# their dataset's (COMBINED).
MODELS = {
    "sodra": {"raw": {"raw_sodra__monthly"}, "staging": {"stg_sodra__monthly"}},
    "jar": {
        "raw": {"raw_jar__entities", "raw_jar__legal_forms", "raw_jar__statuses", "raw_jar__seats"},
        "staging": {"stg_jar__entities"},
    },
    "address_register": {
        "raw": {
            f"raw_address_register__{table}"
            for table in ("buildings", "premises", "streets", "settlements", "municipalities")
        },
        "staging": {"stg_address_register__addresses"},
    },
    "vmi_daily_changes": {"raw": {"raw_vmi_daily_changes__changes"}, "staging": set()},
    "vmi_monthly_register": {
        "raw": {
            "raw_vmi_monthly_register__register",
            "raw_vmi_monthly_register__activities",
            "raw_vmi_monthly_register__last_change",
        },
        "staging": set(),
    },
    "google_places": {"raw": {"raw_google_places__places"}, "staging": set()},
}
COMBINED = {
    "vmi": {"stg_vmi__changes", "stg_vmi__current_register"},
    "google": {"stg_google__listings"},
}
# Sources drawn in their dataset's group rather than one of their own.
GROUPS = {
    "vmi_daily_changes": "vmi/daily_changes",
    "vmi_monthly_register": "vmi/monthly_register",
    "google_places": "google/places",
}
MONTHLY, DAILY = "0 6 21 * *", "0 6 * * *"
SCHEDULES = {
    "sodra": MONTHLY,
    "jar": MONTHLY,
    "address_register": MONTHLY,
    "vmi_daily_changes": DAILY,
    "vmi_monthly_register": MONTHLY,
    "google_places": MONTHLY,
}
# Sources whose fetch takes any period of their history.
WITH_HISTORY = {"vmi_daily_changes"}
# The sources billed per request: their schedules start stopped, and each has a plan.
PAID = {"google_places"}
# Models of no source, in one step: the seeds, and where Google is searched.
SHARED = {
    "seeds": {"municipalities", "activity_categories", "google_search_scope"},
    "intermediate": {"int_google_search_areas"},
}
SEARCH_AREAS = dg.AssetKey(["live", "intermediate", "int_google_search_areas"])


def models(lake: PipelineMode, source: str, layer: str) -> set[dg.AssetKey]:
    return {dg.AssetKey([lake.value, layer, model]) for model in MODELS[source][layer]}


def combined(lake: PipelineMode, folder: str) -> set[dg.AssetKey]:
    return {dg.AssetKey([lake.value, "staging", model]) for model in COMBINED[folder]}


def shared(lake: PipelineMode, *layers: str) -> set[dg.AssetKey]:
    """The lake's models of no source, in the layers given (all, by default)."""
    return {
        dg.AssetKey([lake.value, layer, model])
        for layer, names in SHARED.items()
        if not layers or layer in layers
        for model in names
    }


def plan_of(source: str) -> dg.AssetKey:
    return dg.AssetKey(["live", "plan", f"plan_{source}"])


def fetch_of(source: str) -> dg.AssetKey:
    return dg.AssetKey(["live", "fetch", f"fetch_{source}"])


def archive_of(lake: PipelineMode, source: str) -> dg.AssetKey:
    return dg.AssetKey([lake.value, "archive", f"archive_{source}"])


@pytest.fixture(scope="module")
def locations(
    tmp_path_factory: pytest.TempPathFactory, pytestconfig: pytest.Config
) -> Iterator[dict[PipelineMode, dg.Definitions]]:
    """From a copy of the dbt project, as the parse writes into its target/. Both lakes load in
    this one process, so the second's dbt variables replace the first's."""
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
    # Arrange
    expected = {
        lake: {
            **{
                key: f"{lake.value}/{GROUPS.get(source, source)}"
                for source in MODELS
                for key in {
                    archive_of(lake, source),
                    *models(lake, source, "raw"),
                    *models(lake, source, "staging"),
                    *({fetch_of(source)} if lake is LIVE else set()),
                }
            },
            **{
                key: f"{lake.value}/{folder}"
                for folder in COMBINED
                for key in combined(lake, folder)
            },
            # In models/intermediate/google/: drawn beside Google's source, whose plan reads it.
            **dict.fromkeys(shared(lake, "intermediate"), f"{lake.value}/google"),
            **dict.fromkeys(shared(lake, "seeds"), f"{lake.value}/static_data"),
            **(
                {plan_of(source): f"live/{GROUPS.get(source, source)}" for source in PAID}
                if lake is LIVE
                else {}
            ),
        }
        for lake in PipelineMode
    }

    # Act
    groups = {
        mode: {key: graph.get(key).group_name for key in graph.get_all_asset_keys()}
        for mode, graph in (
            (mode, definitions.resolve_asset_graph()) for mode, definitions in locations.items()
        )
    }

    # Assert
    assert groups == expected


def test_only_a_fetch_writes_the_archive_and_only_live_fetches(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Arrange
    live = locations[LIVE].resolve_asset_graph()
    replay = locations[REPLAY].resolve_asset_graph()

    # Act / Assert
    for source in MODELS:
        assert live.get(fetch_of(source)).is_executable
        assert not live.get(archive_of(LIVE, source)).is_executable
        assert live.get(archive_of(LIVE, source)).parent_keys == {fetch_of(source)}
        assert not replay.get(archive_of(REPLAY, source)).is_executable


def test_a_fetch_takes_a_period_only_where_the_source_keeps_its_history(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Arrange
    graph = locations[LIVE].resolve_asset_graph()

    # Act
    fetches = {source: graph.get(fetch_of(source)).partitions_def for source in MODELS}

    # Assert
    assert {source for source, periods in fetches.items() if periods is not None} == WITH_HISTORY
    vmi = fetches["vmi_daily_changes"]
    assert isinstance(vmi, dg.TimeWindowPartitionsDefinition)
    assert (vmi.cron_schedule, vmi.timezone, vmi.start.date().isoformat()) == (
        DAILY,
        "Europe/Vilnius",
        "2026-09-01",
    )


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
    # Act
    partitions = {
        (lake, source, layer): {partitions_name(locations[lake], key) for key in keys}
        for lake in PipelineMode
        for source in MODELS
        for layer in ("raw", "staging")
        if (keys := models(lake, source, layer))
    }

    # Assert
    assert partitions == {
        (lake, source, layer): {f"{lake.value}_{source}_snapshots" if layer == "raw" else None}
        for lake in PipelineMode
        for source in MODELS
        for layer in ("raw", "staging")
        if MODELS[source][layer]
    }
    assert {
        partitions_name(locations[lake], key)
        for lake in PipelineMode
        for folder in COMBINED
        for key in combined(lake, folder)
    } == {None}


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
    for seed in shared(LIVE, "seeds"):
        condition = graph.get(seed).automation_condition
        assert condition is not None
        assert condition.get_label() == BUILD_STATIC_DATA
    assert graph.get(FETCH).automation_condition is None
    for name in (ARCHIVE_INDEX, TRANSFORM_AUTOMATION, LINEAGE):
        assert definitions.resolve_sensor_def(name).default_status is dg.DefaultSensorStatus.RUNNING


def register_snapshot(
    instance: dg.DagsterInstance, source: str, snapshot: str, *, raw_built: bool = False
) -> None:
    """As the archive index registers and reports a complete snapshot; with `raw_built`, as if
    raw had then built it."""
    instance.add_dynamic_partitions(f"live_{source}_snapshots", [snapshot])
    built = models(LIVE, source, "raw") if raw_built else set()
    for key in {archive_of(LIVE, source), *built}:
        instance.report_runless_asset_event(dg.AssetMaterialization(key, partition=snapshot))


def requested_in(
    result: dg.EvaluateAutomationConditionsResult, instance: dg.DagsterInstance
) -> dict[dg.AssetKey, set[str | None]]:
    with partition_loading_context(dynamic_partitions_store=instance):
        return {
            evaluation.key: evaluation.true_subset.expensively_compute_partition_keys()
            if evaluation.true_subset.is_partitioned
            else {None}
            for evaluation in result.results
            if evaluation.true_subset.size
        }


def requested_after(
    definitions: dg.Definitions, *, snapshot_before_first_evaluation: bool
) -> tuple[dict[dg.AssetKey, set[str | None]], dict[dg.AssetKey, set[str | None]]]:
    """What the automation requests at its first evaluation and once the index has registered
    and reported snapshot 2026-09, either before the first evaluation or between it and the
    second."""
    with dg.instance_for_test() as instance:
        if snapshot_before_first_evaluation:
            register_snapshot(instance, "sodra", "2026-09")
        first = dg.evaluate_automation_conditions(defs=definitions, instance=instance)
        result = first
        if not snapshot_before_first_evaluation:
            register_snapshot(instance, "sodra", "2026-09")
            result = dg.evaluate_automation_conditions(
                defs=definitions, instance=instance, cursor=first.cursor
            )
        return requested_in(first, instance), requested_in(result, instance)


def test_a_snapshot_the_index_reports_is_built_and_nothing_else(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Act
    first, requested = requested_after(locations[LIVE], snapshot_before_first_evaluation=False)

    # Assert
    assert first == {key: {None} for key in shared(LIVE, "seeds")}
    assert requested == {RAW: {"2026-09"}}


def test_a_snapshot_registered_before_the_automation_started_is_built_too(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Act
    _, requested = requested_after(locations[LIVE], snapshot_before_first_evaluation=True)

    # Assert
    assert requested == {RAW: {"2026-09"}, **{key: {None} for key in shared(LIVE, "seeds")}}


def test_a_model_of_two_snapshot_sources_waits_until_both_have_one(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    """VMI's staging reads both VMI sources' raw. Before the guard, the register's first
    snapshot started it while the daily changes had none, and it failed on their missing
    table: `any_deps_missing()` sees no partition missing where none exists."""
    # Arrange
    definitions = locations[LIVE]
    staging = combined(LIVE, "vmi")

    # Act
    with dg.instance_for_test() as instance:
        first = dg.evaluate_automation_conditions(defs=definitions, instance=instance)
        register_snapshot(instance, "vmi_monthly_register", "2026-09", raw_built=True)
        one = dg.evaluate_automation_conditions(
            defs=definitions, instance=instance, cursor=first.cursor
        )
        register_snapshot(instance, "vmi_daily_changes", "2026-09-01", raw_built=True)
        both = dg.evaluate_automation_conditions(
            defs=definitions, instance=instance, cursor=one.cursor
        )
        with_one, with_both = requested_in(one, instance), requested_in(both, instance)

    # Assert
    assert not staging & with_one.keys()
    assert staging <= with_both.keys()


def rebuilt(lake: PipelineMode, source: str) -> set[dg.AssetKey]:
    """The source's models, its dataset's combined staging, and for Sodra the search areas, but
    never Google's plan or fetch."""
    built = models(lake, source, "raw") | models(lake, source, "staging")
    if source in GROUPS:
        built |= combined(lake, GROUPS[source].split("/")[0])
    if source == "sodra":
        built |= {dg.AssetKey([lake.value, "intermediate", "int_google_search_areas"])}
    return built


def test_live_fetches_on_its_schedule_and_rebuilds_without_fetching(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Arrange
    definitions = locations[LIVE]

    # Act
    schedules = {
        source: definitions.resolve_schedule_def(f"fetch_{source}_schedule") for source in MODELS
    }

    # Assert
    for source, schedule in schedules.items():
        assert job_assets(definitions, f"fetch_{source}") == {
            fetch_of(source),
            *({plan_of(source)} if source in PAID else set()),
        }
        assert job_assets(definitions, f"rebuild_{source}_from_archive") == rebuilt(LIVE, source)
        assert (schedule.cron_schedule, schedule.execution_timezone) == (
            SCHEDULES[source],
            "Europe/Vilnius",
        )
        assert schedule.default_status is (
            dg.DefaultScheduleStatus.STOPPED if source in PAID else dg.DefaultScheduleStatus.RUNNING
        )


def test_only_googles_fetch_may_run_longer_than_run_monitorings_hour(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Arrange
    definitions = locations[LIVE]

    # Act
    runtimes = {
        source: definitions.resolve_job_def(f"fetch_{source}").tags.get("dagster/max_runtime")
        for source in MODELS
    }

    # Assert
    assert {source: runtime for source, runtime in runtimes.items() if runtime} == {
        "google_places": "10800"
    }


def test_a_source_with_history_fetches_the_period_its_tick_begins(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Arrange
    schedule = locations[LIVE].resolve_schedule_def("fetch_vmi_daily_changes_schedule")
    tick = datetime(2026, 10, 17, 6, 0, tzinfo=ZoneInfo("Europe/Vilnius"))

    # Act
    with dg.instance_for_test() as instance:
        result = schedule.evaluate_tick(
            dg.build_schedule_context(instance=instance, scheduled_execution_time=tick)
        )

    # Assert
    assert [request.partition_key for request in result.run_requests or []] == ["2026-10-17"]


def test_replay_only_rebuilds_from_its_sample(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Arrange
    definitions = locations[REPLAY]

    # Act
    jobs = {job.name for job in definitions.jobs or []}

    # Assert
    assert jobs == {f"rebuild_{source}_from_archive" for source in MODELS}
    for source in MODELS:
        assert job_assets(definitions, f"rebuild_{source}_from_archive") == rebuilt(REPLAY, source)
    assert not definitions.schedules


def test_each_lakes_steps_run_in_its_own_pools(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Act
    pools = {
        (lake, source): {
            locations[lake].resolve_assets_def(key).op.pool
            for key in {
                *models(lake, source, "raw"),
                *models(lake, source, "staging"),
                *({fetch_of(source)} if lake is LIVE else set()),
            }
        }
        for lake in PipelineMode
        for source in MODELS
    }

    # Assert
    assert pools == {
        (lake, source): {
            f"{lake.value}_transform_{source}",
            *({f"live_fetch_{source}"} if lake is LIVE else set()),
        }
        for lake in PipelineMode
        for source in MODELS
    }
    for lake in PipelineMode:
        for folder in COMBINED:
            for key in combined(lake, folder):
                pool = locations[lake].resolve_assets_def(key).op.pool
                assert pool == f"{lake.value}_transform_{folder}"
        for key in shared(lake):
            assert locations[lake].resolve_assets_def(key).op.pool == f"{lake.value}_transform"
    for source in PAID:
        pool = locations[LIVE].resolve_assets_def(plan_of(source)).op.pool
        assert pool == f"live_fetch_{source}"


def test_googles_plan_reads_where_it_is_searched_and_its_fetch_pays_for_it(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Arrange
    definitions = locations[LIVE]
    graph = definitions.resolve_asset_graph()

    # Act
    fetch, plan = graph.get(fetch_of("google_places")), graph.get(plan_of("google_places"))
    step = definitions.resolve_assets_def(plan_of("google_places"))

    # Assert: one step makes both, the fetch only after the plan, which reads the search areas.
    assert plan.parent_keys == {SEARCH_AREAS}
    assert fetch.parent_keys == {plan_of("google_places")}
    assert graph.get(SEARCH_AREAS).parent_keys == {
        dg.AssetKey(["live", "staging", "stg_sodra__monthly"]),
        *(dg.AssetKey(["live", "seeds", seed]) for seed in SHARED["seeds"]),
    }
    assert step is definitions.resolve_assets_def(fetch_of("google_places"))
    assert step.can_subset
    [budget] = step.check_specs
    assert (budget.key, budget.blocking) == (
        dg.AssetCheckKey(plan_of("google_places"), WITHIN_BUDGET),
        True,
    )
    assert fetch.automation_condition is None
    assert plan.automation_condition is not None
    assert plan.automation_condition.get_label() == PLAN_ON_CHANGE
    sensor = definitions.resolve_sensor_def("plan_google_places_automation")
    assert isinstance(sensor, dg.AutomationConditionSensorDefinition)
    assert sensor.asset_selection.resolve(graph) == {plan_of("google_places")}
    assert sensor.run_tags == {"dagster/max_runtime": "10800"}
    assert sensor.default_status is dg.DefaultSensorStatus.RUNNING
    dbt = definitions.resolve_sensor_def(TRANSFORM_AUTOMATION)
    assert plan_of("google_places") not in dbt.asset_selection.resolve(graph)


def test_replay_neither_fetches_nor_plans_but_knows_where_google_is_searched(
    locations: dict[PipelineMode, dg.Definitions],
) -> None:
    # Arrange
    graph = locations[REPLAY].resolve_asset_graph()

    # Act
    keys = graph.get_all_asset_keys()

    # Assert
    assert dg.AssetKey(["replay", "intermediate", "int_google_search_areas"]) in keys
    assert not any(key.path[1] in {"fetch", "plan"} for key in keys)
