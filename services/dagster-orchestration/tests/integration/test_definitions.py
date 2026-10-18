"""Both code locations built as Dagster loads them, from a copy of the dbt project: the parse
writes into the project's target/, which the copy keeps away from the working tree."""

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

from leads_dagster_orchestration.archive_index import ARCHIVE_INDEX
from leads_dagster_orchestration.assets.archive import WITHIN_BUDGET
from leads_dagster_orchestration.automation import (
    BUILD_NEW_SNAPSHOTS,
    PLAN_ON_CHANGE,
    REBUILD_ON_CHANGE,
    TRANSFORM_AUTOMATION,
)
from leads_dagster_orchestration.entrypoints.definitions import pipeline_definitions
from leads_dagster_orchestration.lineage import LINEAGE
from leads_ingest.settings import PipelineMode
from leads_testing.config import build_config

LIVE, REPLAY = PipelineMode.LIVE, PipelineMode.REPLAY
FETCH = dg.AssetKey(["live", "fetch", "fetch_sodra"])
ARCHIVE = dg.AssetKey(["live", "archive", "archive_sodra"])
RAW = dg.AssetKey(["live", "raw", "raw_sodra__monthly"])
STAGING = dg.AssetKey(["live", "staging", "stg_sodra__monthly"])
# Each source's dbt models, by layer. The VMI register's two sources have no staging of their
# own: the staging models that combine them are in a folder of the dataset's, `vmi` (COMBINED).
# Google's Places API source has none either: its listings are staged in `google`, which a second
# backend would feed too.
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
# Where a source's assets are drawn, if not in a group of its own name: in the group of the
# folder its staging models are in (COMBINED).
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
# The sources whose history the source keeps, so that any day of it can be fetched.
WITH_HISTORY = {"vmi_daily_changes"}
# The sources billed per request: their schedules start stopped, and each has a plan.
PAID = {"google_places"}
# Models of no source: seeds, drawn in the lake's static data group, and the intermediate model
# that says where Google is searched, which reads Sodra's staging, in the lake's own group. They
# are one step, `transform`.
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
            **dict.fromkeys(shared(lake, "intermediate"), lake.value),
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
    assert graph.get(FETCH).automation_condition is None
    for name in (ARCHIVE_INDEX, TRANSFORM_AUTOMATION, LINEAGE):
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


def rebuilt(lake: PipelineMode, source: str) -> set[dg.AssetKey]:
    """What a rebuild of the source's archive builds: its models, and for a source drawn in a
    dataset's group (either VMI source, Google's) the staging models in the dataset's folder.
    Sodra's is followed by the model of where Google is searched, but never by Google's plan,
    which reads it, or its fetch."""
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
