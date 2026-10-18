from collections.abc import Sequence

import dagster as dg

from owl_dagster_orchestration.run_tags import MAX_RUNTIME_TAG
from owl_dagster_orchestration.snapshots import ArchivedSource

BUILD_NEW_SNAPSHOTS = "build_new_snapshots"
REBUILD_ON_CHANGE = "rebuild_on_change"
BUILD_STATIC_DATA = "build_static_data"
PLAN_ON_CHANGE = "plan_on_change"
DEPS_HAVE_DATA = "deps_have_data"
TRANSFORM_AUTOMATION = "transform_automation"


def build_new_snapshots() -> dg.AutomationCondition:
    """For raw: `eager()`, plus the snapshots already missing at the first evaluation, which
    `eager()` leaves. A SQL change is not acted on: rebuilding every snapshot is a decision."""
    return eager_or(catch_up()).with_label(BUILD_NEW_SNAPSHOTS)


def rebuild_on_change() -> dg.AutomationCondition:
    """For the models rebuilt whole: `eager()`, plus a change to the model's SQL (dagster-dbt's
    code version). No catch-up: on a lake with no snapshot yet it would build from nothing."""
    return eager_or(code_changed()).with_label(REBUILD_ON_CHANGE)


def build_static_data() -> dg.AutomationCondition:
    """For seeds: `rebuild_on_change()` plus raw's catch-up. A seed reads nothing, so it is
    missing from the first evaluation on, which `eager()` leaves, and what reads it waits."""
    return eager_or(code_changed() | catch_up()).with_label(BUILD_STATIC_DATA)


def plan_on_change() -> dg.AutomationCondition:
    """For a paid source's plan, which is free: `eager()`, plus raw's catch-up. The fetch, which
    pays, has no condition."""
    return eager_or(catch_up()).with_label(PLAN_ON_CHANGE)


def catch_up() -> dg.AutomationCondition:
    """Missing at the asset's first evaluation ever, not at each reload: once, so a build that
    fails is not retried every tick."""
    automation = dg.AutomationCondition
    return (automation.initial_evaluation() & automation.missing()).with_label("catch_up")


def code_changed() -> dg.AutomationCondition:
    return dg.AutomationCondition.code_version_changed().since_last_handled()


def eager_or(also: dg.AutomationCondition) -> dg.AutomationCondition:
    """Dagster's `eager()`, true also when `also` is, under the same guards and one more:
    `any_deps_missing()` holds back only for a parent's partitions that exist, so a model would
    build while a parent partitioned by snapshot had none yet, and fail on its missing table.
    A parent requested on the same tick counts as built, as in `any_deps_missing()`, so a chain
    of missing models is still requested at once."""
    automation = dg.AutomationCondition
    deps_have_data = automation.all_deps_match(
        ~automation.missing() | automation.will_be_requested()
    ).with_label(DEPS_HAVE_DATA)
    return (
        ((automation.newly_missing() | automation.any_deps_updated()).since_last_handled() | also)
        & ~automation.any_deps_missing()
        & deps_have_data
        & ~automation.any_deps_in_progress()
        & ~automation.in_progress()
    )


def transform_automation_sensor(
    assets: Sequence[dg.AssetsDefinition],
) -> dg.AutomationConditionSensorDefinition:
    """Dagster's default automation sensor starts stopped, which would leave new snapshots
    unbuilt."""
    return dg.AutomationConditionSensorDefinition(
        TRANSFORM_AUTOMATION,
        target=dg.AssetSelection.assets(*assets),
        default_status=dg.DefaultSensorStatus.RUNNING,
    )


def plan_automation_sensor(
    source: ArchivedSource, plan: dg.AssetKey
) -> dg.AutomationConditionSensorDefinition:
    """A sensor of its own, as the plan's runs need the fetch's longer runtime, which the dbt
    runs must not have."""
    runtime = source.fetch_max_runtime_seconds
    return dg.AutomationConditionSensorDefinition(
        plan_automation_name(source.name),
        target=dg.AssetSelection.assets(plan),
        run_tags={MAX_RUNTIME_TAG: str(runtime)} if runtime else None,
        default_status=dg.DefaultSensorStatus.RUNNING,
    )


def plan_automation_name(source: str) -> str:
    return f"plan_{source}_automation"
