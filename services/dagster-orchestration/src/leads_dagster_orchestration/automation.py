from collections.abc import Sequence

import dagster as dg

from leads_dagster_orchestration.run_tags import MAX_RUNTIME_TAG
from leads_dagster_orchestration.snapshots import ArchivedSource

BUILD_NEW_SNAPSHOTS = "build_new_snapshots"
REBUILD_ON_CHANGE = "rebuild_on_change"
PLAN_ON_CHANGE = "plan_on_change"
TRANSFORM_AUTOMATION = "transform_automation"


def build_new_snapshots() -> dg.AutomationCondition:
    """For raw: `eager()`, plus the snapshots already missing at the first evaluation, which
    `eager()` leaves. A SQL change is not acted on: rebuilding every snapshot is a decision."""
    automation = dg.AutomationCondition
    catch_up = (automation.initial_evaluation() & automation.missing()).with_label("catch_up")
    return eager_or(catch_up).with_label(BUILD_NEW_SNAPSHOTS)


def rebuild_on_change() -> dg.AutomationCondition:
    """For the models rebuilt whole: `eager()`, plus a change to the model's SQL (dagster-dbt's
    code version). No catch-up: on a lake with no snapshot yet it would build from nothing."""
    changed = dg.AutomationCondition.code_version_changed().since_last_handled()
    return eager_or(changed).with_label(REBUILD_ON_CHANGE)


def plan_on_change() -> dg.AutomationCondition:
    """For a paid source's plan, which is free: `eager()`, plus raw's catch-up. The fetch, which
    pays, has no condition."""
    automation = dg.AutomationCondition
    catch_up = (automation.initial_evaluation() & automation.missing()).with_label("catch_up")
    return eager_or(catch_up).with_label(PLAN_ON_CHANGE)


def eager_or(also: dg.AutomationCondition) -> dg.AutomationCondition:
    """Dagster's `eager()`, true also when `also` is, under the same guards."""
    automation = dg.AutomationCondition
    return (
        ((automation.newly_missing() | automation.any_deps_updated()).since_last_handled() | also)
        & ~automation.any_deps_missing()
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
