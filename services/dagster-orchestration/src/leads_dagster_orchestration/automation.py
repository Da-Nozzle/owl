from collections.abc import Sequence

import dagster as dg

BUILD_NEW_SNAPSHOTS = "build_new_snapshots"
REBUILD_ON_CHANGE = "rebuild_on_change"
TRANSFORM_AUTOMATION = "transform_automation"


def build_new_snapshots() -> dg.AutomationCondition:
    """For raw, partitioned by snapshot: `eager()`, so a partition the archive index adds (newly
    missing) is built, and one it reports again (completed again, an updated dependency) is
    rebuilt. Every partition has a snapshot behind it, so nothing is missing for good. A change to
    raw's SQL is not acted on: rebuilding every snapshot is a decision
    (rebuild_<source>_from_archive).

    Plus the snapshots already missing when the condition is first evaluated, which `eager()`
    leaves: it builds only what goes missing after that, and the archive index can register
    snapshots first, at startup, on a first deploy or after the instance is wiped. The first
    evaluation is also when `since_last_handled` resets, so that part stands beside it. It is true
    on that evaluation only, so a build that fails is not retried on every tick."""
    automation = dg.AutomationCondition
    catch_up = (automation.initial_evaluation() & automation.missing()).with_label("catch_up")
    return eager_or(catch_up).with_label(BUILD_NEW_SNAPSHOTS)


def rebuild_on_change() -> dg.AutomationCondition:
    """For the models rebuilt whole (staging and after): `eager()`, plus a change to the model's
    SQL, as dagster-dbt's code version is a hash of it. So a new snapshot's raw is followed by
    staging, and a deploy that changes a model rebuilds it and everything after it. It waits
    while something upstream is missing (a snapshot not yet built) or running. It has no catch-up:
    on a lake with no snapshot yet nothing upstream is missing, and it would build from nothing;
    raw's first build is what starts it."""
    changed = dg.AutomationCondition.code_version_changed().since_last_handled()
    return eager_or(changed).with_label(REBUILD_ON_CHANGE)


def eager_or(also: dg.AutomationCondition) -> dg.AutomationCondition:
    """Dagster's `eager()` (built when newly missing or a dependency updated, once nothing
    upstream is missing or running), true also when `also` is, under the same guards."""
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
    """Evaluates the dbt assets' automation conditions. Dagster's default sensor for them starts
    stopped, which would leave a new snapshot unbuilt until someone turned it on."""
    return dg.AutomationConditionSensorDefinition(
        TRANSFORM_AUTOMATION,
        target=dg.AssetSelection.assets(*assets),
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
