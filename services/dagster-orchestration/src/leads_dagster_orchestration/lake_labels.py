from collections.abc import Mapping
from enum import StrEnum

import dagster as dg

from leads_ingest.settings import PipelineMode
from leads_lake.settings import LakeSettings

MODE_TAG = "leads/mode"
LAKE_TAG = "leads/lake"
TRIGGER_TAG = "leads/trigger"
# Dagster's own run tags (dagster._core.storage.tags) for a run its schedule or its automation
# condition sensor started. A re-execution keeps its parent's.
SCHEDULE_TAG = "dagster/schedule_name"
AUTOMATION_TAG = "dagster/from_automation_condition"


class Trigger(StrEnum):
    """What started a run."""

    SCHEDULE = "schedule"  # a source's fetch schedule
    FETCH = "fetch"  # a fetch started by hand
    AUTOMATION = "automation"  # Dagster, building a new snapshot or after a SQL change
    REBUILD = "rebuild"  # a build from archived snapshots started by hand


def lake_labels(mode: PipelineMode, lake: LakeSettings) -> dict[str, str]:
    """Which lake a run writes. Each lake is a code location of its own, with asset keys of its
    own; the labels also mark every run and materialization, so a run list or a table's history
    filtered by them shows one lake's."""
    return {MODE_TAG: mode.value, LAKE_TAG: lake.catalog.metadata_schema}


def run_trigger(run_tags: Mapping[str, str], *, fetches: bool) -> Trigger:
    """Read from the run's tags and what the step does, not from which job it ran, so a run
    started from the asset graph is labelled as one from a job."""
    if SCHEDULE_TAG in run_tags:
        return Trigger.SCHEDULE
    if AUTOMATION_TAG in run_tags:
        return Trigger.AUTOMATION
    return Trigger.FETCH if fetches else Trigger.REBUILD


def label_run(
    context: dg.AssetExecutionContext, labels: Mapping[str, str], *, fetches: bool = False
) -> dict[str, str]:
    """Tags the run with the lake labels and its trigger, and returns them all for the step's
    materializations. `fetches` is for a source's fetch."""
    run_labels = {**labels, TRIGGER_TAG: run_trigger(context.run.tags, fetches=fetches).value}
    context.instance.add_run_tags(context.run.run_id, run_labels)
    return run_labels


def with_labels[Event](event: Event, labels: Mapping[str, str]) -> Event:
    if not isinstance(event, dg.Output | dg.AssetMaterialization):
        return event
    return event.with_metadata({**event.metadata, **labels})
