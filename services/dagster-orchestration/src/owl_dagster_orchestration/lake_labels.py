from collections.abc import Mapping
from enum import StrEnum

import dagster as dg

from owl_dagster_orchestration.run_tags import (
    AUTOMATION_TAG,
    LAKE_TAG,
    MODE_TAG,
    SCHEDULE_TAG,
    TRIGGER_TAG,
)
from owl_ingest.settings import PipelineMode
from owl_lake.settings import LakeSettings


class Trigger(StrEnum):
    SCHEDULE = "schedule"
    FETCH = "fetch"  # started by hand
    AUTOMATION = "automation"
    REBUILD = "rebuild"  # started by hand


def lake_labels(mode: PipelineMode, lake: LakeSettings) -> dict[str, str]:
    return {MODE_TAG: mode.value, LAKE_TAG: lake.catalog.metadata_schema}


def run_trigger(run_tags: Mapping[str, str], *, fetches: bool) -> Trigger:
    """From the run's tags, not its job, so a run started from the asset graph is labelled too."""
    if SCHEDULE_TAG in run_tags:
        return Trigger.SCHEDULE
    if AUTOMATION_TAG in run_tags:
        return Trigger.AUTOMATION
    return Trigger.FETCH if fetches else Trigger.REBUILD


def label_run(
    context: dg.AssetExecutionContext, labels: Mapping[str, str], *, fetches: bool = False
) -> dict[str, str]:
    """Tags the run with the labels and its trigger, and returns them for its materializations."""
    run_labels = {**labels, TRIGGER_TAG: run_trigger(context.run.tags, fetches=fetches).value}
    context.instance.add_run_tags(context.run.run_id, run_labels)
    return run_labels


def with_labels[Event](event: Event, labels: Mapping[str, str]) -> Event:
    if not isinstance(event, dg.Output | dg.AssetMaterialization):
        return event
    return event.with_metadata({**event.metadata, **labels})
