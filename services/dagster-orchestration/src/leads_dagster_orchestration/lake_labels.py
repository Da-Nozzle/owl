from collections.abc import Mapping

import dagster as dg

from leads_ingest.settings import PipelineMode
from leads_lake.settings import LakeSettings

MODE_TAG = "leads/mode"
LAKE_TAG = "leads/lake"


def lake_labels(mode: PipelineMode, lake: LakeSettings) -> dict[str, str]:
    """Which lake a run writes. Replay and live build into separate lakes but share Dagster's
    history (the same asset keys and run-month partitions), so the asset overview shows whichever
    mode ran last; each run is tagged, and each materialization carries the labels as metadata."""
    return {MODE_TAG: mode.value, LAKE_TAG: lake.catalog.metadata_schema}


def label_run(context: dg.AssetExecutionContext, labels: Mapping[str, str]) -> None:
    context.instance.add_run_tags(context.run_id, dict(labels))


def with_labels[Event](event: Event, labels: Mapping[str, str]) -> Event:
    if not isinstance(event, dg.Output | dg.AssetMaterialization):
        return event
    return event.with_metadata({**event.metadata, **labels})
