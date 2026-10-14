from collections.abc import Iterator

import dagster as dg
from dagster_dlt import DagsterDltResource, dlt_assets
from dlt import Pipeline

from leads_dagster_orchestration.translators import RawTableDltTranslator
from leads_ingest.adapters.lake_probe.source import lake_probe_source


def build_lake_probe_assets(pipeline: Pipeline) -> dg.AssetsDefinition:
    @dlt_assets(
        dlt_source=lake_probe_source(),
        dlt_pipeline=pipeline,
        name="lake_probe",
        pool=pipeline.dataset_name,
        dagster_dlt_translator=RawTableDltTranslator(),
    )
    def lake_probe_assets(
        context: dg.AssetExecutionContext, dlt: DagsterDltResource
    ) -> Iterator[object]:
        yield from dlt.run(context=context)

    return lake_probe_assets
