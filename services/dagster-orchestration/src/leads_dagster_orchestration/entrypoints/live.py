import dagster as dg

from leads_dagster_orchestration.entrypoints.definitions import pipeline_definitions
from leads_ingest.settings import PipelineMode


@dg.definitions
def live_definitions() -> dg.Definitions:
    """The live code location: fetches from the sources and builds the live lake."""
    return pipeline_definitions(PipelineMode.LIVE)
