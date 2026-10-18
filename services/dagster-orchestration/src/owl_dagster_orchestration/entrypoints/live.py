import dagster as dg

from owl_dagster_orchestration.entrypoints.definitions import pipeline_definitions
from owl_ingest.settings import PipelineMode


@dg.definitions
def live_definitions() -> dg.Definitions:
    """Fetches from the sources and builds the live lake."""
    return pipeline_definitions(PipelineMode.LIVE)
