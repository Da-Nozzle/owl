import dagster as dg

from owl_dagster_orchestration.entrypoints.definitions import pipeline_definitions
from owl_ingest.settings import PipelineMode


@dg.definitions
def replay_definitions() -> dg.Definitions:
    """Builds the replay lake from the committed sample, calling no source."""
    return pipeline_definitions(PipelineMode.REPLAY)
