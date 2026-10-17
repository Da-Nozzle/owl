import dagster as dg

from leads_dagster_orchestration.entrypoints.definitions import pipeline_definitions
from leads_ingest.settings import PipelineMode


@dg.definitions
def replay_definitions() -> dg.Definitions:
    """The replay code location: builds the replay lake from the committed sample, calling no
    source, under asset keys starting with replay/."""
    return pipeline_definitions(PipelineMode.REPLAY)
