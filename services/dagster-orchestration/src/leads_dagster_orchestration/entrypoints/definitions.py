import dagster as dg


@dg.definitions
def pipeline_definitions() -> dg.Definitions:
    return dg.Definitions()
