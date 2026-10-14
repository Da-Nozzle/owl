from collections.abc import Iterator

import dlt
from dlt.sources import DltResource

PROBE_ROWS = (
    {"probe_id": 1, "label": "written by dlt"},
    {"probe_id": 2, "label": "read by dbt"},
)


@dlt.source(name="lake_probe")
def lake_probe_source() -> DltResource:
    """Fixed rows that prove dlt and dbt share one DuckLake. Remove once a real source lands."""
    return probe_rows()


@dlt.resource(name="probe_rows", write_disposition="replace", primary_key="probe_id")
def probe_rows() -> Iterator[dict[str, object]]:
    yield from PROBE_ROWS
