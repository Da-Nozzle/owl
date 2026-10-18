import pytest

from leads_ingest.sources.google_places.file_names import query_name


@pytest.mark.parametrize(
    ("query", "name"),
    [("kavinė", "kavine"), ("grožio salonas", "grozio-salonas"), ("Car Repair", "car-repair")],
)
def test_a_query_is_written_in_ascii_in_file_names(query: str, name: str) -> None:
    # Act / Assert
    assert query_name(query) == name
