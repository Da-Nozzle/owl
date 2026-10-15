from leads_ingest.adapters.ducklake.destination import raw_schema_name


def test_each_source_loads_into_its_own_raw_schema() -> None:
    # Act / Assert
    assert (raw_schema_name("sodra"), raw_schema_name("jar")) == ("raw_sodra", "raw_jar")
