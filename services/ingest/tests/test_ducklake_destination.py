from leads_ingest.adapters.ducklake.destination import raw_schema_name
from leads_ingest.adapters.s3.configuration import endpoint_url
from leads_lake.settings import LakeStorage


def build_storage(*, use_ssl: bool) -> LakeStorage:
    return LakeStorage(
        data_path="s3://leads-lake/",
        archive_path="s3://leads-lake/archive/",
        endpoint="s3mock:9090",
        region="eu-central-1",
        url_style="path",
        use_ssl=use_ssl,
        key_id="s3mock",
        secret="s3mock",
    )


def test_endpoint_without_ssl_uses_plain_http() -> None:
    # Arrange
    storage = build_storage(use_ssl=False)

    # Act
    url = endpoint_url(storage)

    # Assert
    assert url == "http://s3mock:9090"


def test_endpoint_with_ssl_uses_https() -> None:
    # Arrange
    storage = build_storage(use_ssl=True)

    # Act
    url = endpoint_url(storage)

    # Assert
    assert url == "https://s3mock:9090"


def test_each_source_loads_into_its_own_raw_schema() -> None:
    # Arrange
    source_names = ("sodra", "jar")

    # Act
    schemas = {raw_schema_name(source_name) for source_name in source_names}

    # Assert
    assert schemas == {"raw_sodra", "raw_jar"}
