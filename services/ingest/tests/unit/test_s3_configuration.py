import pytest

from leads_ingest.adapters.s3.configuration import endpoint_url, filesystem_configuration
from leads_lake.settings import load_lake_settings
from leads_testing.config import build_config


@pytest.mark.parametrize(("use_ssl", "scheme"), [(False, "http"), (True, "https")])
def test_endpoint_scheme_follows_the_ssl_flag(use_ssl: bool, scheme: str) -> None:
    # Arrange
    storage = load_lake_settings(
        build_config({"lake.s3.endpoint": "s3mock:9090", "lake.s3.use_ssl": use_ssl})
    ).storage

    # Act
    url = endpoint_url(storage)

    # Assert
    assert url == f"{scheme}://s3mock:9090"


def test_a_lake_location_carries_the_buckets_credentials() -> None:
    # Arrange
    storage = load_lake_settings(
        build_config(
            {
                "lake.s3.key_id": "key",
                "lake.s3.secret": "secret",
                "lake.s3.region": "eu-central-1",
                "lake.s3.url_style": "path",
            }
        )
    ).storage

    # Act
    location = filesystem_configuration("s3://leads-lake/archive/", storage)

    # Assert
    credentials = location.credentials
    assert location.bucket_url == "s3://leads-lake/archive/"
    assert (
        credentials.aws_access_key_id,
        credentials.aws_secret_access_key,
        credentials.region_name,
        credentials.s3_url_style,
        credentials.endpoint_url,
    ) == ("key", "secret", "eu-central-1", "path", endpoint_url(storage))
