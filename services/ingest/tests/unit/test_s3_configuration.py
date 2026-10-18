import pytest

from leads_ingest.adapters.s3.configuration import endpoint_url, s3fs_options
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


def test_the_bucket_is_reached_with_its_credentials_region_and_url_style() -> None:
    # Arrange
    storage = load_lake_settings(
        build_config(
            {
                "lake.s3.key_id": "key",
                "lake.s3.secret": "secret",
                "lake.s3.region": "eu-central-1",
                "lake.s3.url_style": "vhost",
            }
        )
    ).storage

    # Act
    options = s3fs_options(storage)

    # Assert
    assert (options["key"], options["secret"], options["endpoint_url"]) == (
        "key",
        "secret",
        endpoint_url(storage),
    )
    assert options["client_kwargs"] == {"region_name": "eu-central-1"}
    assert options["config_kwargs"] == {
        "s3": {"addressing_style": "virtual"},
        "request_checksum_calculation": "when_required",
        "response_checksum_validation": "when_required",
    }


def test_an_unknown_url_style_is_rejected() -> None:
    # Arrange
    storage = load_lake_settings(build_config({"lake.s3.url_style": "dns"})).storage

    # Act / Assert
    with pytest.raises(ValueError, match="url_style"):
        s3fs_options(storage)
