import dlt
from dlt.common.configuration.specs import AwsCredentials, ConnectionStringCredentials
from dlt.common.destination import Destination
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

from leads_lake.settings import LakeCatalog, LakeSettings, LakeStorage

LAKE_ATTACH_NAME = "lake"


def build_ducklake_destination(settings: LakeSettings) -> Destination:
    return dlt.destinations.ducklake(
        credentials=DuckLakeCredentials(
            ducklake_name=LAKE_ATTACH_NAME,
            metadata_schema=settings.catalog.metadata_schema,
            catalog=catalog_credentials(settings.catalog),
            storage=storage_configuration(settings.storage),
        )
    )


def build_raw_pipeline(source_name: str, destination: Destination) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=source_name,
        destination=destination,
        dataset_name=raw_schema_name(source_name),
    )


def raw_schema_name(source_name: str) -> str:
    """Each source loads into its own schema, so pipelines share no dlt bookkeeping tables."""
    return f"raw_{source_name}"


def catalog_credentials(catalog: LakeCatalog) -> ConnectionStringCredentials:
    return ConnectionStringCredentials(
        {
            "drivername": "postgres",
            "host": catalog.host,
            "port": catalog.port,
            "database": catalog.database,
            "username": catalog.user,
            "password": catalog.password,
        }
    )


def storage_configuration(storage: LakeStorage) -> FilesystemConfiguration:
    return FilesystemConfiguration(
        bucket_url=storage.data_path,
        credentials=AwsCredentials(
            aws_access_key_id=storage.key_id,
            aws_secret_access_key=storage.secret,
            region_name=storage.region,
            endpoint_url=endpoint_url(storage),
            s3_url_style=storage.url_style,
        ),
    )


def endpoint_url(storage: LakeStorage) -> str:
    """dlt reads whether to use TLS from the endpoint's scheme."""
    scheme = "https" if storage.use_ssl else "http"
    return f"{scheme}://{storage.endpoint}"
