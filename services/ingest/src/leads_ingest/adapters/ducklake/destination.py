import dlt
from dlt.common.configuration.specs import ConnectionStringCredentials
from dlt.common.destination import Destination
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials

from leads_ingest.adapters.s3.configuration import filesystem_configuration
from leads_lake.settings import LakeCatalog, LakeSettings

LAKE_ATTACH_NAME = "lake"


def build_ducklake_destination(settings: LakeSettings) -> Destination:
    return dlt.destinations.ducklake(
        credentials=DuckLakeCredentials(
            ducklake_name=LAKE_ATTACH_NAME,
            metadata_schema=settings.catalog.metadata_schema,
            catalog=catalog_credentials(settings.catalog),
            storage=filesystem_configuration(settings.storage.data_path, settings.storage),
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
