"""How DuckDB reaches the lake: the catalog's connection string, the S3 secret and the ATTACH,
as dbt's profile builds them from the same settings. The lake MCP server and ingest's lake
reader run these."""

from leads_lake.settings import LakeCatalog, LakeSettings, LakeStorage


def ducklake_catalog_connection(catalog: LakeCatalog) -> str:
    return libpq_connection_string(
        {
            "host": catalog.host,
            "port": str(catalog.port),
            "dbname": catalog.database,
            "user": catalog.user,
            "password": catalog.password,
        }
    )


def s3_secret_sql(storage: LakeStorage) -> str:
    return (
        "CREATE SECRET lake_s3 ("
        "TYPE s3, "
        f"KEY_ID {sql_literal(storage.key_id)}, "
        f"SECRET {sql_literal(storage.secret)}, "
        f"REGION {sql_literal(storage.region)}, "
        f"ENDPOINT {sql_literal(storage.endpoint)}, "
        f"URL_STYLE {sql_literal(storage.url_style)}, "
        f"USE_SSL {'true' if storage.use_ssl else 'false'})"
    )


def attach_sql(settings: LakeSettings, alias: str, *, read_only: bool = False) -> str:
    catalog = sql_literal("ducklake:postgres:" + ducklake_catalog_connection(settings.catalog))
    return (
        f"ATTACH {catalog} AS {alias} ("
        f"DATA_PATH {sql_literal(settings.storage.data_path)}, "
        f"METADATA_SCHEMA {sql_literal(settings.catalog.metadata_schema)}"
        f"{', READ_ONLY' if read_only else ''})"
    )


def libpq_connection_string(parameters: dict[str, str]) -> str:
    return " ".join(f"{key}={quote_libpq_value(value)}" for key, value in parameters.items())


def quote_libpq_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def sql_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"
