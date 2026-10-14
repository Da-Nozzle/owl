from dataclasses import dataclass

from leads_config.configuration import Configuration


@dataclass(frozen=True, slots=True)
class LakeCatalog:
    """The Postgres database and schema that hold the lake's metadata."""

    host: str
    port: int
    database: str
    user: str
    password: str
    metadata_schema: str


@dataclass(frozen=True, slots=True)
class LakeStorage:
    """The S3-compatible bucket that holds the lake's Parquet files."""

    data_path: str
    endpoint: str
    region: str
    url_style: str
    use_ssl: bool
    key_id: str
    secret: str


@dataclass(frozen=True, slots=True)
class LakeSettings:
    catalog: LakeCatalog
    storage: LakeStorage


def load_lake_settings(config: Configuration) -> LakeSettings:
    return LakeSettings(catalog=load_lake_catalog(config), storage=load_lake_storage(config))


def load_lake_catalog(
    config: Configuration,
) -> LakeCatalog:  # TODO in aws would use IAM tokens so this can only work for local dev. Meed to re-think how to do this in a way that works for both local dev and AWS in a elegant way.
    return LakeCatalog(
        host=config.get_str("postgres.host"),
        port=config.get_int("postgres.port"),
        database=config.get_str("postgres.databases.ducklake_catalog"),
        user=config.get_str("postgres.user"),
        password=config.get_str("postgres.password"),
        metadata_schema=config.get_str("lake.metadata_schema"),
    )


def load_lake_storage(config: Configuration) -> LakeStorage:
    use_ssl = config.get("lake.s3.use_ssl")
    if not isinstance(use_ssl, bool):
        raise TypeError(f"'lake.s3.use_ssl' is {type(use_ssl).__name__}, expected bool")
    return LakeStorage(
        data_path=config.get_str("lake.data_path"),
        endpoint=config.get_str("lake.s3.endpoint"),
        region=config.get_str("lake.s3.region"),
        url_style=config.get_str("lake.s3.url_style"),
        use_ssl=use_ssl,
        key_id=config.get_str("lake.s3.key_id"),
        secret=config.get_str("lake.s3.secret"),
    )
