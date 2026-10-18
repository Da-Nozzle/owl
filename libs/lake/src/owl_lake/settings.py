from dataclasses import dataclass, replace

from owl_config.configuration import Configuration


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
    """The S3-compatible bucket that holds the lake's Parquet files and the raw source archive."""

    data_path: str
    archive_path: str
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


def load_lake_settings(config: Configuration, *, replay: bool = False) -> LakeSettings:
    """The live lake, or with `replay` the replay lake: its own catalog schema and data path
    (`lake.replay`) on the same Postgres and bucket."""
    catalog = load_lake_catalog(config)
    storage = load_lake_storage(config)
    if not replay:
        return LakeSettings(catalog=catalog, storage=storage)
    return LakeSettings(
        catalog=replace(catalog, metadata_schema=config.get_str("lake.replay.metadata_schema")),
        storage=replace(storage, data_path=config.get_str("lake.replay.data_path")),
    )


def load_lake_catalog(config: Configuration) -> LakeCatalog:
    return LakeCatalog(
        host=config.get_str("postgres.host"),
        port=config.get_int("postgres.port"),
        database=config.get_str("postgres.databases.ducklake_catalog"),
        user=config.get_str("postgres.user"),
        password=config.get_str("postgres.password"),
        metadata_schema=config.get_str("lake.metadata_schema"),
    )


def load_lake_storage(config: Configuration) -> LakeStorage:
    return LakeStorage(
        data_path=config.get_str("lake.data_path"),
        archive_path=config.get_str("lake.archive_path"),
        endpoint=config.get_str("lake.s3.endpoint"),
        region=config.get_str("lake.s3.region"),
        url_style=config.get_str("lake.s3.url_style"),
        use_ssl=config.get_bool("lake.s3.use_ssl"),
        key_id=config.get_str("lake.s3.key_id"),
        secret=config.get_str("lake.s3.secret"),
    )
