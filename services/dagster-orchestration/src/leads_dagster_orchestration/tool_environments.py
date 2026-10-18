from leads_config.configuration import Configuration
from leads_ingest.settings import PipelineMode, load_pipeline_settings, raw_archive_path
from leads_lake.connection import (
    attach_sql,
    ducklake_catalog_connection,
    s3_secret_sql,
    sql_literal,
)
from leads_lake.settings import load_lake_settings


def dagster_storage_environment(config: Configuration) -> dict[str, str]:
    """Values that the instance's dagster.yaml reads from the environment."""
    return {
        "DAGSTER_PG_HOST": config.get_str("postgres.host"),
        "DAGSTER_PG_PORT": str(config.get_int("postgres.port")),
        "DAGSTER_PG_USER": config.get_str("postgres.user"),
        "DAGSTER_PG_PASSWORD": config.get_str("postgres.password"),
        "DAGSTER_PG_DB": config.get_str("postgres.databases.dagster"),
    }


def dbt_environment(config: Configuration, mode: PipelineMode) -> dict[str, str]:
    """Values that the dbt project reads through env_var(): the mode's lake and the archive it
    reads, and how much memory DuckDB may take in each dbt process. What it spills beyond that
    goes to a folder of the process's own under the temp directory (the profile adds dbt's
    invocation id): DuckDB names its spill files the same in every process, so processes that
    shared a folder, as they do its default `.tmp` in the working directory, would write over
    each other's."""
    pipeline = load_pipeline_settings(config)
    lake = load_lake_settings(config, replay=mode is PipelineMode.REPLAY)
    return {
        "LEADS_ARCHIVE_PATH": raw_archive_path(mode, pipeline, lake.storage),
        "DBT_PROJECT_DIR": config.get_str("transform.project_dir"),
        "DBT_PROFILES_DIR": config.get_str("transform.project_dir"),
        "LEADS_DUCKDB_MEMORY_LIMIT": config.get_str("transform.duckdb_memory_limit"),
        "LEADS_DUCKDB_TEMP_DIRECTORY": f"{dbt_target_dir(config)}/duckdb_tmp",
        "LEADS_DUCKLAKE_CATALOG": ducklake_catalog_connection(lake.catalog),
        "LEADS_DUCKLAKE_METADATA_SCHEMA": lake.catalog.metadata_schema,
        "LEADS_LAKE_DATA_PATH": lake.storage.data_path,
        "LEADS_S3_ENDPOINT": lake.storage.endpoint,
        "LEADS_S3_REGION": lake.storage.region,
        "LEADS_S3_URL_STYLE": lake.storage.url_style,
        "LEADS_S3_USE_SSL": "true" if lake.storage.use_ssl else "false",
        "LEADS_S3_KEY_ID": lake.storage.key_id,
        "LEADS_S3_SECRET": lake.storage.secret,
    }


def dagster_dbt_environment(config: Configuration, mode: PipelineMode) -> dict[str, str]:
    """dbt under a code location keeps its artifacts in that location's folder, target/<mode>/:
    the parse made at load, its partial parse and each run's folder (dagster-dbt reads
    DBT_TARGET_PATH for the last two). The two code servers parse the same project with each
    lake's values, so sharing target/ each would overwrite the other's parse and re-parse.

    Its DuckDB main database is a file there instead of memory. After a build, dagster-dbt
    reopens that database read-only to read each table's columns and row count, which it cannot
    do for `:memory:`. The file holds nothing (models build into the lake); run by hand, dbt
    stays in memory, so it never waits on the file's lock."""
    location_dir = dbt_location_dir(config, mode)
    return {
        "DBT_TARGET_PATH": location_dir,
        "LEADS_DBT_DUCKDB_PATH": f"{location_dir}/dagster.duckdb",
    }


def dbt_dev_environment(config: Configuration, mode: PipelineMode) -> dict[str, str]:
    """dbt on the dev target (leads-dbt-dev), for iterating on models without running the
    pipeline: it builds into a DuckDB file of its own and reads the mode's lake read-only. Every
    model it does not build resolves to the lake's table, as that location's last build named it
    (dbt's defer). Its artifacts are kept apart from the location's, so neither invalidates the
    other's partial parse, and each lake has its own dev copy, so one never shadows the other's
    tables."""
    dev_dir = f"{dbt_location_dir(config, mode)}/dev"
    return {
        "DBT_TARGET": "dev",
        "DBT_TARGET_PATH": dev_dir,
        "LEADS_DBT_DEV_PATH": f"{dev_dir}/dev.duckdb",
        "DBT_DEFER": "true",
        "DBT_STATE": dbt_main_state_dir(config, mode),
    }


def lake_init_sql(config: Configuration) -> str:
    """What the lake MCP server (leads-mcp-lake) runs on startup: DuckDB in memory, capped as
    each dbt process is, with both lakes attached writable as `live` and `replay`, through the
    same catalog, bucket and secret the dbt profile uses. DuckLake keeps its catalog in Postgres,
    so this connection writes beside Dagster's and dbt's runs without locking them out."""
    environment = dbt_environment(config, PipelineMode.LIVE)
    statements = [
        f"SET memory_limit = {sql_literal(environment['LEADS_DUCKDB_MEMORY_LIMIT'])}",
        f"SET temp_directory = {sql_literal(environment['LEADS_DUCKDB_TEMP_DIRECTORY'] + '/mcp')}",
        *(f"INSTALL {name}; LOAD {name}" for name in ["httpfs", "postgres", "ducklake"]),
        "INSTALL zipfs FROM community; LOAD zipfs",  # reads the archived zips in place
        s3_secret_sql(load_lake_settings(config).storage),
        *(
            attach_sql(load_lake_settings(config, replay=mode is PipelineMode.REPLAY), mode.value)
            for mode in PipelineMode
        ),
    ]
    return "".join(f"{statement};\n" for statement in statements)


def dagster_webserver_url(config: Configuration) -> str:
    """The webserver leads-dagster-dev starts, as a client on the same machine reaches it: a
    wildcard bind address is not one to connect to."""
    host = config.get_str("dagster.webserver.host")
    if host in {"0.0.0.0", "::"}:
        host = "localhost"
    return f"http://{host}:{config.get_int('dagster.webserver.port')}"


def dbt_main_state_dir(config: Configuration, mode: PipelineMode) -> str:
    """Where a code location keeps the manifest of its last dbt build: the state the dev target
    defers to."""
    return f"{dbt_location_dir(config, mode)}/main"


def dbt_location_dir(config: Configuration, mode: PipelineMode) -> str:
    return f"{dbt_target_dir(config)}/{mode.value}"


def dbt_target_dir(config: Configuration) -> str:
    return f"{config.get_str('transform.project_dir').rstrip('/')}/target"
