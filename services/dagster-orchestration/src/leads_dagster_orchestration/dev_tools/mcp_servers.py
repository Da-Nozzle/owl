import sys
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

from leads_config.configuration import Configuration
from leads_config.loading import load_configuration
from leads_dagster_orchestration.dbt.environment import dbt_environment, dbt_target_dir
from leads_dagster_orchestration.dbt.resource import dbt_core_executable
from leads_dagster_orchestration.dev_tools.dagster_dev import dagster_webserver_url
from leads_dagster_orchestration.dev_tools.dbt_cli import dev_environment
from leads_dagster_orchestration.dev_tools.launch import (
    LAKE_OPTION,
    replace_process_with,
    split_lake_option,
)
from leads_ingest.settings import PipelineMode
from leads_lake.connection import (
    attach_sql,
    load_lake_extensions_sql,
    s3_secret_sql,
    sql_literal,
)
from leads_lake.settings import load_lake_settings

DEV_OPTION = "--dev"


@dataclass(frozen=True, slots=True)
class McpSettings:
    dbt_package: str
    dbt_command_timeout_seconds: int
    lake_package: str
    dagster_package: str


def load_mcp_settings(config: Configuration) -> McpSettings:
    timeout = config.get_int("mcp.dbt.command_timeout_seconds")
    if timeout < 1:
        raise ValueError(f"'mcp.dbt.command_timeout_seconds' must be at least 1: {timeout}")
    return McpSettings(
        dbt_package=config.get_str("mcp.dbt.package"),
        dbt_command_timeout_seconds=timeout,
        lake_package=config.get_str("mcp.lake.package"),
        dagster_package=config.get_str("mcp.dagster.package"),
    )


def run_dbt_mcp() -> NoReturn:
    """dbt Labs' MCP server, running dbt as leads-dbt does (--lake names the lake it writes to)
    or, with --dev, as leads-dbt-dev does (replay unless --lake is given)."""
    arguments = sys.argv[1:]
    dev = DEV_OPTION in arguments
    arguments = [argument for argument in arguments if argument != DEV_OPTION]
    mode, rest = split_lake_option(arguments, default=PipelineMode.REPLAY if dev else None)
    if rest:
        sys.exit(f"leads-mcp-dbt takes only {LAKE_OPTION} and {DEV_OPTION}, not {rest}")
    config = load_configuration()
    settings = load_mcp_settings(config)
    environment = dev_environment(config, mode) if dev else dbt_environment(config, mode)
    replace_process_with(
        ["uvx", settings.dbt_package],
        extra_environment=environment | dbt_mcp_environment(settings.dbt_command_timeout_seconds),
    )


def dbt_mcp_environment(command_timeout_seconds: int) -> dict[str, str]:
    """The project's dbt, and none of the dbt platform's tools, which need an account."""
    return {
        "DBT_PATH": str(dbt_core_executable()),
        "DBT_CLI_TIMEOUT": str(command_timeout_seconds),
        "DISABLE_SEMANTIC_LAYER": "true",
        "DISABLE_DISCOVERY": "true",
        "DISABLE_ADMIN_API": "true",
        "DISABLE_REMOTE": "true",
        "DISABLE_SQL": "true",
        "DISABLE_LSP": "true",
        "DO_NOT_TRACK": "1",
    }


def run_lake_mcp() -> NoReturn:
    """Both lakes, writable. Its startup SQL holds the lake's credentials, so only this user
    may read it."""
    config = load_configuration()
    settings = load_mcp_settings(config)
    init_sql = Path(dbt_target_dir(config)) / "mcp" / "lake_init.sql"
    init_sql.parent.mkdir(parents=True, exist_ok=True)
    init_sql.touch(mode=0o600)
    init_sql.write_text(lake_init_sql(config))
    replace_process_with(
        [
            "uvx",
            settings.lake_package,
            "--db-path",
            ":memory:",
            "--init-sql",
            str(init_sql),
            "--read-write",
        ],
        extra_environment={},
    )


def lake_init_sql(config: Configuration) -> str:
    """DuckDB in memory, capped as each dbt process is, with both lakes attached as `live` and
    `replay` as the dbt profile attaches them."""
    environment = dbt_environment(config, PipelineMode.LIVE)
    statements = [
        f"SET memory_limit = {sql_literal(environment['LEADS_DUCKDB_MEMORY_LIMIT'])}",
        f"SET temp_directory = {sql_literal(environment['LEADS_DUCKDB_TEMP_DIRECTORY'] + '/mcp')}",
        *load_lake_extensions_sql(),
        "INSTALL zipfs FROM community; LOAD zipfs",  # reads the archived zips in place
        s3_secret_sql(load_lake_settings(config).storage),
        *(
            attach_sql(load_lake_settings(config, replay=mode is PipelineMode.REPLAY), mode.value)
            for mode in PipelineMode
        ),
    ]
    return "".join(f"{statement};\n" for statement in statements)


def run_dagster_mcp() -> NoReturn:
    """On the webserver leads-dagster-dev starts, with its write tools on."""
    config = load_configuration()
    replace_process_with(
        ["uvx", load_mcp_settings(config).dagster_package],
        extra_environment={
            "DAGSTER_URL": dagster_webserver_url(config),
            "DAGSTER_READ_ONLY": "false",
        },
    )
