import json
import os
import subprocess
import sys
import sysconfig
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import NoReturn

from leads_config.configuration import Configuration
from leads_config.loading import configure_logging, load_configuration
from leads_dagster_orchestration.tool_environments import (
    dagster_storage_environment,
    dagster_webserver_url,
    dbt_dev_environment,
    dbt_environment,
    dbt_main_state_dir,
    dbt_target_dir,
    lake_init_sql,
)
from leads_ingest.settings import PipelineMode

LAKE_OPTION = "--lake"
LAKES = ", ".join(mode.value for mode in PipelineMode)
DEV_OPTION = "--dev"

# The MCP servers agents use (.mcp.json), each run by uvx in an environment of its own.
# mcp-server-motherduck pins its DuckDB, which must be the project's (duckdb in pyproject.toml):
# another version may not read the lake's DuckLake catalog.
# dbt-mcp before 2.7 imports a private function pydantic 2.14 removed, and leaves pydantic
# unpinned, so it no longer starts.
DBT_MCP = "dbt-mcp@2.7.0"
LAKE_MCP = "mcp-server-motherduck@1.1.0"
DAGSTER_MCP = "dagster-mcp@0.12.3"
# How long one dbt command an agent starts may take; dbt-mcp's default is a minute.
DBT_MCP_TIMEOUT_SECONDS = 3600


# TODO split dev helper stuff from system entrypoints for more clarity
def run_dagster_dev() -> NoReturn:
    config = load_configuration()
    configure_logging(config)
    replace_process_with(
        [
            "dagster",
            "dev",
            "--host",
            config.get_str("dagster.webserver.host"),
            "--port",
            str(config.get_int("dagster.webserver.port")),
            "--workspace",
            config.get_str("dagster.workspace"),
        ],
        # dbt's variables are set by each code location's definitions, for its own lake.
        extra_environment=dagster_storage_environment(config),
    )


def run_dbt() -> NoReturn:
    """dbt on a lake: --lake names which, as dbt writes to it."""
    mode, arguments = split_lake_option(sys.argv[1:], default=None)
    config = load_configuration()
    replace_process_with(["dbt", *arguments], extra_environment=dbt_environment(config, mode))


def run_dbt_dev() -> NoReturn:
    """dbt on the dev target, reading a lake read-only: --lake names which, replay unless given."""
    mode, arguments = split_lake_option(sys.argv[1:], default=PipelineMode.REPLAY)
    config = load_configuration()
    replace_process_with(["dbt", *arguments], extra_environment=dev_environment(config, mode))


def run_dbt_diff() -> NoReturn:
    """Compares models built on the dev target with the lake's tables: the models named (or
    given by unique id), or every model the last dev command built. The dbt `diff` macro does
    the comparing."""
    mode, models = split_lake_option(sys.argv[1:], default=PipelineMode.REPLAY)
    config = load_configuration()
    environment = dev_environment(config, mode)
    names = models or built_models(Path(environment["DBT_TARGET_PATH"]) / "run_results.json")
    if not names:
        sys.exit(
            f"leads-dbt-diff: no model was built on the dev target of the {mode.value} lake: "
            "build some with leads-dbt-dev, or name them"
        )
    # Not writing run_results.json keeps the build's, so a second diff finds the same models.
    arguments = json.dumps({"models": names})
    replace_process_with(
        ["dbt", "run-operation", "diff", "--args", arguments, "--no-write-json"],
        extra_environment=environment,
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
    environment = dev_environment(config, mode) if dev else dbt_environment(config, mode)
    replace_process_with(
        ["uvx", DBT_MCP],
        extra_environment=environment | dbt_mcp_environment(),
    )


def dbt_mcp_environment() -> dict[str, str]:
    """dbt-mcp's settings: the project's dbt (not the dbt Fusion the VS Code extension
    installs), and none of the dbt platform's tools, which need an account."""
    return {
        "DBT_PATH": str(Path(sysconfig.get_path("scripts")) / "dbt"),
        "DBT_CLI_TIMEOUT": str(DBT_MCP_TIMEOUT_SECONDS),
        "DISABLE_SEMANTIC_LAYER": "true",
        "DISABLE_DISCOVERY": "true",
        "DISABLE_ADMIN_API": "true",
        "DISABLE_REMOTE": "true",
        "DISABLE_SQL": "true",
        "DISABLE_LSP": "true",
        "DO_NOT_TRACK": "1",
    }


def run_lake_mcp() -> NoReturn:
    """MotherDuck's DuckDB MCP server on both lakes, `live` and `replay`, writable. Its startup
    SQL holds the lake's credentials, so it is written where only this user reads it."""
    config = load_configuration()
    init_sql = Path(dbt_target_dir(config)) / "mcp" / "lake_init.sql"
    init_sql.parent.mkdir(parents=True, exist_ok=True)
    init_sql.touch(mode=0o600)
    init_sql.write_text(lake_init_sql(config))
    replace_process_with(
        ["uvx", LAKE_MCP, "--db-path", ":memory:", "--init-sql", str(init_sql), "--read-write"],
        extra_environment={},
    )


def run_dagster_mcp() -> NoReturn:
    """The community Dagster MCP server, on the webserver leads-dagster-dev starts, with its
    write tools (launch, terminate, reload, schedules and sensors) on."""
    config = load_configuration()
    replace_process_with(
        ["uvx", DAGSTER_MCP],
        extra_environment={
            "DAGSTER_URL": dagster_webserver_url(config),
            "DAGSTER_READ_ONLY": "false",
        },
    )


def dev_environment(config: Configuration, mode: PipelineMode) -> dict[str, str]:
    """The dev target's variables. If no build has stored the state the dev target defers to,
    the lake's project is parsed into it first."""
    environment = dbt_environment(config, mode)
    main_state = Path(dbt_main_state_dir(config, mode))
    if not (main_state / "manifest.json").exists():
        subprocess.run(
            ["dbt", "parse", "--quiet", "--target-path", str(main_state)],
            env=launch_environment(environment),
            check=True,
        )
    dev = dbt_dev_environment(config, mode)
    Path(dev["LEADS_DBT_DEV_PATH"]).parent.mkdir(parents=True, exist_ok=True)
    return environment | dev


def split_lake_option(
    arguments: Sequence[str], *, default: PipelineMode | None
) -> tuple[PipelineMode, list[str]]:
    """Takes --lake <live|replay> (or --lake=<...>) out of a tool's arguments, which dbt has no
    option of that name to clash with."""
    rest: list[str] = []
    value: str | None = None
    remaining = iter(arguments)
    for argument in remaining:
        if argument == LAKE_OPTION:
            value = next(remaining, None)
            if value is None:
                sys.exit(f"{LAKE_OPTION} needs a lake: {LAKES}")
        elif argument.startswith(f"{LAKE_OPTION}="):
            value = argument.removeprefix(f"{LAKE_OPTION}=")
        else:
            rest.append(argument)
    if value is None:
        if default is None:
            sys.exit(f"Name the lake dbt writes to: {LAKE_OPTION} <{LAKES.replace(', ', '|')}>")
        return default, rest
    try:
        return PipelineMode(value), rest
    except ValueError:
        sys.exit(f"{LAKE_OPTION} {value!r} is no lake: {LAKES}")


def built_models(run_results: Path) -> list[str]:
    """The models a dbt command built successfully, by unique id, from its run_results.json."""
    if not run_results.exists():
        return []
    results = json.loads(run_results.read_text())["results"]
    return [
        result["unique_id"]
        for result in results
        if result["unique_id"].startswith("model.") and result["status"] == "success"
    ]


def replace_process_with(command: list[str], *, extra_environment: Mapping[str, str]) -> NoReturn:
    environment = launch_environment(extra_environment)
    os.execvpe(command[0], command, environment)


def launch_environment(extra_environment: Mapping[str, str]) -> dict[str, str]:
    """The parent environment plus the tool's variables, with this environment's scripts first on
    PATH, so the tool and whatever it starts (dagster-dbt runs `dbt` from PATH) run the versions
    the project installs. The dbt VS Code extension puts dbt Fusion at ~/.local/bin/dbt, which
    comes before dbt-core on the dev container's PATH."""
    path = os.pathsep.join([sysconfig.get_path("scripts"), os.environ.get("PATH", "")])
    return {**os.environ, **extra_environment, "PATH": path}
