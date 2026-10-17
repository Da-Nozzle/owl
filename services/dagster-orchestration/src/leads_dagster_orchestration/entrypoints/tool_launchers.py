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
    dbt_dev_environment,
    dbt_environment,
    dbt_main_state_dir,
)
from leads_ingest.settings import PipelineMode

LAKE_OPTION = "--lake"
LAKES = ", ".join(mode.value for mode in PipelineMode)


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
