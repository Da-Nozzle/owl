import os
import subprocess
import sys
import sysconfig
from collections.abc import Mapping
from pathlib import Path
from typing import NoReturn

from leads_config.loading import configure_logging, load_configuration
from leads_dagster_orchestration.tool_environments import (
    dagster_storage_environment,
    dbt_dev_environment,
    dbt_environment,
    dbt_main_state_dir,
)

DEFINITIONS_MODULE = "leads_dagster_orchestration.entrypoints.definitions"


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
            "--module-name",
            DEFINITIONS_MODULE,
        ],
        # dbt's variables are set by the definitions, from the configuration they load.
        extra_environment=dagster_storage_environment(config),
    )


def run_dbt() -> NoReturn:
    config = load_configuration()
    replace_process_with(["dbt", *sys.argv[1:]], extra_environment=dbt_environment(config))


def run_dbt_dev() -> NoReturn:
    config = load_configuration()
    environment = dbt_environment(config)
    main_state = Path(dbt_main_state_dir(config))
    if not (main_state / "manifest.json").exists():
        subprocess.run(
            ["dbt", "parse", "--quiet", "--target-path", str(main_state)],
            env=launch_environment(environment),
            check=True,
        )
    dev_environment = dbt_dev_environment(config)
    Path(dev_environment["LEADS_DBT_DEV_PATH"]).parent.mkdir(parents=True, exist_ok=True)
    replace_process_with(["dbt", *sys.argv[1:]], extra_environment=environment | dev_environment)


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
