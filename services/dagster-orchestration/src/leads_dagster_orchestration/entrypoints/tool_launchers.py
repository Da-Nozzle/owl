import os
import sys
from collections.abc import Mapping
from typing import NoReturn

from leads_config.loading import configure_logging, load_configuration
from leads_dagster_orchestration.tool_environments import (
    dagster_storage_environment,
    dbt_environment,
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
        extra_environment=dagster_storage_environment(config) | dbt_environment(config),
    )


def run_dbt() -> NoReturn:
    config = load_configuration()
    replace_process_with(["dbt", *sys.argv[1:]], extra_environment=dbt_environment(config))


def replace_process_with(command: list[str], *, extra_environment: Mapping[str, str]) -> NoReturn:
    os.execvpe(command[0], command, {**os.environ, **extra_environment})
