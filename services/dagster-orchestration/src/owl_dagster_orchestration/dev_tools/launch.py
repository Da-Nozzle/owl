import os
import sys
import sysconfig
from collections.abc import Mapping, Sequence
from typing import NoReturn

from owl_ingest.settings import PipelineMode

LAKE_OPTION = "--lake"
LAKES = ", ".join(mode.value for mode in PipelineMode)


def split_lake_option(
    arguments: Sequence[str], *, default: PipelineMode | None
) -> tuple[PipelineMode, list[str]]:
    """Takes `--lake <lake>` or `--lake=<lake>` out of the arguments; dbt has no such option."""
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


def replace_process_with(command: list[str], *, extra_environment: Mapping[str, str]) -> NoReturn:
    environment = launch_environment(extra_environment)
    os.execvpe(command[0], command, environment)


def launch_environment(extra_environment: Mapping[str, str]) -> dict[str, str]:
    """This environment's scripts go first on PATH, so the tool and what it starts run the
    project's versions, not the dbt Fusion the VS Code extension puts earlier on PATH."""
    path = os.pathsep.join([sysconfig.get_path("scripts"), os.environ.get("PATH", "")])
    return {**os.environ, **extra_environment, "PATH": path}
