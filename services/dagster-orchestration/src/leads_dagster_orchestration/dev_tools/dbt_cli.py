import json
import subprocess
import sys
from pathlib import Path
from typing import NoReturn

from leads_config.configuration import Configuration
from leads_config.loading import load_configuration
from leads_dagster_orchestration.dbt.environment import (
    dbt_environment,
    dbt_location_dir,
    dbt_main_state_dir,
)
from leads_dagster_orchestration.dev_tools.launch import (
    launch_environment,
    replace_process_with,
    split_lake_option,
)
from leads_ingest.settings import PipelineMode


def run_dbt() -> NoReturn:
    """--lake names the lake dbt writes to."""
    mode, arguments = split_lake_option(sys.argv[1:], default=None)
    config = load_configuration()
    replace_process_with(["dbt", *arguments], extra_environment=dbt_environment(config, mode))


def run_dbt_dev() -> NoReturn:
    """dbt on the dev target, reading a lake read-only: --lake names which, replay unless given."""
    mode, arguments = split_lake_option(sys.argv[1:], default=PipelineMode.REPLAY)
    config = load_configuration()
    replace_process_with(["dbt", *arguments], extra_environment=dev_environment(config, mode))


def run_dbt_diff() -> NoReturn:
    """Compares the models named, or those the last dev command built, with the lake's tables,
    through the project's `diff` macro."""
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
    """Parses the lake's project into the state the dev target defers to, if no build has."""
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


def dbt_dev_environment(config: Configuration, mode: PipelineMode) -> dict[str, str]:
    """Builds into a DuckDB file of its own, deferring to the lake for other models. Kept apart
    from the location's artifacts and per lake, so neither invalidates the other's parse."""
    dev_dir = f"{dbt_location_dir(config, mode)}/dev"
    return {
        "DBT_TARGET": "dev",
        "DBT_TARGET_PATH": dev_dir,
        "LEADS_DBT_DEV_PATH": f"{dev_dir}/dev.duckdb",
        "DBT_DEFER": "true",
        "DBT_STATE": dbt_main_state_dir(config, mode),
    }


def built_models(run_results: Path) -> list[str]:
    """The models a dbt command built successfully, by unique id."""
    if not run_results.exists():
        return []
    results = json.loads(run_results.read_text())["results"]
    return [
        result["unique_id"]
        for result in results
        if result["unique_id"].startswith("model.") and result["status"] == "success"
    ]
