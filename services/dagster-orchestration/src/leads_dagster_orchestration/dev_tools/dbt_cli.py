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
    LAKE_OPTION,
    launch_environment,
    replace_process_with,
    split_lake_option,
)
from leads_ingest.settings import PipelineMode

VSCODE_ENVIRONMENT_SETTING = "dbt.environmentVariables"


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


def write_vscode_settings() -> None:
    """Gives the dbt VS Code extension the environment leads-dbt-dev runs with (--lake names the
    lake it reads, replay unless given). It goes in the machine's settings, not the repository's:
    it holds the lake's credentials."""
    mode, rest = split_lake_option(sys.argv[1:], default=PipelineMode.REPLAY)
    if rest:
        sys.exit(f"leads-dbt-vscode takes only {LAKE_OPTION}, not {rest}")
    config = load_configuration()
    environment = dev_environment(config, mode) | vscode_dbt_environment(config, mode)
    Path(environment["DBT_TARGET_PATH"]).mkdir(parents=True, exist_ok=True)
    settings_file = Path(config.get_str("vscode.machine_settings"))
    text = settings_file.read_text() if settings_file.exists() else "{}"
    try:
        updated = with_setting(text, VSCODE_ENVIRONMENT_SETTING, environment)
    except json.JSONDecodeError as error:
        sys.exit(
            f"leads-dbt-vscode: {settings_file} is not plain JSON ({error}); remove its comments"
        )
    settings_file.parent.mkdir(parents=True, exist_ok=True)
    settings_file.write_text(updated)
    settings_file.chmod(0o600)
    print(f"Wrote {VSCODE_ENVIRONMENT_SETTING} ({mode.value} lake) to {settings_file}.")
    print("Run 'Developer: Reload Window' in VS Code for the dbt extension to pick it up.")


def vscode_dbt_environment(config: Configuration, mode: PipelineMode) -> dict[str, str]:
    """The extension's dbt Fusion parses into a folder and DuckDB file of its own, so neither it
    nor leads-dbt-dev invalidates the other's parse or locks the other's file. A file, too, as
    Fusion reads a rendered ':memory:' back as YAML, a map, and refuses the profile."""
    editor_dir = f"{dbt_location_dir(config, mode)}/vscode"
    return {
        "DBT_TARGET_PATH": editor_dir,
        "LEADS_DBT_DEV_PATH": f"{editor_dir}/editor.duckdb",
    }


def with_setting(settings_text: str, name: str, value: object) -> str:
    """VS Code settings with one setting set, the others kept as they were."""
    settings = json.loads(settings_text) if settings_text.strip() else {}
    if not isinstance(settings, dict):
        raise json.JSONDecodeError("settings are not a JSON object", settings_text, 0)
    settings[name] = value
    return json.dumps(settings, indent="\t") + "\n"


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
