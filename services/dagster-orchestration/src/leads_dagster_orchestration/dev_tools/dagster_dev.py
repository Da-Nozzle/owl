from typing import NoReturn

from leads_config.configuration import Configuration
from leads_config.loading import configure_logging, load_configuration
from leads_dagster_orchestration.dev_tools.launch import replace_process_with


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
        # Each code location sets dbt's variables for its own lake.
        extra_environment=dagster_storage_environment(config),
    )


def dagster_storage_environment(config: Configuration) -> dict[str, str]:
    """What the instance's dagster.yaml reads from the environment."""
    return {
        "DAGSTER_PG_HOST": config.get_str("postgres.host"),
        "DAGSTER_PG_PORT": str(config.get_int("postgres.port")),
        "DAGSTER_PG_USER": config.get_str("postgres.user"),
        "DAGSTER_PG_PASSWORD": config.get_str("postgres.password"),
        "DAGSTER_PG_DB": config.get_str("postgres.databases.dagster"),
    }


def dagster_webserver_url(config: Configuration) -> str:
    """As a client on this machine reaches it: a wildcard bind address is not one to connect to."""
    host = config.get_str("dagster.webserver.host")
    if host in {"0.0.0.0", "::"}:
        host = "localhost"
    return f"http://{host}:{config.get_int('dagster.webserver.port')}"
