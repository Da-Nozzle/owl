from collections.abc import Iterator, MutableMapping

import dagster as dg
import pytest
import yaml

from owl_config.configuration import Configuration
from owl_dagster_orchestration.dev_tools.dagster_dev import dagster_storage_environment


@pytest.fixture
def postgres_instance(
    pytestconfig: pytest.Config,
    services_config: Configuration,
    environ: MutableMapping[str, str],
) -> Iterator[dg.DagsterInstance]:
    """An instance as the dev container's dagster.yaml makes it, its storage in Postgres, in the
    session's own database, emptied for each test."""
    dagster_yaml = pytestconfig.rootpath / ".devcontainer" / "dagster" / "dagster.yaml"
    # Read by dagster.yaml, here and in the processes a run starts.
    environ.update(dagster_storage_environment(services_config))
    with dg.instance_for_test(overrides=yaml.safe_load(dagster_yaml.read_text())) as instance:
        instance.wipe()
        instance.schedule_storage.wipe()
        yield instance
