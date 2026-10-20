"""Fixtures for the dev container's services (.devcontainer/compose.yml): Postgres, s3mock and
Marquez, reached as config/local says. A test session makes resources of its own in them and
removes them after it, so the tests never write to the dev databases, the dev lake's bucket or
Marquez's dev namespace. A service that does not answer in time fails the tests that need it."""

import secrets
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import psycopg2
import psycopg2.extensions
import pytest
from psycopg2 import sql
from s3fs import S3FileSystem

from owl_config.configuration import Configuration
from owl_config.loading import load_configuration, read_config_directory
from owl_config.secret_stores import build_secret_store
from owl_ingest.adapters.s3.configuration import s3fs_options
from owl_lake.settings import load_lake_storage
from owl_testing.config import with_overrides

# In CI the services start with the tests, and Marquez (a JVM, migrating its database first) may
# take a minute to answer.
SERVICE_TIMEOUT_SECONDS = 120
POLL_SECONDS = 1
# Every database, bucket and Marquez namespace the tests make is named after it.
TEST_PREFIX = "owl_test"
# Postgres's own database, from which the tests' databases are made and dropped.
MAINTENANCE_DATABASE = "postgres"


def local_config_directory(pytestconfig: pytest.Config) -> Path:
    return pytestconfig.rootpath / "config" / "local"


@pytest.fixture(scope="session")
def local_config(pytestconfig: pytest.Config) -> Configuration:
    """config/local, as the dev container mounts it: where each service is reached."""
    return load_configuration(local_config_directory(pytestconfig))


@pytest.fixture(scope="session")
def session_name() -> str:
    """What this session's resources are named after, so that two sessions at once, or one that
    died before its cleanup, never share one."""
    return f"{TEST_PREFIX}_{secrets.token_hex(4)}"


def wait_for(service: str, address: str, attempt: Callable[[], object]) -> None:
    """Returns once `attempt` succeeds; fails the test if `service` has not answered at `address`
    within SERVICE_TIMEOUT_SECONDS."""
    deadline = time.monotonic() + SERVICE_TIMEOUT_SECONDS
    while True:
        try:
            attempt()
        except Exception as error:
            if time.monotonic() >= deadline:
                pytest.fail(
                    f"{service} did not answer at {address} within {SERVICE_TIMEOUT_SECONDS} s: "
                    f"{error}. The integration tests use the dev container's services "
                    "(.devcontainer/compose.yml), reached as config/local says.",
                    pytrace=False,
                )
            time.sleep(POLL_SECONDS)
        else:
            return


@dataclass(frozen=True, slots=True)
class PostgresServer:
    """The dev container's Postgres, as config/local reaches it."""

    host: str
    port: int
    user: str
    password: str

    def connect(self, database: str = MAINTENANCE_DATABASE) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=database,
            connect_timeout=5,
        )

    def execute(self, statement: sql.Composable) -> None:
        """In the maintenance database, outside a transaction, as CREATE and DROP DATABASE run."""
        connection = self.connect()
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(statement)
        finally:
            connection.close()


@pytest.fixture(scope="session")
def postgres(local_config: Configuration) -> PostgresServer:
    server = PostgresServer(
        host=local_config.get_str("postgres.host"),
        port=local_config.get_int("postgres.port"),
        user=local_config.get_str("postgres.user"),
        password=local_config.get_str("postgres.password"),
    )
    wait_for("Postgres", f"{server.host}:{server.port}", lambda: server.connect().close())
    return server


@contextmanager
def throwaway_database(server: PostgresServer, name: str) -> Iterator[str]:
    """A new, empty database, dropped afterwards with whatever is still connected to it."""
    server.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        yield name
    finally:
        server.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
        )


@pytest.fixture(scope="session")
def dagster_database(postgres: PostgresServer, session_name: str) -> Iterator[str]:
    """In place of postgres.databases.dagster: Dagster's storage."""
    with throwaway_database(postgres, f"{session_name}_dagster") as name:
        yield name


@pytest.fixture(scope="session")
def catalog_database(postgres: PostgresServer, session_name: str) -> Iterator[str]:
    """In place of postgres.databases.ducklake_catalog: each test's lakes keep their metadata in
    schemas of their own in it."""
    with throwaway_database(postgres, f"{session_name}_ducklake_catalog") as name:
        yield name


@pytest.fixture(scope="session")
def bucket(local_config: Configuration, session_name: str) -> Iterator[str]:
    """A bucket beside the dev lake's, removed with everything in it."""
    storage = load_lake_storage(local_config)
    filesystem = S3FileSystem(**s3fs_options(storage))
    wait_for("s3mock", storage.endpoint, lambda: filesystem.ls(""))
    # A bucket's name takes no underscores.
    name = session_name.replace("_", "-")
    filesystem.mkdir(name)
    try:
        yield name
    finally:
        filesystem.rm(name, recursive=True)


@pytest.fixture
def s3_prefix(bucket: str) -> str:
    """`s3://<bucket>/<random>`: the test's own folder in the session's bucket."""
    return f"s3://{bucket}/{secrets.token_hex(4)}"


@pytest.fixture
def services_config(
    pytestconfig: pytest.Config,
    session_name: str,
    dagster_database: str,
    catalog_database: str,
    s3_prefix: str,
) -> Configuration:
    """config/local, with what the pipeline writes to the test's own: Dagster's storage and the
    lakes' catalogs in the session's databases, each lake's metadata in a schema of the test's
    own, the archive and the lakes' data in the test's folder of the session's bucket, and lineage
    in a Marquez namespace of the test's own."""
    # The folder's random name names the test's schemas and namespace too.
    test_name = s3_prefix.rsplit("/", 1)[-1]
    values = with_overrides(
        read_config_directory(local_config_directory(pytestconfig)),
        {
            "postgres.databases.dagster": dagster_database,
            "postgres.databases.ducklake_catalog": catalog_database,
            "lake.metadata_schema": f"ducklake_{test_name}",
            "lake.data_path": f"{s3_prefix}/lake/",
            "lake.archive_path": f"{s3_prefix}/archive/",
            "lake.replay.metadata_schema": f"ducklake_replay_{test_name}",
            "lake.replay.data_path": f"{s3_prefix}/replay/",
            "lineage.namespace": f"{session_name}_{test_name}",
        },
    )
    return Configuration(values=values, secret_store=build_secret_store(values))


@pytest.fixture(scope="session")
def marquez_client(local_config: Configuration) -> Iterator[httpx.Client]:
    """Marquez's API, once it answers."""
    url = local_config.get_str("lineage.url")
    with httpx.Client(base_url=f"{url.rstrip('/')}/api/v1/", timeout=10) as client:
        wait_for("Marquez", url, lambda: client.get("namespaces").raise_for_status())
        yield client


@dataclass(frozen=True, slots=True)
class Marquez:
    """What Marquez shows of `namespace`, through its API."""

    client: httpx.Client
    namespace: str

    def dataset(self, name: str) -> dict[str, Any]:
        return self.read(f"namespaces/{quote(self.namespace)}/datasets/{quote(name, safe='')}")

    def job(self, name: str) -> dict[str, Any]:
        return self.read(f"namespaces/{quote(self.namespace)}/jobs/{quote(name, safe='')}")

    def runs(self, job: str) -> list[dict[str, Any]]:
        path = f"namespaces/{quote(self.namespace)}/jobs/{quote(job, safe='')}/runs"
        return self.read(path)["runs"]

    def lineage(self, dataset: str) -> list[dict[str, Any]]:
        """The graph of jobs and datasets around `dataset`."""
        return self.read("lineage", nodeId=f"dataset:{self.namespace}:{dataset}")["graph"]

    def column_lineage(self, dataset: str) -> list[dict[str, Any]]:
        """The graph of the columns `dataset`'s columns are computed from."""
        return self.read("column-lineage", nodeId=f"dataset:{self.namespace}:{dataset}")["graph"]

    def read(self, path: str, **params: str) -> Any:
        response = self.client.get(path, params=params)
        response.raise_for_status()
        return response.json()


@pytest.fixture
def marquez_api(marquez_client: httpx.Client, services_config: Configuration) -> Iterator[Marquez]:
    """Marquez, in services_config's lineage.namespace, deleted after the test. Marquez has no
    way to remove a namespace: deleted, it is hidden with its jobs and datasets."""
    namespace = services_config.get_str("lineage.namespace")
    yield Marquez(marquez_client, namespace)
    response = marquez_client.delete(f"namespaces/{quote(namespace)}")
    # Not found if nothing was sent.
    if response.status_code != httpx.codes.NOT_FOUND:
        response.raise_for_status()
