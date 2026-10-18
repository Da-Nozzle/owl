import os
from collections.abc import Iterator, MutableMapping
from pathlib import Path
from unittest import mock

import pytest
from hypothesis import settings

TEST_KINDS = ("unit", "integration")

# The properties are of pure functions: a slow example is a slow machine, not a failure.
settings.register_profile("owl", deadline=None)
settings.load_profile("owl")


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        kind = kind_of_test(item.path)
        if kind is None:
            raise pytest.UsageError(
                f"{item.nodeid} is in neither tests/unit/ nor tests/integration/: unit tests run "
                "in-process only; integration tests use the filesystem, a database, an HTTP "
                "server or Dagster's executor"
            )
        item.add_marker(kind)


def kind_of_test(path: Path) -> str | None:
    parts = path.parts
    below_tests = [parts[index + 1] for index, part in enumerate(parts[:-1]) if part == "tests"]
    return below_tests[-1] if below_tests and below_tests[-1] in TEST_KINDS else None


@pytest.fixture
def environ() -> Iterator[MutableMapping[str, str]]:
    """os.environ, restored after the test to what it was, whatever the code under test set."""
    environment = os.environ  # noqa: TID251 - tests what code sets for the tools it starts
    with mock.patch.dict(environment):
        yield environment
