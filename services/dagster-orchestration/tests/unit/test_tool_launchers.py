import os
import sysconfig

import pytest

from leads_dagster_orchestration.entrypoints.tool_launchers import launch_environment


def test_project_scripts_come_before_other_installs_on_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    monkeypatch.setenv("PATH", os.pathsep.join(["/home/dev/.local/bin", "/usr/bin"]))

    # Act
    environment = launch_environment({"LEADS_S3_REGION": "eu-central-1"})

    # Assert
    assert environment["PATH"].split(os.pathsep) == [
        sysconfig.get_path("scripts"),
        "/home/dev/.local/bin",
        "/usr/bin",
    ]
    assert environment["LEADS_S3_REGION"] == "eu-central-1"
