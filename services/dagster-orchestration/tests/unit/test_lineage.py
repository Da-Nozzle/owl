from zoneinfo import ZoneInfo

import pytest

from owl_dagster_orchestration.lineage.events import nominal_time_facet
from owl_dagster_orchestration.lineage.settings import load_lineage_settings
from owl_testing.config import build_config

VILNIUS = ZoneInfo("Europe/Vilnius")


def test_a_monthly_snapshot_runs_from_its_first_day_to_the_next_months() -> None:
    # Act
    facet = nominal_time_facet("2026-12", VILNIUS)

    # Assert
    assert facet is not None
    assert facet.nominalStartTime == "2026-12-01T00:00:00+02:00"
    assert facet.nominalEndTime == "2027-01-01T00:00:00+02:00"


def test_a_daily_snapshot_runs_one_day_in_the_pipelines_timezone() -> None:
    # Act
    facet = nominal_time_facet("2026-10-24", VILNIUS)

    # Assert
    assert facet is not None
    assert facet.nominalStartTime == "2026-10-24T00:00:00+03:00"
    assert facet.nominalEndTime == "2026-10-25T00:00:00+03:00"


@pytest.mark.parametrize("partition", [None, "sample", "2026-13", "2026-10-18|vilnius"])
def test_a_key_that_names_no_snapshot_has_no_nominal_time(partition: str | None) -> None:
    # Act
    facet = nominal_time_facet(partition, VILNIUS)

    # Assert
    assert facet is None


def test_lineage_settings_come_from_the_lineage_table() -> None:
    # Arrange
    config = build_config({"lineage.url": "http://marquez.test:5000"})

    # Act
    settings = load_lineage_settings(config)

    # Assert
    assert settings.endpoint_url == "http://marquez.test:5000/api/v1/lineage"
    assert settings.namespace == "owl"
    assert (settings.sensor_interval_seconds, settings.events_per_tick) == (30, 500)
    assert settings.http.retry.attempts == 3


@pytest.mark.parametrize("key", ["lineage.sensor_interval_seconds", "lineage.events_per_tick"])
def test_the_sensor_must_tick_and_send_something(key: str) -> None:
    # Arrange
    config = build_config({key: 0})

    # Act / Assert
    with pytest.raises(ValueError, match=key):
        load_lineage_settings(config)
