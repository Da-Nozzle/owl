"""The lineage sensor on a test instance, sending to a fake Marquez: what each event says, and
what the sensor does when Marquez is down or refuses an event."""

import json
from collections.abc import Callable, Iterator
from typing import Any

import dagster as dg
import httpx
import pytest

from leads_dagster_orchestration.assets.transform import COMPILED_SQL
from leads_dagster_orchestration.lineage import (
    LINEAGE,
    LineageSettings,
    build_lineage_sensor,
    unsent,
)
from leads_ingest.adapters.http.settings import HttpClientSettings, RetryPolicy

ARCHIVE = dg.AssetKey(["live", "archive", "archive_sodra"])
RAW = dg.AssetKey(["live", "raw", "raw_sodra__monthly"])
STAGING = dg.AssetKey(["live", "staging", "stg_sodra__monthly"])
SNAPSHOTS = dg.StaticPartitionsDefinition(["2026-09"])
SETTINGS = LineageSettings(
    url="http://marquez.test:5000",
    namespace="leads",
    # One attempt, so a failing Marquez fails at once rather than after the retries' waits.
    http=HttpClientSettings(
        timeout_seconds=1,
        retry=RetryPolicy(attempts=1, backoff_seconds=0, max_wait_seconds=0),
        min_interval_seconds=0,
    ),
)
# Raw keeps the source's headers; dagster-dbt names columns in its lineage in lowercase.
RAW_SCHEMA = dg.TableSchema(
    columns=[
        dg.TableColumn("Draudėjo kodas (code)", "varchar", description="Sodra's employer code"),
        dg.TableColumn("insured", "integer"),
    ]
)
RAW_LINEAGE = dg.TableColumnLineage(
    deps_by_column={
        "draudėjo kodas (code)": [dg.TableColumnDep(asset_key=ARCHIVE, column_name="kodas")]
    }
)
# As dagster-dbt reports them: a build, each column's type read from the table; the definition,
# the descriptions of the dbt YAML, with no type.
STAGING_SCHEMA = dg.TableSchema(columns=[dg.TableColumn("insurer_code", "bigint")])
STAGING_DEFINITION_SCHEMA = dg.TableSchema(
    columns=[dg.TableColumn("insurer_code", "?", description="Sodra's code for the employer")]
)
STAGING_SQL = 'select "Draudėjo kodas (code)"::bigint as insurer_code from raw_sodra.monthly'
STAGING_LINEAGE = dg.TableColumnLineage(
    deps_by_column={
        "insurer_code": [dg.TableColumnDep(asset_key=RAW, column_name="draudėjo kodas (code)")]
    }
)

archive = dg.AssetSpec(ARCHIVE)


@dg.asset(key=RAW, deps=[ARCHIVE], partitions_def=SNAPSHOTS, description="Sodra's months.")
def raw() -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata={
            "dagster/column_schema": RAW_SCHEMA,
            "dagster/column_lineage": RAW_LINEAGE,
            "dagster/row_count": 3,
        }
    )


@dg.asset(key=STAGING, deps=[RAW], metadata={"dagster/column_schema": STAGING_DEFINITION_SCHEMA})
def staging() -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata={
            "dagster/column_schema": STAGING_SCHEMA,
            "dagster/column_lineage": STAGING_LINEAGE,
            COMPILED_SQL: dg.MetadataValue.text(STAGING_SQL),
        }
    )


# Staging's tests, as dagster-dbt makes them checks: one passes; one fails, a warning.
@dg.asset_check(asset=STAGING, name="not_null_stg_sodra__monthly_insurer_code")
def insurer_code_not_null() -> dg.AssetCheckResult:
    return dg.AssetCheckResult(passed=True)


@dg.asset_check(asset=STAGING, name="unique_stg_sodra__monthly_insurer_code")
def insurer_code_unique() -> dg.AssetCheckResult:
    return dg.AssetCheckResult(passed=False, severity=dg.AssetCheckSeverity.WARN)


STAGING_CHECKS = [insurer_code_not_null, insurer_code_unique]


class FakeMarquez:
    """Takes OpenLineage events as Marquez's API does, or answers each as `answer` says."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.answer: Callable[[dict[str, Any]], int] = lambda _event: 201

    def handle(self, request: httpx.Request) -> httpx.Response:
        assert request.url == "http://marquez.test:5000/api/v1/lineage"
        event = json.loads(request.content)
        status = self.answer(event)
        if status < 300:
            self.events.append(event)
        return httpx.Response(status)

    def jobs(self) -> list[str]:
        return [event["job"]["name"] for event in self.events]


@pytest.fixture
def instance() -> Iterator[dg.DagsterInstance]:
    with dg.instance_for_test() as instance:
        yield instance


@pytest.fixture
def marquez() -> FakeMarquez:
    return FakeMarquez()


def build_definitions(marquez: FakeMarquez) -> dg.Definitions:
    sensor = build_lineage_sensor(
        SETTINGS, "leads-test", "Europe/Vilnius", transport=httpx.MockTransport(marquez.handle)
    )
    return dg.Definitions(
        assets=[archive, raw, staging], asset_checks=STAGING_CHECKS, sensors=[sensor]
    )


def tick(
    definitions: dg.Definitions, instance: dg.DagsterInstance, cursor: str | None = None
) -> dg.SensorResult:
    sensor = definitions.resolve_sensor_def(LINEAGE)
    result = sensor(
        dg.build_sensor_context(instance=instance, cursor=cursor, definitions=definitions)
    )
    assert isinstance(result, dg.SensorResult)
    return result


def skip_message(result: dg.SensorResult) -> str:
    assert result.skip_reason is not None
    return result.skip_reason.skip_message or ""


def build_history(instance: dg.DagsterInstance) -> None:
    """The archive index reports a snapshot, then raw and staging build it, staging's tests in the
    same run, as dbt build runs them."""
    instance.report_runless_asset_event(dg.AssetMaterialization(ARCHIVE, partition="2026-09"))
    dg.materialize([raw], instance=instance, partition_key="2026-09")
    dg.materialize([staging, *STAGING_CHECKS], instance=instance)


def test_each_materialization_is_a_completed_run_of_its_assets_job_from_its_parents(
    instance: dg.DagsterInstance, marquez: FakeMarquez
) -> None:
    # Arrange
    build_history(instance)

    # Act
    tick(build_definitions(marquez), instance)

    # Assert
    assert marquez.jobs() == [
        "live/archive/archive_sodra",
        "live/raw/raw_sodra__monthly",
        "live/staging/stg_sodra__monthly",
    ]
    assert {event["eventType"] for event in marquez.events} == {"COMPLETE"}
    archived, built, staged = marquez.events
    assert archived["inputs"] == []
    assert built["job"]["namespace"] == "leads"
    assert [(d["namespace"], d["name"]) for d in built["inputs"]] == [
        ("leads", "live/archive/archive_sodra")
    ]
    assert [d["name"] for d in built["outputs"]] == ["live/raw/raw_sodra__monthly"]
    assert [d["name"] for d in staged["inputs"]] == ["live/raw/raw_sodra__monthly"]


def test_a_built_table_carries_its_columns_rows_description_and_snapshot(
    instance: dg.DagsterInstance, marquez: FakeMarquez
) -> None:
    # Arrange
    build_history(instance)

    # Act
    tick(build_definitions(marquez), instance)

    # Assert
    built = marquez.events[1]
    (output,) = built["outputs"]
    columns = output["facets"]["schema"]["fields"]
    assert [(c["name"], c["type"], c.get("description")) for c in columns] == [
        ("Draudėjo kodas (code)", "varchar", "Sodra's employer code"),
        ("insured", "integer", None),
    ]
    assert output["facets"]["documentation"]["description"] == "Sodra's months."
    assert output["outputFacets"]["outputStatistics"]["rowCount"] == 3
    nominal = built["run"]["facets"]["nominalTime"]
    assert nominal["nominalStartTime"] == "2026-09-01T00:00:00+03:00"
    assert nominal["nominalEndTime"] == "2026-10-01T00:00:00+03:00"


def input_columns(event: dict[str, Any]) -> dict[str, list[tuple[str, str, str]]]:
    """Each column of the event's output, from the parents' columns it is computed from."""
    lineage = event["outputs"][0]["facets"]["columnLineage"]["fields"]
    return {
        column: [(f["namespace"], f["name"], f["field"]) for f in fields["inputFields"]]
        for column, fields in lineage.items()
    }


def test_column_lineage_names_each_column_as_its_table_has_it(
    instance: dg.DagsterInstance, marquez: FakeMarquez
) -> None:
    # Arrange
    build_history(instance)

    # Act
    tick(build_definitions(marquez), instance)

    # Assert
    _, built, staged = marquez.events
    assert input_columns(built) == {
        # The archive is files, with no columns Dagster knows of: named as dagster-dbt has it
        "Draudėjo kodas (code)": [("leads", "live/archive/archive_sodra", "kodas")]
    }
    assert input_columns(staged) == {
        "insurer_code": [("leads", "live/raw/raw_sodra__monthly", "Draudėjo kodas (code)")]
    }


def test_a_later_tick_sends_only_what_happened_since(
    instance: dg.DagsterInstance, marquez: FakeMarquez
) -> None:
    # Arrange
    definitions = build_definitions(marquez)
    build_history(instance)
    first = tick(definitions, instance)
    marquez.events.clear()
    dg.materialize([raw], instance=instance, partition_key="2026-09")

    # Act
    second = tick(definitions, instance, first.cursor)
    third = tick(definitions, instance, second.cursor)

    # Assert
    assert marquez.jobs() == ["live/raw/raw_sodra__monthly"]
    assert skip_message(third) == "Marquez took 0 events and refused 0"


def test_runs_of_the_same_asset_are_distinct_and_each_is_stable(
    instance: dg.DagsterInstance, marquez: FakeMarquez
) -> None:
    # Arrange
    definitions = build_definitions(marquez)
    dg.materialize([raw], instance=instance, partition_key="2026-09")
    dg.materialize([raw], instance=instance, partition_key="2026-09")

    # Act
    tick(definitions, instance)
    tick(definitions, instance)  # from no cursor again: the same events, resent

    # Assert
    run_ids = [event["run"]["runId"] for event in marquez.events]
    assert len(run_ids) == 4
    assert run_ids[:2] == run_ids[2:]
    assert run_ids[0] != run_ids[1]


def test_when_marquez_is_down_the_next_tick_resends_from_where_this_one_stopped(
    instance: dg.DagsterInstance, marquez: FakeMarquez
) -> None:
    # Arrange
    definitions = build_definitions(marquez)
    build_history(instance)
    marquez.answer = lambda event: 503 if "raw" in event["job"]["name"] else 201

    # Act
    failed = tick(definitions, instance)
    marquez.answer = lambda _event: 201
    recovered = tick(definitions, instance, failed.cursor)

    # Assert
    assert skip_message(failed).startswith("Marquez took 1 events, then failed: 503")
    assert skip_message(recovered) == "Marquez took 2 events and refused 0"
    assert marquez.jobs() == [
        "live/archive/archive_sodra",
        "live/raw/raw_sodra__monthly",
        "live/staging/stg_sodra__monthly",
    ]


def test_an_event_marquez_refuses_is_passed_over(
    instance: dg.DagsterInstance, marquez: FakeMarquez
) -> None:
    # Arrange
    definitions = build_definitions(marquez)
    build_history(instance)
    marquez.answer = lambda event: 422 if "raw" in event["job"]["name"] else 201

    # Act
    first = tick(definitions, instance)
    second = tick(definitions, instance, first.cursor)

    # Assert
    assert skip_message(first) == "Marquez took 2 events and refused 1"
    assert skip_message(second) == "Marquez took 0 events and refused 0"
    assert marquez.jobs() == ["live/archive/archive_sodra", "live/staging/stg_sodra__monthly"]


def test_only_the_locations_own_assets_are_sent(
    instance: dg.DagsterInstance, marquez: FakeMarquez
) -> None:
    # Arrange: the other lake's asset, built on the same instance by the other code location
    @dg.asset(key=["replay", "raw", "raw_sodra__monthly"])
    def replay_raw() -> None:
        pass

    dg.materialize([replay_raw], instance=instance)

    # Act
    tick(build_definitions(marquez), instance)

    # Assert
    assert marquez.events == []


def test_a_column_is_typed_as_built_and_described_as_its_definition_says(
    instance: dg.DagsterInstance, marquez: FakeMarquez
) -> None:
    # Arrange
    build_history(instance)

    # Act
    tick(build_definitions(marquez), instance)

    # Assert
    staged = marquez.events[2]
    (column,) = staged["outputs"][0]["facets"]["schema"]["fields"]
    assert (column["name"], column["type"], column["description"]) == (
        "insurer_code",
        "bigint",
        "Sodra's code for the employer",
    )


def test_a_model_built_from_sql_shows_its_sql_with_its_run(
    instance: dg.DagsterInstance, marquez: FakeMarquez
) -> None:
    # Arrange
    build_history(instance)

    # Act
    tick(build_definitions(marquez), instance)

    # Assert
    _, built, staged = marquez.events
    assert "sql" not in built["job"]["facets"]
    assert staged["job"]["facets"]["sql"]["query"] == STAGING_SQL


def test_a_tables_tests_are_its_quality_in_marquez(
    instance: dg.DagsterInstance, marquez: FakeMarquez
) -> None:
    # Arrange
    build_history(instance)

    # Act
    tick(build_definitions(marquez), instance)

    # Assert
    _, built, staged = marquez.events
    assert "dataQualityAssertions" not in built["outputs"][0]["facets"]
    assertions = staged["outputs"][0]["facets"]["dataQualityAssertions"]["assertions"]
    assert [(a["assertion"], a["success"], a["severity"]) for a in assertions] == [
        ("not_null_stg_sodra__monthly_insurer_code", True, "error"),
        ("unique_stg_sodra__monthly_insurer_code", False, "warn"),
    ]


def test_an_assets_materializations_wait_while_one_of_their_runs_is_going(
    instance: dg.DagsterInstance, marquez: FakeMarquez
) -> None:
    # Arrange: raw built twice, the second run still going; staging built once
    graph = build_definitions(marquez).get_repository_def().asset_graph
    first = dg.materialize([raw], instance=instance, partition_key="2026-09").run_id
    going = dg.materialize([raw], instance=instance, partition_key="2026-09").run_id
    staged = dg.materialize([staging], instance=instance).run_id

    # Act
    pending = list(unsent(instance, graph, {}, 10, lambda record: record.run_id != going))

    # Assert
    assert [(key.path[-1], record.run_id) for key, record in pending] == [
        ("raw_sodra__monthly", first),
        ("stg_sodra__monthly", staged),
    ]
