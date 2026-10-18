"""Lineage in Marquez: every asset a code location materializes, sent as an OpenLineage event.

An asset is a job in the OpenLineage sense, named by its key (`live/raw/raw_sodra__monthly`), whose
input datasets are its parents in the asset graph and whose output dataset is the asset itself, by
the same name. So the graph Marquez draws is Dagster's: fetch -> archive -> raw -> staging, per
lake, with each table's columns, its rows, its description and its checks' results (dbt's tests),
which Marquez shows as its quality. A materialization is a completed run of that job, so a job's
runs are the asset's history, each with the snapshot it built as its nominal time. Failures are
not sent: Dagster OSS records a failed step, not which of its assets failed, so they are Dagster's
to show.

Each code location has its own sensor, which reads only its own assets' events: their keys start
with the lake, so the two never send the same event. The sensor reads the event log rather than
wrapping the instance's storage (dagster-openlineage's way, see DECISIONS.md), and sends after the
fact: a materialization reaches Marquez once its run has ended, within a tick of the sensor, half
a minute, so that it carries its checks."""

import functools
import json
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import dagster as dg
import httpx
from dagster._core.definitions.assets.graph.asset_graph import AssetGraph, AssetNode
from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.facet_v2 import (
    column_lineage_dataset,
    data_quality_assertions_dataset,
    documentation_dataset,
    job_type_job,
    nominal_time_run,
    output_statistics_output_dataset,
    schema_dataset,
    sql_job,
)
from openlineage.client.serde import Serde

from leads_config.configuration import Configuration
from leads_dagster_orchestration.assets.transform import COMPILED_SQL, ROW_COUNT
from leads_ingest.adapters.http.client import build_http_client
from leads_ingest.adapters.http.settings import HttpClientSettings, load_http_client_settings

LINEAGE = "lineage"  # TODO move some of these vars to toml settings (maybe under  new file like marquez.toml, for marque)
PRODUCER = "urn:leads:dagster-orchestration"
LINEAGE_ENDPOINT = "api/v1/lineage"
# The most events one tick sends. A first tick has the instance's whole history to send, which
# goes over several ticks, each well within the time the daemon gives a sensor.
EVENTS_PER_TICK = 500
# The formats snapshot keys are in (each source's SNAPSHOT_KEY_FORMAT): a month or a day.
MONTH_KEY_FORMAT = "%Y-%m"
DAY_KEY_FORMAT = "%Y-%m-%d"
COLUMN_SCHEMA = "dagster/column_schema"
COLUMN_LINEAGE = "dagster/column_lineage"
# OpenLineage run ids are UUIDs; each is derived from what it stands for, so an event sent twice
# (a tick that stopped before saving its cursor) is the same run in Marquez, not a second one.
RUN_ID_NAMESPACE = uuid.UUID("5b3a4c1e-2f0d-4d6b-9a57-3c8e1f6a9d20")


# TODO put this in a seperate sub-dir and split this file up.
@dataclass(frozen=True, slots=True)
class LineageSettings:
    """Where lineage goes, from the `lineage` table: Marquez's API, the namespace every job and
    dataset is in, and how requests to it are sent (`lineage.http`)."""

    url: str
    namespace: str
    http: HttpClientSettings


def load_lineage_settings(config: Configuration) -> LineageSettings:
    return LineageSettings(
        url=config.get_str("lineage.url"),
        namespace=config.get_str("lineage.namespace"),
        http=load_http_client_settings(config, "lineage.http"),
    )


def build_lineage_sensor(
    settings: LineageSettings,
    user_agent: str,
    timezone: str,
    *,
    transport: httpx.BaseTransport | None = None,
) -> dg.SensorDefinition:
    """Sends Marquez each materialization of the location's assets, oldest first. Its cursor is,
    per asset, the last materialization sent; on a first tick that is none, so the instance's
    history goes too. If Marquez cannot be reached, the tick stops where it is and the next one
    carries on from there. An event Marquez refuses (4xx) would be refused again, so it is logged
    and passed over. `timezone` is the pipeline's, which snapshot keys are in. Tests replace the
    network with `transport`."""
    client = build_http_client(user_agent, settings.http, transport=transport)
    endpoint = f"{settings.url.rstrip('/')}/{LINEAGE_ENDPOINT}"
    zone = ZoneInfo(timezone)

    @dg.sensor(
        name=LINEAGE,
        minimum_interval_seconds=30,
        default_status=dg.DefaultSensorStatus.RUNNING,
        description="Sends each materialization of this location's assets to Marquez.",
    )
    def lineage(context: dg.SensorEvaluationContext) -> dg.SensorResult:
        assert context.repository_def is not None, "the daemon evaluates a sensor in its location"
        graph = context.repository_def.asset_graph
        sent: dict[str, int] = json.loads(context.cursor) if context.cursor else {}
        taken = refused = 0

        @functools.cache
        def columns(key: dg.AssetKey) -> Mapping[str, str]:
            return last_columns(context.instance, key)

        @functools.cache
        def run_checks(run_id: str) -> Sequence[dg.AssetCheckEvaluation]:
            return check_results(context.instance, run_id)

        @functools.cache
        def ended(run_id: str) -> bool:
            run = context.instance.get_run_by_id(run_id)
            return run is None or run.is_finished

        def ready(record: dg.EventLogRecord) -> bool:
            return not record.run_id or ended(record.run_id)

        try:
            for key, record in unsent(context.instance, graph, sent, EVENTS_PER_TICK, ready):
                checks = checks_of(record, run_checks(record.run_id) if record.run_id else [])
                event = materialization_event(
                    record, graph.get(key), settings.namespace, zone, columns, checks
                )
                if send(client, endpoint, event, context.log):
                    taken += 1
                else:
                    refused += 1
                sent[key.to_user_string()] = record.storage_id
        except httpx.HTTPError as error:
            context.log.warning(f"Marquez did not take lineage; the next tick resends: {error}")
            return dg.SensorResult(
                cursor=json.dumps(sent, sort_keys=True),
                skip_reason=f"Marquez took {taken} events, then failed: {error}",
            )
        return dg.SensorResult(
            cursor=json.dumps(sent, sort_keys=True),
            skip_reason=f"Marquez took {taken} events and refused {refused}",
        )

    return lineage


def unsent(
    instance: dg.DagsterInstance,
    graph: AssetGraph,
    sent: Mapping[str, int],
    limit: int,
    ready: Callable[[dg.EventLogRecord], bool],
) -> Iterator[tuple[dg.AssetKey, dg.EventLogRecord]]:
    """Up to `limit` materializations not yet sent, by a run or reported by the archive index: an
    asset at a time, upstream first, each asset's oldest first. An asset's materializations wait,
    from the first that is not `ready` on: one whose run is still going may not have all its
    checks yet (dbt tests a model after building it)."""
    for key in graph.toposorted_asset_keys:
        if limit == 0:
            return
        records = instance.fetch_materializations(
            dg.AssetRecordsFilter(asset_key=key, after_storage_id=sent.get(key.to_user_string())),
            limit=limit,
            ascending=True,
        ).records
        for record in records:
            if not ready(record):
                break
            limit -= 1
            yield key, record


def send(client: httpx.Client, endpoint: str, event: RunEvent, log: dg.DagsterLogManager) -> bool:
    """False if Marquez refused the event; raises if it could not be reached or failed."""
    try:
        client.post(
            endpoint, content=Serde.to_json(event), headers={"Content-Type": "application/json"}
        )
    except httpx.HTTPStatusError as error:
        if not error.response.is_client_error:
            raise
        log.error(f"Marquez refused the lineage of {event.job.name}: {error}")
        return False
    return True


def materialization_event(
    record: dg.EventLogRecord,
    node: AssetNode,
    namespace: str,
    timezone: ZoneInfo,
    columns: Callable[[dg.AssetKey], Mapping[str, str]],
    checks: Sequence[dg.AssetCheckEvaluation] = (),
) -> RunEvent:
    """A completed run of the asset's job: its parents in, the asset out, with the columns, the
    column lineage, the rows and the SQL the materialization reports, the asset's description,
    and the results of `checks`, the checks of the materialization. `columns` gives an asset's
    columns by their lowercase names (`last_columns`)."""
    materialization = record.asset_materialization
    assert materialization is not None
    metadata = materialization.metadata
    facets: dict[str, object] = {}
    if checks:
        # OpenLineage defines these assertions as an input's, checked by the job reading it;
        # Marquez shows a table's quality from its own facets, and dbt tests the table its model
        # has just written, so they are the output's.
        facets["dataQualityAssertions"] = quality_facet(checks)
    schema = metadata.get(COLUMN_SCHEMA)
    own_columns: Mapping[str, str] = {}
    if isinstance(schema, dg.TableSchemaMetadataValue):
        facets["schema"] = schema_facet(schema.schema, column_descriptions(node))
        own_columns = by_lowercase_name(schema.schema)
    column_lineage = metadata.get(COLUMN_LINEAGE)
    if isinstance(column_lineage, dg.TableColumnLineageMetadataValue):
        facets["columnLineage"] = column_lineage_facet(
            column_lineage.column_lineage, namespace, own_columns, columns
        )
    if node.description:
        facets["documentation"] = documentation_dataset.DocumentationDatasetFacet(
            description=node.description, producer=PRODUCER
        )
    output_facets: dict[str, object] = {}
    rows = metadata.get(ROW_COUNT)
    if isinstance(rows, dg.IntMetadataValue) and rows.value is not None:
        output_facets["outputStatistics"] = (
            output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(
                rowCount=rows.value, producer=PRODUCER
            )
        )
    run_facets: dict[str, object] = {}
    nominal = nominal_time_facet(record.partition_key, timezone)
    if nominal is not None:
        run_facets["nominalTime"] = nominal
    job_facets: dict[str, object] = {
        "jobType": job_type_job.JobTypeJobFacet(
            processingType="BATCH", integration="DAGSTER", jobType="ASSET", producer=PRODUCER
        )
    }
    sql = metadata.get(COMPILED_SQL)
    if isinstance(sql, dg.TextMetadataValue) and sql.value:
        job_facets["sql"] = sql_job.SQLJobFacet(query=sql.value, producer=PRODUCER)
    name = dataset_name(node.key)
    return RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.fromtimestamp(record.timestamp, UTC).isoformat(),
        run=Run(runId=lineage_run_id(record, node.key), facets=run_facets),
        job=Job(namespace=namespace, name=name, facets=job_facets),
        producer=PRODUCER,
        inputs=[
            InputDataset(namespace=namespace, name=dataset_name(parent))
            for parent in sorted(parent_keys(node), key=dg.AssetKey.to_user_string)
        ],
        outputs=[
            OutputDataset(namespace=namespace, name=name, facets=facets, outputFacets=output_facets)
        ],
    )


def lineage_run_id(record: dg.EventLogRecord, key: dg.AssetKey) -> str:
    """One run per Dagster run, asset and partition. A materialization the archive index reported
    has no Dagster run: it is a run of its own."""
    dagster_run = record.run_id or f"runless/{record.storage_id}/{record.timestamp}"
    name = f"{dagster_run}|{key.to_user_string()}|{record.partition_key or ''}"
    return str(uuid.uuid5(RUN_ID_NAMESPACE, name))


def nominal_time_facet(
    partition: str | None, timezone: ZoneInfo
) -> nominal_time_run.NominalTimeRunFacet | None:
    """The period a snapshot key names, a month or a day, in the pipeline's timezone. Marquez lists
    a job's runs with it, so each run shows which snapshot it built."""
    if partition is None:
        return None
    for key_format, period_after in ((DAY_KEY_FORMAT, day_after), (MONTH_KEY_FORMAT, month_after)):
        try:
            start = datetime.strptime(partition, key_format).replace(tzinfo=timezone)
        except ValueError:
            continue
        return nominal_time_run.NominalTimeRunFacet(
            nominalStartTime=start.isoformat(),
            nominalEndTime=period_after(start).isoformat(),
            producer=PRODUCER,
        )
    return None


def day_after(start: datetime) -> datetime:
    return start + timedelta(days=1)


def month_after(start: datetime) -> datetime:
    return (start + timedelta(days=32)).replace(day=1)


def schema_facet(
    schema: dg.TableSchema, descriptions: Mapping[str, str]
) -> schema_dataset.SchemaDatasetFacet:
    """The table's columns as built, each described as the build says or else as `descriptions`
    does, by lowercase name."""
    return schema_dataset.SchemaDatasetFacet(
        fields=[
            schema_dataset.SchemaDatasetFacetFields(
                name=column.name,
                type=column.type,
                description=column.description or descriptions.get(column.name.lower()),
            )
            for column in schema.columns
        ],
        producer=PRODUCER,
    )


def column_descriptions(node: AssetNode) -> Mapping[str, str]:
    """The asset's column descriptions, by lowercase name. dagster-dbt puts those of the dbt YAML
    in the asset's definition, with no types (they are in the SQL); a build reports each column's
    type, read from the table, with no description. A definition keeps the schema as given, a
    `TableSchema`, where an event's metadata wraps it."""
    schema = node.metadata.get(COLUMN_SCHEMA)
    if isinstance(schema, dg.TableSchemaMetadataValue):
        schema = schema.schema
    if not isinstance(schema, dg.TableSchema):
        return {}
    return {
        column.name.lower(): column.description for column in schema.columns if column.description
    }


def column_lineage_facet(
    lineage: dg.TableColumnLineage,
    namespace: str,
    own_columns: Mapping[str, str],
    columns: Callable[[dg.AssetKey], Mapping[str, str]],
) -> column_lineage_dataset.ColumnLineageDatasetFacet:
    """Each column of the table, from the parents' columns it is computed from. dagster-dbt names
    them in lowercase (sqlglot's), while a table keeps its columns' case: raw keeps the source's
    headers, `Draudėjo kodas (code)`. So each is named as its table has it, if the table's columns
    are known, and the lineage names columns the tables in Marquez have."""

    def named(column: str, known: Mapping[str, str]) -> str:
        return known.get(column.lower(), column)

    return column_lineage_dataset.ColumnLineageDatasetFacet(
        fields={
            named(column, own_columns): column_lineage_dataset.Fields(
                inputFields=[
                    column_lineage_dataset.InputField(
                        namespace=namespace,
                        name=dataset_name(dep.asset_key),
                        field=named(dep.column_name, columns(dep.asset_key)),
                    )
                    for dep in deps
                ]
            )
            for column, deps in lineage.deps_by_column.items()
        },
        producer=PRODUCER,
    )


def check_results(instance: dg.DagsterInstance, run_id: str) -> Sequence[dg.AssetCheckEvaluation]:
    """The results of the checks a run evaluated, in order: under dagster-dbt, its dbt tests."""
    records = instance.get_records_for_run(
        run_id, of_type=dg.DagsterEventType.ASSET_CHECK_EVALUATION
    ).records
    return [record.event_log_entry.get_dagster_event().event_specific_data for record in records]


def checks_of(
    record: dg.EventLogRecord, results: Sequence[dg.AssetCheckEvaluation]
) -> Sequence[dg.AssetCheckEvaluation]:
    """Of a run's check results, those that checked the materialization, the last of each check
    (a check evaluated again in the run, on retry, is its last result)."""
    last: dict[str, dg.AssetCheckEvaluation] = {}
    for result in results:
        target = result.target_materialization_data
        if result.asset_key == record.asset_key and (
            target is None or target.storage_id == record.storage_id
        ):
            last[result.check_name] = result
    return [last[name] for name in sorted(last)]


def quality_facet(
    checks: Sequence[dg.AssetCheckEvaluation],
) -> data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet:
    """Each check as an assertion that passed or failed, with its severity. Marquez rates the
    table healthy if all passed."""
    return data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(
        assertions=[
            data_quality_assertions_dataset.Assertion(
                assertion=check.check_name,
                success=check.passed,
                severity=check.severity.value.lower(),
            )
            for check in checks
        ],
        producer=PRODUCER,
    )


def last_columns(instance: dg.DagsterInstance, key: dg.AssetKey) -> Mapping[str, str]:
    """The columns of the asset's last materialization, by their lowercase names; none if it
    reported none."""
    event = instance.get_latest_materialization_event(key)
    materialization = event.asset_materialization if event else None
    schema = materialization.metadata.get(COLUMN_SCHEMA) if materialization else None
    if not isinstance(schema, dg.TableSchemaMetadataValue):
        return {}
    return by_lowercase_name(schema.schema)


def by_lowercase_name(schema: dg.TableSchema) -> Mapping[str, str]:
    return {column.name.lower(): column.name for column in schema.columns}


def parent_keys(node: AssetNode) -> Sequence[dg.AssetKey]:
    return [key for key in node.parent_entity_keys if isinstance(key, dg.AssetKey)]


def dataset_name(key: dg.AssetKey) -> str:
    return key.to_user_string()
