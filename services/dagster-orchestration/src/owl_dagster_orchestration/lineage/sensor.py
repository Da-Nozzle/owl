import functools
import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo

import dagster as dg
import httpx
from dagster._core.definitions.assets.graph.asset_graph import AssetGraph
from openlineage.client.event_v2 import RunEvent
from openlineage.client.serde import Serde

from owl_dagster_orchestration.lineage.events import by_lowercase_name, materialization_event
from owl_dagster_orchestration.lineage.settings import LineageSettings
from owl_dagster_orchestration.metadata_keys import COLUMN_SCHEMA
from owl_http.client import build_http_client

LINEAGE = "lineage"


def build_lineage_sensor(
    settings: LineageSettings,
    user_agent: str,
    snapshot_timezone: str,
    *,
    transport: httpx.BaseTransport | None = None,
) -> dg.SensorDefinition:
    """Sends the location's materializations, oldest first. If Marquez is unreachable, the tick
    stops and the next one resends; an event it refuses (4xx) is skipped, or it would block."""
    client = build_http_client(user_agent, settings.http, transport=transport)
    endpoint = settings.endpoint_url
    zone = ZoneInfo(snapshot_timezone)

    @dg.sensor(
        name=LINEAGE,
        minimum_interval_seconds=settings.sensor_interval_seconds,
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
            for key, record in unsent(
                context.instance, graph, sent, settings.events_per_tick, ready
            ):
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
    """Upstream assets first, each asset's oldest first, stopping at an asset's first that is not
    `ready`: a run still going may not have run its checks yet."""
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


def check_results(instance: dg.DagsterInstance, run_id: str) -> Sequence[dg.AssetCheckEvaluation]:
    records = instance.get_records_for_run(
        run_id, of_type=dg.DagsterEventType.ASSET_CHECK_EVALUATION
    ).records
    return [record.event_log_entry.get_dagster_event().event_specific_data for record in records]


def checks_of(
    record: dg.EventLogRecord, results: Sequence[dg.AssetCheckEvaluation]
) -> Sequence[dg.AssetCheckEvaluation]:
    """Those that checked this materialization, the last of each, as a retry evaluates it again."""
    last: dict[str, dg.AssetCheckEvaluation] = {}
    for result in results:
        target = result.target_materialization_data
        if result.asset_key == record.asset_key and (
            target is None or target.storage_id == record.storage_id
        ):
            last[result.check_name] = result
    return [last[name] for name in sorted(last)]


def last_columns(instance: dg.DagsterInstance, key: dg.AssetKey) -> Mapping[str, str]:
    """The columns of the asset's last materialization, by lowercase name."""
    event = instance.get_latest_materialization_event(key)
    materialization = event.asset_materialization if event else None
    schema = materialization.metadata.get(COLUMN_SCHEMA) if materialization else None
    if not isinstance(schema, dg.TableSchemaMetadataValue):
        return {}
    return by_lowercase_name(schema.schema)
