import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import dagster as dg
from dagster._core.definitions.assets.graph.asset_graph import AssetNode
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

from owl_dagster_orchestration.metadata_keys import (
    COLUMN_LINEAGE,
    COLUMN_SCHEMA,
    COMPILED_SQL,
    ROW_COUNT,
)

PRODUCER = "urn:owl:dagster-orchestration"
# Every source's SNAPSHOT_KEY_FORMAT is one of these.
MONTH_KEY_FORMAT = "%Y-%m"
DAY_KEY_FORMAT = "%Y-%m-%d"
# Run ids derive from what they stand for, so a resent event is the same run in Marquez. A new
# namespace would make every resent event a new run.
RUN_ID_NAMESPACE = uuid.UUID("5b3a4c1e-2f0d-4d6b-9a57-3c8e1f6a9d20")


def materialization_event(
    record: dg.EventLogRecord,
    node: AssetNode,
    namespace: str,
    timezone: ZoneInfo,
    columns: Callable[[dg.AssetKey], Mapping[str, str]],
    checks: Sequence[dg.AssetCheckEvaluation] = (),
) -> RunEvent:
    """A completed run of the asset's job, reading its parents. `columns` gives an asset's
    columns by lowercase name."""
    materialization = record.asset_materialization
    assert materialization is not None
    metadata = materialization.metadata
    facets: dict[str, object] = {}
    if checks:
        # OpenLineage defines these as an input's, but Marquez reads a table's quality from its
        # own facets.
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
    """One run per Dagster run, asset and partition; the archive index's reports have no run."""
    dagster_run = record.run_id or f"runless/{record.storage_id}/{record.timestamp}"
    name = f"{dagster_run}|{key.to_user_string()}|{record.partition_key or ''}"
    return str(uuid.uuid5(RUN_ID_NAMESPACE, name))


def nominal_time_facet(
    partition: str | None, timezone: ZoneInfo
) -> nominal_time_run.NominalTimeRunFacet | None:
    """The month or day a snapshot key names, so each run in Marquez shows its snapshot."""
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
    """Columns as built, described as the build or else `descriptions` (by lowercase name) say."""
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
    """By lowercase name, from the dbt YAML, which dagster-dbt puts in the asset's definition: a
    build reports types without descriptions."""
    schema = node.metadata.get(COLUMN_SCHEMA)
    # An event's metadata wraps the schema; a definition may keep it bare.
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
    """Names each column as its table has it: dagster-dbt lowercases them, while raw keeps the
    source's headers (`Draudėjo kodas (code)`)."""

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


def quality_facet(
    checks: Sequence[dg.AssetCheckEvaluation],
) -> data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet:
    """Marquez rates the table healthy if every assertion passed."""
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


def by_lowercase_name(schema: dg.TableSchema) -> Mapping[str, str]:
    return {column.name.lower(): column.name for column in schema.columns}


def parent_keys(node: AssetNode) -> Sequence[dg.AssetKey]:
    return [key for key in node.parent_entity_keys if isinstance(key, dg.AssetKey)]


def dataset_name(key: dg.AssetKey) -> str:
    return key.to_user_string()
