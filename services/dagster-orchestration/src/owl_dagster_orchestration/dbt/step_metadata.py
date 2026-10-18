from collections.abc import Mapping
from contextlib import chdir
from typing import Any

import dagster as dg
from dagster_dbt import DbtCliInvocation

from owl_dagster_orchestration.metadata_keys import COMPILED_SQL, ROW_COUNT

# dbt meta keys: a raw model's snapshot column, and the column of when each row's file was fetched.
SNAPSHOT_COLUMN_META = "snapshot_column"
FETCHED_AT_COLUMN_META = "fetched_at_column"
TABLE_ROW_COUNT = "table_row_count"
ARCHIVE_FETCHED_AT = "archive_fetched_at"


def snapshot_metadata[Event](event: Event, build: DbtCliInvocation, snapshot: str) -> Event:
    """Counts a sliced model's rows of this snapshot, where dagster-dbt counts the whole table's
    (kept as table_row_count), and adds the snapshot's newest fetch."""
    if not isinstance(event, dg.Output | dg.AssetMaterialization):
        return event
    unique_id = event.metadata.get("unique_id")
    node = build.manifest["nodes"].get(unique_id.value if unique_id else None)
    query = snapshot_query(node, snapshot) if node else None
    if query is None or build.adapter is None:
        return event
    with chdir(build.project_dir), build.adapter.connection_named(f"snapshot_rows_{node['name']}"):
        _, result = build.adapter.execute(query, fetch=True)
    rows, fetched_at = result[0][0], result[0][1]
    table_rows = event.metadata.get(ROW_COUNT)
    return event.with_metadata(
        {
            **event.metadata,
            ROW_COUNT: dg.MetadataValue.int(int(rows)),
            **({TABLE_ROW_COUNT: table_rows} if table_rows is not None else {}),
            **({ARCHIVE_FETCHED_AT: dg.MetadataValue.text(str(fetched_at))} if fetched_at else {}),
        }
    )


def snapshot_query(node: Mapping[str, Any], snapshot: str) -> str | None:
    meta = node.get("meta", {})
    column = meta.get(SNAPSHOT_COLUMN_META)
    if column is None:
        return None
    fetched_at = meta.get(FETCHED_AT_COLUMN_META)
    newest_fetch = f'max("{fetched_at}")' if fetched_at else "null"
    return (
        f"select count(*), {newest_fetch} from {node['relation_name']} "
        f"where \"{column}\" = '{snapshot}'"
    )


def with_compiled_sql[Event](event: Event, build: DbtCliInvocation) -> Event:
    """The SQL dbt compiled for this build, which the lineage sensor sends to Marquez."""
    if not isinstance(event, dg.Output | dg.AssetMaterialization):
        return event
    unique_id = event.metadata.get("unique_id")
    node = build.manifest["nodes"].get(unique_id.value if unique_id else None)
    if node is None or node["resource_type"] != "model":
        return event
    path = build.target_path / "compiled" / node["package_name"] / node["original_file_path"]
    if not path.exists():
        return event
    sql = dg.MetadataValue.text(path.read_text())
    return event.with_metadata({**event.metadata, COMPILED_SQL: sql})
