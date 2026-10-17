import json
import os
import shutil
import uuid
from collections.abc import Iterator, Mapping
from contextlib import chdir, contextmanager, nullcontext
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb
from dagster._core.definitions.definitions_load_context import (
    DefinitionsLoadContext,
    DefinitionsLoadType,
)
from dagster_dbt import DbtCliInvocation, DbtCliResource, DbtProject, dbt_assets

from leads_dagster_orchestration.automation import build_new_snapshots, rebuild_on_change
from leads_dagster_orchestration.lake_labels import label_run, with_labels
from leads_dagster_orchestration.translators import LakeDbtTranslator
from leads_ingest.settings import PipelineMode

# A source's models are in models/<layer>/<source>/ for each of these layers.
SOURCE_LAYERS = ("raw", "staging")
# The layer whose models keep one slice per snapshot, read from that snapshot's archive folder:
# the only one partitioned, by its source's snapshots. Staging and after are rebuilt whole.
SNAPSHOT_LAYER = "raw"
SHARED_STEP = "transform"
# The snapshot a dbt step builds, which the project's snapshot() macro reads.
SNAPSHOT_VARIABLE = "LEADS_SNAPSHOT"
# dbt meta naming the column of a model that keeps one slice per snapshot (raw tables), and the
# column holding when each row's file was fetched.
SNAPSHOT_COLUMN_META = "snapshot_column"
FETCHED_AT_COLUMN_META = "fetched_at_column"
ROW_COUNT = "dagster/row_count"
TABLE_ROW_COUNT = "table_row_count"
ARCHIVE_FETCHED_AT = "archive_fetched_at"


def build_transform_assets(
    project: DbtProject,
    snapshot_partitions: Mapping[str, dg.PartitionsDefinition],
    main_state_dir: Path,
    labels: Mapping[str, str],
    lake: PipelineMode,
) -> list[dg.AssetsDefinition]:
    """The dbt project as two steps per source, plus one for the models that combine sources.
    A source's raw models are `transform_<source>_raw`, partitioned by the source's snapshots
    (`snapshot_partitions`, by source name), each source on its own cadence; its staging models
    are `transform_<source>_staging`, unpartitioned, as staging is one table rebuilt whole from
    every snapshot's raw. Both are in the pool `transform_<source>`: sources build side by side,
    and nothing writes one source's tables twice at once. The shared step, `transform`
    (intermediate, marts, seeds), exists once the project has such a model, and is rebuilt whole
    too. Each step is one dbt invocation, so models within it still run on dbt's threads. Pools
    are instance-wide, so each lake's are prefixed with it, `live_transform_sodra` and
    `replay_transform_sodra`: neither lake queues behind the other."""
    manifest = json.loads(project.manifest_path.read_text())

    def step(
        name: str,
        select: str,
        *,
        pool: str,
        exclude: str | None = None,
        partitions: dg.PartitionsDefinition | None = None,
    ) -> dg.AssetsDefinition:
        return build_dbt_step(
            project,
            main_state_dir,
            labels,
            lake,
            name=name,
            select=select,
            exclude=exclude,
            pool=pool_name(lake, pool),
            partitions=partitions,
        )

    steps = []
    for source, layers in source_layers(manifest).items():
        pool = f"{SHARED_STEP}_{source}"
        if SNAPSHOT_LAYER in layers and source not in snapshot_partitions:
            raise ValueError(
                f"dbt models in models/{SNAPSHOT_LAYER}/{source}/ have no archived source named "
                f"{source!r}: add it to the code location's sources"
            )
        steps.extend(
            step(
                f"{pool}_{layer}",
                f"path:models/{layer}/{source}",
                pool=pool,
                partitions=snapshot_partitions[source] if layer == SNAPSHOT_LAYER else None,
            )
            for layer in SOURCE_LAYERS
            if layer in layers
        )
    if has_shared_models(manifest):
        steps.append(
            step(
                SHARED_STEP,
                "path:models path:seeds",
                pool=SHARED_STEP,
                exclude=" ".join(f"path:models/{layer}" for layer in SOURCE_LAYERS),
            )
        )
    return steps


def pool_name(lake: PipelineMode, pool: str) -> str:
    return f"{lake.value}_{pool}"


def build_dbt_step(
    project: DbtProject,
    main_state_dir: Path,
    labels: Mapping[str, str],
    lake: PipelineMode,
    *,
    name: str,
    select: str,
    pool: str,
    partitions: dg.PartitionsDefinition | None,
    exclude: str | None = None,
) -> dg.AssetsDefinition:
    """A step partitioned by snapshot builds that snapshot's slice, a backfill one snapshot per
    run, and builds each new snapshot by itself (`build_new_snapshots`); an unpartitioned one
    rebuilds its tables whole, and rebuilds itself when its SQL or what it reads changes
    (`rebuild_on_change`)."""

    @dbt_assets(
        manifest=project.manifest_path,
        project=project,
        select=select,
        exclude=exclude,
        name=name,
        partitions_def=partitions,
        backfill_policy=(
            dg.BackfillPolicy.multi_run(max_partitions_per_run=1) if partitions else None
        ),
        pool=pool,
        dagster_dbt_translator=LakeDbtTranslator(
            lake, automation_condition=build_new_snapshots() if partitions else rebuild_on_change()
        ),
    )
    def dbt_step(context: dg.AssetExecutionContext, dbt: DbtCliResource) -> Iterator[object]:
        run_labels = label_run(context, labels)
        # An unpartitioned step in a partitioned run sees the run's partition key, so whether
        # the step builds a snapshot is its own partitioning's, not the context's.
        snapshot = context.partition_key if partitions else None
        with environment_variable(SNAPSHOT_VARIABLE, snapshot) if snapshot else nullcontext():
            build = dbt.cli(["build"], context=context)
        events = build.stream().fetch_column_metadata(with_column_lineage=False).fetch_row_counts()
        for event in events:
            counted = snapshot_metadata(event, build, snapshot) if snapshot else event
            yield with_labels(counted, run_labels)
        keep_main_state(build.target_path / "manifest.json", main_state_dir)

    return dbt_step


@contextmanager
def environment_variable(name: str, value: str) -> Iterator[None]:
    """Sets a variable for the dbt process that dagster-dbt starts in this block, which takes its
    environment from this process's. The snapshot goes to dbt this way, not as --vars: dbt
    re-parses the whole project whenever --vars differs from the parse Dagster made at startup,
    which no snapshot's run matches. A step runs in a process of its own, or in-process one after
    another, so no other step sees the variable."""
    environment = os.environ  # noqa: TID251 - sets a variable for dbt; reads no config
    previous = environment.get(name)
    environment[name] = value
    try:
        yield
    finally:
        if previous is None:
            del environment[name]
        else:
            environment[name] = previous


def snapshot_metadata[Event](event: Event, build: DbtCliInvocation, snapshot: str) -> Event:
    """dagster-dbt's row count is the whole table's. A model that keeps one slice per snapshot
    (its dbt meta names the column) grows by a slice each snapshot, so its partition's row count
    is the snapshot's rows, and the table's is kept as table_row_count. If its meta also names
    the column of each row's fetch, the newest fetch of the snapshot's files is
    archive_fetched_at: which download the slice was built from."""
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
    """Counts a snapshot's rows of a model whose dbt meta names its snapshot column, and finds
    the newest fetch among them if the meta names that column too."""
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


def set_dbt_environment(variables: Mapping[str, str]) -> None:
    """Sets the values the dbt project reads through env_var() in this process, which the parse
    made when the definitions load and every dbt step pass on to dbt. Set where the definitions
    read the configuration, so a reload moves both to a changed configuration together: dbt's
    partial parse then re-parses once, at the reload, and never in a run, which it would whenever
    a run's values differ from those the saved parse recorded."""
    environment = os.environ  # noqa: TID251 - sets variables for dbt; reads no config
    environment.update(variables)


def source_layers(manifest: Mapping[str, Any]) -> dict[str, set[str]]:
    """Each source's layers that hold a model of it, by source folder name, sorted by source."""
    layers: dict[str, set[str]] = {}
    for node in manifest["nodes"].values():
        fqn = node["fqn"]  # [project, layer, folder..., model]
        if node["resource_type"] != "model" or fqn[1] not in SOURCE_LAYERS:
            continue
        if len(fqn) < 4:
            raise ValueError(
                f"{node['original_file_path']} is not in a source folder: "
                f"put it in models/{fqn[1]}/<source>/"
            )
        layers.setdefault(fqn[2], set()).add(fqn[1])
    return dict(sorted(layers.items()))


def has_shared_models(manifest: Mapping[str, Any]) -> bool:
    """Whether the project has a seed or a model outside the source layers."""
    return any(
        node["resource_type"] == "seed"
        or (node["resource_type"] == "model" and node["fqn"][1] not in SOURCE_LAYERS)
        for node in manifest["nodes"].values()
    )


def keep_main_state(manifest: Path, state_dir: Path) -> None:
    """Keeps the manifest of a successful build as main's state, which dbt on the dev target
    defers to. Replaced in one step, through a file of its own, so neither a dev command nor
    another dbt step finishing at the same time sees half a file."""
    state_dir.mkdir(parents=True, exist_ok=True)
    partial = state_dir / f"manifest.json.{uuid.uuid4().hex}.partial"
    shutil.copyfile(manifest, partial)
    partial.replace(state_dir / "manifest.json")


def ensure_metadata_database(path: Path) -> None:
    """Creates the empty DuckDB file that Dagster's own process opens, read-only, to read each
    built table's columns and row count: DuckDB opens neither memory nor a missing file
    read-only. The tables themselves are in the lake, which the profile attaches."""
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.{uuid.uuid4().hex}.partial")
    duckdb.connect(str(partial)).close()
    partial.replace(path)


def prepare_on_initial_load(project: DbtProject) -> None:
    """Parses the dbt project under `dagster dev` when the code server loads the definitions, at
    startup and on every reload, but not when a run or one of its steps rebuilds them from that
    load. Each of those is a new process, and the parse (`dbt --version`, an in-process adapter
    and `dbt parse`) cost about 11 s in each, twice per run. A run now uses the manifest of the
    load it was launched from, so its assets are the ones that load showed; dbt still runs the
    project's current files."""
    if DefinitionsLoadContext.get().load_type is DefinitionsLoadType.INITIALIZATION:
        project.prepare_if_dev()
