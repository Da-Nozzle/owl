# Decisions

## Dev environment (2026-10-14)

**Compose is for local work only, and lives in `.devcontainer/`.** Its Dockerfile installs system
tools only; `post-create.sh` runs `uv sync`, so dependency changes need no image rebuild.

**The dev image is built by `initializeCommand` in `devcontainer.json`, not by Compose.** For a
Compose service with `build:`, Dev Containers generates a Dockerfile in `/tmp` and builds it through
Compose. Compose v5 always builds with Bake, and Bake refuses to read a Dockerfile outside the
paths it allows ("additional privileges requested") when it builds a single target. A plain
`docker build` has no such check, and a service that only names an image gets no generated
Dockerfile.

**The dev container runs as `dev` with passwordless `sudo`, not as root.** The repo is a bind
mount, so files created as root would be root-owned in the host checkout on Linux.

**Dagster runs inside the dev container (`dagster dev`), not in its own containers.** Dagster
publishes no general-purpose image: `dagster/dagster-k8s` and its siblings exist for the Helm chart.
The webserver and daemon must import our definitions, or reach a code server that does, so any
Dagster container would need our dependencies anyway. Locally the dev container already has them.

**Dagster's Postgres storage uses psycopg2 explicitly.** SQLAlchemy 2.1 made psycopg 3 the default
`postgresql://` driver, but `dagster-postgres` 0.29.26 only installs `psycopg2-binary`.
`.devcontainer/dagster/dagster.yaml` sets `scheme: postgresql+psycopg2`.

**Debian slim, not Alpine, for the Python images.** DuckDB has published no musllinux wheels since
1.3.2, so on Alpine `duckdb==1.5.6` compiles from source, and DuckDB lists no official musl builds
of its extensions (DuckLake, httpfs, postgres). The shared DuckLake attach is the riskiest
connection in the plan, so the dev image uses `python:3.14.7-slim`. Postgres uses
`postgres:18-alpine`. Marquez and s3mock publish no slim or Alpine variants.

**dagster-dbt on Python 3.14 via a uv metadata override.** `dagster-dbt` 0.29.26 declares
`requires-python <3.14`. The cap exists for dbt-core 1.7–1.11, whose mashumaro pin breaks on 3.14;
dbt-core 1.12 (which this plan uses) works. The root `pyproject.toml` restates dagster-dbt's
metadata with `<3.15` under `[[tool.uv.dependency-metadata]]`, pinned to that exact version.
Remove the override once a dagster-dbt release lifts the cap (dagster-io/dagster#33903).

**Postgres 17 with Marquez 0.51.1.** Marquez is tested upstream against Postgres 14 and bundles
Flyway 8.5, which warns about newer servers but migrates. If Marquez fails to start, pin
`postgres:14-alpine`.

**Ruff targets py314.** The code standards say `py313`; the interpreter is 3.14.

**Local dev credentials are plain values in `config/local/`.** The Postgres password and the
s3mock keys protect nothing, so they are committed, and nothing has to generate them before the
first start. Settings that hold real secrets, such as the Google key, are references to a secret
store; locally that store reads git-ignored files, and only a live run asks for them.

**Config errors are built-in exceptions.** Nothing catches a config error by type: a missing key
raises `KeyError`, a wrong type `TypeError`, a malformed table `ValueError`.

**The configuration service lives in `services/`, beside the services that use it.** The
orchestration service is named after Dagster (`leads-dagster-orchestration`), because its code is
Dagster definitions and launchers; another orchestrator would be a new service.

**One file may read `os.environ`.** `leads_dagster_orchestration/entrypoints/tool_launchers.py` passes the
parent environment through to Dagster and dbt plus the variables it sets. It is exempted from
`TID251` in `pyproject.toml`; no code reads configuration from the environment. The dbt step in
`assets/transform.py` also sets variables for dbt (the run month, and the configuration's values
when the definitions load), each line marked `noqa: TID251`.

**Duplicated names in tool-owned config.** Database names (`.devcontainer/postgres/initdb`), the
bucket name (`s3mock` in Compose) and the dev Postgres password (Compose) repeat values from
`config/local/storage.toml`, because Postgres and s3mock are configured before any of our code
runs.

## dlt and dbt on one DuckLake (2026-10-14; dlt removed 2026-10-18, see dlt removed)

**Both tools name the lake's metadata schema from config (`lake.metadata_schema`).** dlt attaches
DuckLake with `METADATA_SCHEMA` set to its lake name, while dbt-duckdb's attach sets none and lands
in Postgres's default schema. Left alone, the two would write two separate lakes in the same
`ducklake_catalog` database. The dbt profile passes the same value as an attach option.

**dlt gets its destination as an object, built from config.** The lake's settings (Postgres
catalog, S3 storage) are a `LakeSettings` dataclass in `libs/lake`, loaded once from config.
`leads_ingest` builds dlt's DuckLake credentials from it, so there is no `.dlt/` folder and no
environment variable, and dbt's environment variables come from the same object, so the two cannot
drift. It is a library rather than part of ingest because dbt, and later the analyst app, read the
same lake.

**One graph through asset keys.** dlt resources are keyed `raw/raw_<source>__<table>`; a dbt
source is keyed the same way, and a dbt model as `<layer>/<model>`. A `generate_schema_name` macro makes
`+schema: staging` build into `staging`, not dbt's default `main_staging`.

**`lake_probe` is gone.** A fixed-row source proved dlt and dbt share the lake until the Sodra
source replaced it. Sodra no longer loads through dlt (see below), so nothing proves the
shared lake now; the first dlt API source will.

**One raw schema per source (`raw_<source>`), not one shared `raw`.** dlt keeps its bookkeeping
tables (`_dlt_loads`, `_dlt_version`, `_dlt_pipeline_state`) once per schema. Three pipelines
loading into a fresh shared schema at the same time failed in half the attempts (6 of 12) with a
DuckLake transaction conflict on creating `_dlt_pipeline_state`. CI starts from a fresh lake, so it
would have hit this on every pull request. A schema per source shares nothing, and also keeps two
sources' same-named tables apart. Asset keys stay `raw/raw_<source>__<table>`.

**Sources load in parallel; each lake writer is a Dagster pool with a limit of 1.** Concurrent
appends and merges into one existing table also succeeded and stayed correct in a short test, but
that is a test, not a guarantee. The pools (set by `concurrency.pools.default_limit: 1`) stop two
runs writing the same tables at once, for example a backfill over months, while different sources
still load side by side. A run that dies while it holds a slot keeps it, and every later step in
that pool queues behind it: free it with the `freeConcurrencySlotsForRun` mutation or from the
run's page in the UI (since 2026-10-17 run monitoring does it within the hour, see Run
monitoring). Reloading definitions while a run starts can cause that, if the reload
renames an asset the run selected. The dlt-era pools `raw_lake_probe` and `raw_sodra` were deleted
from the instance (2026-10-15).

**dbt runs as one step per source, each in its own pool** (2026-10-15; since 2026-10-17 two steps
per source, raw and staging, in that one pool: see Runs and lakes). A source's raw and
staging models (`models/raw/<source>/`, `models/staging/<source>/`) are one `@dbt_assets` step,
`transform_<source>`, in a pool of the same name; models that combine sources (intermediate,
marts, seeds) will be one more step, `transform`, in the `transform` pool, created once the first
such model exists. Before, the whole project was one step in `transform`, so every source would
have queued behind every other. The steps are found from the manifest, so a new source's folders
make a new step and pool with no Python change; a raw or staging model outside a source folder is
rejected when the definitions load. A source's step and the shared step meet only through asset
dependencies, which Dagster orders. dbt's own `threads: 4` still runs independent models inside a
step. Each step is a separate dbt invocation, which costs a few seconds of dbt start-up per source.

**dbt runs under Dagster keep DuckDB in memory.** The steps could not run side by side while
every dbt run opened `target/dagster.duckdb` read-write: of three dbt processes started at once on
one file, two failed with DuckDB's "Could not set lock on file". dagster-dbt marks the processes
it starts with `DAGSTER_DBT_CLI`, and the profile keeps those in memory; only Dagster's own
process opens the file, read-only, for metadata (below), and DuckDB lets several processes do
that at once.

**A source's step stays at 1.** Two run months of one source at once would still conflict. Raw
now writes only its own snapshot, but as a `delete+insert`, and DuckLake aborts a transaction that
deletes from a table another one inserted into since (or the reverse): in a test, one of two
concurrent builds failed with "attempting to insert into table ... but another transaction has
deleted from it". Staging is one table that every run month rebuilds, so even when both commit,
the result depends on which finished last (2026-10-17: staging is now unpartitioned and rebuilt
once per backfill, see Runs and lakes). A build takes about a minute, so a 13-month backfill of
one source, run one month at a time, is acceptable. Raising the limit would need raw to append only
(a rerun adding a newer load that staging prefers, never deleting) and staging that does not
rebuild one shared table per run month.

## dbt dev target (2026-10-15)

**Models are iterated on with `leads-dbt-dev`, not through Dagster.** Dagster runs dbt by run
month, one step per source in that source's pool, so changing a staging model meant re-running
raw, queueing behind the pipeline and picking a partition. `leads-dbt-dev` runs dbt on
the profile's `dev` target: models build into a DuckDB file, `target/dev/dev.duckdb`, with the lake
attached `READ_ONLY` as `lake`. A model that is not selected resolves to the lake's table through
dbt's defer (`DBT_DEFER`, `DBT_STATE`), so sources are loaded once, by the pipeline, and staging and
later models are rebuilt as often as needed. Building staging alone takes about 1.5 s of dbt work,
7 s with dbt's startup and the lake attach.

**The dev copy is a local DuckDB file, not a second DuckLake.** It writes nothing to Postgres or
s3mock, is faster to build into, and is reset by deleting `target/dev/`. The cost: DuckLake's own
behaviour, such as transaction conflicts, does not show in dev, and the file is locked while dbt
runs, so it cannot be read from another process at the same time.

**The state dev defers to is the manifest of main's last Dagster build.** After each successful
build, each dbt step copies its invocation's `manifest.json` (the whole project's, whatever the
step selected) to `target/main/` (`target/<lake>/main/` since 2026-10-17), replaced in one step through a file of its own, so two steps
finishing together cannot interleave. That names the tables as they are in the lake, and makes `state:modified+` select what
changed since that build. If no build has stored one yet, `leads-dbt-dev` parses the `local`
target into `target/main/` first; it does not parse every time, which takes about 5.5 s. In
production the plan stores these artifacts with each run (`audit`), and CI defers to them.

**Dev and main keep separate target paths** (`target/dev/`, `target/` and `target/main/`; since
2026-10-17 `target/<lake>/dev/`, `target/<lake>/` and `target/<lake>/main/`, see Runs and lakes). dbt's
partial parse is invalidated whenever the profile changes, which it does between Dagster
(`LEADS_DBT_DUCKDB_PATH` set) and `leads-dbt`; a third target sharing the folder would make every
switch a full parse.

**A model built in dev shadows the lake's.** dbt's defer only substitutes a relation that is
missing from the dev target, so after `raw_sodra__monthly` is built in dev, staging reads the dev
copy. `--favor-state` reads the lake's again. The `dev` output reuses `local` through YAML merge
keys and overrides only the path, the database name and the read-only attach.

**The launchers put this environment's scripts first on `PATH`.** The dbt VS Code extension
installs dbt Fusion as `~/.local/bin/dbt` and puts that directory first on `PATH` in `.bashrc`,
so a plain `dbt`, and dagster-dbt's `DbtCliResource`, ran Fusion 2.0 instead of dbt-core 1.12.
Fusion rejects this profile (an extension given as `{name, repo}`, needed for the community
`zipfs`), and the project is built on dbt-core and dbt-duckdb. Fusion stays the extension's
language server; `dbtf` still runs it by hand.

## Source contracts (2026-10-14)

**No generated specs, no generated clients.** Each source's contract is kept in the form its owner
publishes it, as the single source of truth: DSA tables for data.gov.lt, the UAPI OpenAPI from
`ivpk/uapi`, Google's Discovery document for Places. Nothing is converted into another format
(Discovery or DSA into OpenAPI): a converted copy is a second source of truth, and it describes
the converter's assumptions as well as the API. Sources are written by hand as
`dlt.sources.rest_api` configs. `dlt-init-openapi`, the only generator that targets dlt, is
unmaintained (last release 0.1.0, May 2024, requires Python < 3.13). Where a source publishes no
spec (Oxylabs, Sodra's files), its docs and a probe's observed output are the contract.

## Sodra source (2026-10-14, parsing moved to dbt 2026-10-15)

**Every source goes through a raw archive.** (Since 2026-10-17 a run month is a snapshot, see
Snapshots define the partitions.) Live runs download into
`lake.archive_path/<source>/<run month>/` (with a `.meta.json` sidecar: URL, fetch time,
`Last-Modified`, size, SHA-256), and everything downstream reads the archive. Replay runs read
`pipeline.replay_path` instead and download nothing, so live and replay share one parser. The
committed sample in `replay/` uses the same layout. A failed live run still archives what it
downloaded. dbt finds the archive through `LEADS_ARCHIVE_PATH`, which `raw_archive_path` derives
from the same settings as the Python archive.

**The archive is the record; raw is rebuilt from it** (2026-10-15). Sodra rewrites its files in
place, so the archive holds the only copy of a version Sodra has since replaced, and it keeps the
bytes, which a parse that turns out wrong can be redone from. Raw holds nothing the archive does
not: built from nothing (first build or `--full-refresh`), `sodra_monthly_files` reads every
archived run month (`archived_run_months()` gives `*`), and otherwise only its run month. Before,
a full refresh read only the run month's folder and dropped every other run's rows. The archive
is never deleted; raw may be. A run month's files are stored again when unchanged since the last
run (about 25 MB zipped a month): that copy is the record of what the run saw, and the archive
asset's `files` metadata shows each file's SHA-256, so equal copies are visible.

**Python fetches files; dbt parses them.** Downloading and archiving are transport, so they stay in
Python (`sources/sodra/files.py`, the `archive/sodra` asset). Which column is which, what type it
has and which months a run loads are domain knowledge, so they are SQL, where someone who writes
the domain rules but not Python can read and change them. Before this, a Sodra column was named
in three places (a Python layout, a pyarrow window filter and a second rename in staging); now
each published header is mapped to its name and type once, in `stg_sodra__monthly`. dlt did
little for a flat file beyond writing it, so Sodra no longer uses dlt; dlt stays the tool for
API sources, where paging, rate limits, nested JSON and incremental state are the hard part.

**DuckDB reads the zips in place through the `zipfs` community extension.** The archive keeps
Sodra's files byte for byte; dbt reads `zip://<archive>/sodra/<run month>/monthly-*.csv.zip/*.csv`,
locally and on S3. `zipfs` is a community extension: it installs from DuckDB's community
repository at first use (CI needs network for it) and has no version pin beyond DuckDB's own. The
Python step rejects a zip that holds more than one CSV, since `*.csv` would read both.

**Raw keeps every published column, as text, under its published header.** `raw_sodra.monthly`
is `read_csv(..., all_varchar = true, union_by_name = true)` of the run's files, plus
`_source_file`, `_snapshot_month` (from the archive path) and the sidecar's `_file_url`,
`_file_fetched_at`, `_file_last_modified` and `_file_sha256`. A column Sodra adds is added to raw
(`on_schema_change: append_new_columns`), so the columns not used yet are visible and one line in
staging away. The 2025 file has no `ecoActCodeStr`; its rows get null. Types are given once, in
staging, which is a table so that every value is cast when it is built: a value that does not parse
fails staging, and a renamed or dropped header fails it too. The bad row is then in raw, as text,
and nothing downstream of staging builds. This replaces typed raw through declared `CsvLayout`s,
whose header check (`read_csv(columns = ...)` assigns names by position and checks no header, so it
had to be done by hand) and dlt contract existed to guard dlt's first load.

**A column may be added to Sodra's file, not dropped.** Reading by name would turn a column dropped
from the newest file into nulls for the new months. `tests/sodra_no_column_dropped.sql` fails when
a column an older file fills has no values in the newest. "Every column has values" would be too
strict: `avgWage2` is empty in the whole Vilnius sample.

**Raw holds Sodra's yearly files whole, as each run downloaded them** (2026-10-15). Sodra has no
rolling window: it publishes one file per year and rewrites it in place, the current year's
growing by a month each time Sodra publishes, a closed year's frozen once its December is in.
The 13 months are ours (the plan's "13 months on the first run"), so they are not applied in raw:
`sodra.history_months` only picks which years' files a run downloads (the years holding the
months M−1 back to M−13, so two files, about 25 MB zipped, 2.1 M rows). `raw_sodra.monthly` is a
dbt incremental `delete+insert` keyed on `_snapshot_month`, so a run replaces only its own
downloads, and a file Sodra did not change is there again, as that run's download of it: raw
records what each run saw, and no hash decides what is loaded. Before, raw held each run's 13-month
cut, which hid the files' shape and applied our window before the record of what was published.
Raw was keyed on `"Mėnuo (month)"` before that, so a rerun of an older run month put its older
download back over newer revisions.

**Staging takes each year's file from its newest download.** A year's newest download is its
current version, so `stg_sodra__monthly` keeps the rows of the newest `_snapshot_month` per file
name, every month the files hold, with no window; the window belongs to whatever uses it.
`file_published_at` (the file's Last-Modified) is what a row is as of. Whole files are picked, not
rows, so an employer a revision drops is dropped from staging too. The unit test on
`stg_sodra__monthly` covers that, a year a later run no longer downloads, and downloads loaded out
of order.

**A past run month does not download** (2026-10-15;superseded 2026-10-17, see Snapshots define the partitions) A download is the version of the day it is
made, whichever run month it is for, so a live backfill of a passed run month archived October's
files as May's, and staging took them for May's version. In live mode the archive asset fails for
any run month but the current one (`require_current_run_month`, from the partitions' own
schedule) and says how to rebuild it from what it archived: materialize only the assets
downstream of the archive (since 2026-10-17, the `rebuild_from_archive` job).

**Replay builds into a lake of its own** (2026-10-15). The sample is filtered, not what Sodra
published, yet its run months look like live ones; sharing the lake, its 2026-09 was the newest
download for staging and won over live rows. In replay mode the catalog schema and data path come
from `lake.replay` (`ducklake_replay`, `s3://leads-lake/replay/`), on the same Postgres and bucket.
Dagster's history is still shared (same asset keys and partitions), so the asset overview shows
whichever mode ran last; every run is tagged `leads/mode` and `leads/lake` (the catalog schema),
and every materialization carries the same two as metadata. A separate Dagster database per mode
would split the history too, but Dagster's storage is fixed when `leads-dagster-dev` starts, so a
mode switch would need a restart, not a reload. (Superseded 2026-10-17: replay is a code location
of its own with keys of its own, so the history is split and nothing switches. See Runs and
lakes.)

**An unpublished month stops the run before anything loads.** The run's files are read by an
ephemeral model, `sodra_monthly_files`, which both checks and `raw_sodra.monthly` read.
`dbt build` runs a singular test before the models downstream of what it tests, so when
`tests/sodra_previous_month_published.sql` fails, raw is skipped and left as it was. The checks
gate inside dbt, not as Dagster asset checks: they test the ephemeral model, which is no asset, so
Dagster logs them as observations. They are a safety net for a late publication: normally the
run month's partition does not exist yet (below).

**A run month starts on the 21st, 06:00 Vilnius time** (`pipeline.run_day`, `run_hour`,
`timezone`; 2026-10-15;superseded 2026-10-17, see Snapshots define the partitions) Sodra publishes the previous month by the 20th, so before then a run could
only fail its published-month check. The run-month partitions split on that day and hour
(`day_offset`, `hour_offset`), so partition M appears then and cannot be launched earlier; their
cron, `0 6 21 * *`, is the plan's monthly schedule. Partition keys stay `YYYY-MM`. Dagster splits
monthly partitions on days 1 to 28 only, so the setting is checked against that.

**Replay partitions are the months the replay sample holds** (2026-10-15;superseded 2026-10-17, see Snapshots define the partitions) The sample is a fixed
download (`replay/sodra/2026-09/`), but live partitions start at `pipeline.first_month` and roll
forward, so replay offered months it had no files for and failed on them: `2026-08` at once, and
`2026-10` from its run day. In replay mode the partitions run from the sample's first to its last
month (`RawArchive.months`), with no run day: the sample is already published, so its months exist
whatever the date. `first_month`, `run_day` and `run_hour` apply to live runs only. Adding a month
to the sample adds its partition; a gap is rejected when the definitions load, since a monthly
partitions definition is a range. Only Sodra's months count for now; when a second source is
sampled, take the months every source holds.

**dbt is partitioned by run month** (raw only since 2026-10-17, by its source's snapshots;
staging and after are rebuilt whole, see Runs and lakes and Snapshots define the partitions). Raw models read the archive folder of the month they are built
for, so the dbt assets share the run-month partitions and Dagster sets `LEADS_RUN_MONTH=YYYY-MM`
for each step's dbt process. Run by hand on the lake (`leads-dbt`), dbt needs the same variable,
so nothing is written to the lake for a month nobody chose. The month is an environment variable,
not `--vars`, and `run_month()` reads it only once dbt executes (2026-10-15): dbt's partial parse
is discarded whenever `--vars` differs from the parse it was saved by, and an `env_var()` read
while parsing re-parses the files that read it when its value changes. Dagster parses the project
once at startup, with no month, so with `--vars` every step re-parsed the whole project, about 5 s. On the dev target
(`leads-dbt-dev`) the month defaults to the newest run month in the archive, found by the
`run_month()` macro with DuckDB's `glob()` only when a model that reads the archive is compiled:
a changed raw model is in `state:modified+`, and iterating on it should not need the variable.

**The definitions set dbt's variables, not `leads-dagster-dev`** (2026-10-15). dbt reads the lake
and archive settings through `env_var()`, and dagster-dbt passes its own process's
environment to every dbt it starts. When the launcher set them, they were fixed at startup, while
**Reload definitions** re-read the configuration: switched to live mode, the archive asset
downloaded to S3 and dbt still read `replay/`. Now `pipeline_definitions()` sets them from the
configuration it loads, before Dagster's startup parse, so the parse, the definitions and every
run agree. Building them per run instead would also re-parse in every run after a configuration
change (about 5 s each), since the saved parse would hold the old values; set at load, a changed
value is re-parsed once, at the reload, and only in the files that read it.

**Column types and row counts reach Dagster from the lake, not from YAML.** After each build the
dbt step calls `fetch_column_metadata()` and `fetch_row_counts()`, which read every built table's
columns and size back, so the types are written once, in the SQL. dagster-dbt reads them in
Dagster's own process, through the same profile opened read-only, which it cannot do for
`:memory:` (it turns the path into `<project>/:memory:`), so there that database is a file,
`target/dagster.duckdb` (`LEADS_DBT_DUCKDB_PATH`), which the definitions create empty if it is
missing. It holds nothing; the tables are in the lake, which the profile attaches. dbt's own runs,
under Dagster or `leads-dbt`, keep it in memory (above). dbt reports DuckDB's `VARCHAR` as `character varying(256)`; the
column has no length limit.

**A raw table's row count is its run month's** (2026-10-15). dagster-dbt counts the whole table,
which for raw grows by a run's downloads each run, so every partition showed the running total. A
model whose dbt meta names a `run_month_column` (raw) gets `dagster/row_count` counted for the
step's run month instead, and the whole table's as `table_row_count`. Staging is rebuilt whole
each run, so its whole-table count is the right one.

**Every asset is keyed `<layer>/<layer prefix>_<source>__<name>`** (2026-10-15; since 2026-10-17
`<lake>/` comes first and each lake is a group, see Runs and lakes): `archive/archive_sodra`,
`raw/raw_sodra__monthly`, `staging/stg_sodra__monthly`, later `int_`, `fct_` and `dim_`. The
Dagster UI labels a node with only the last part of its key and has no separate display name, so
that part says both layer and source; dbt model names, unique across the project, already do. A
dbt model in a `raw_<source>` schema is in the raw layer, and the dbt source `archive` is keyed as
the Python asset that fills it, so the graph runs archive → raw → staging. Each asset is in the
Dagster group of its layer, the first part of its key.

## dlt (2026-10-15; superseded 2026-10-18, see dlt removed)

**dlt stays for now, but may not earn its place.** Since Sodra moved to dbt, no source loads
through dlt. What is left is its `requests` helper (retries with backoff) in `HttpDownloader`, its
fsspec configuration for the archive, and the DuckLake destination and dlt translator, which
nothing uses yet. The REST sources still to come (Google Places, JAR) are full monthly snapshots,
so dlt's incremental state is not needed; its paginators and nested-JSON normalisation are what it
would add. If those sources also archive their responses and dbt parses them with `read_json`,
dlt shrinks to an HTTP helper and can go. Decide when the Google source is written. Changing is
cheap by design: sources depend only on ports (`Downloader`, `RawArchive`), so replacing dlt means
new adapters in `adapters/`, and the destination and translator can be deleted with their tests.

## Ingest layout (2026-10-14)

**`leads_ingest` is split into ports, adapters and sources.** `ports/` holds only interfaces
(`Downloader`, `RawArchive`) and the values they exchange (`Download`). `adapters/` implements
them (`http/`, `archive/` on fsspec, `s3/`) and holds the DuckLake destination for dlt sources
(removed 2026-10-18 with dlt).
`sources/<name>/` is per-source code that fetches into the archive or, for API sources, dlt code;
it depends on ports only. The composition root (`entrypoints/definitions.py`) picks the adapters:
S3 archive and HTTP downloader for live runs, the local replay directory and no downloader for
replay.

## Run start-up (2026-10-15)

**dbt is parsed when the code server loads the definitions, not in every run.** dagster-dbt's
`prepare_if_dev()` parses the project whenever `DAGSTER_IS_DEV_CLI` is set, and `dagster dev`
sets it for the code server and every process the code server starts. So a run's own process and
its step's process each ran `dbt --version`, built a dbt adapter and ran `dbt parse`: about 11 s
each, which with Python's start-up made 25 s and 16 s of a one-minute run. `prepare_on_initial_load()`
prepares only on Dagster's `INITIALIZATION` load (startup and **Reload definitions**). A run
rebuilds the definitions as `RECONSTRUCTION` and reads the manifest that load wrote, so its assets
are the ones the UI showed when it was launched; dbt still runs the project's current files. The
load type is not public API in Dagster 1.13 (`dagster._core.definitions.definitions_load_context`):
recheck it when upgrading.

**A run whose steps form a chain runs them in its own process.** The default multiprocess
executor starts a process per step, which imports Dagster and dbt and loads the definitions again
(about 5 s), even for a run of one step, as every run of a backfill is. `sequential_in_process_executor`
runs the steps in the run's process when no two of them could run at once, and hands the run to
the multiprocess executor, with the same configuration, when two could, as when two sources load in
one run. It is built from Dagster's own executors, but the types it names are not public API.

**dagster-dbt does not run `dbt --version`.** It runs it once per run to tell dbt Fusion (2.x)
from dbt-core, which took 3.5 s: dbt's start-up and a check of PyPI for a newer release.
`DbtCoreCliResource` reads the version of the installed `dbt-core` package instead, and runs that
package's `dbt` (this environment's scripts directory) rather than the first on `PATH`, so the
version it reports is the dbt that runs. It overrides `_cli_version`, a private member of
`DbtCliResource`: recheck it when upgrading dagster-dbt. The code server's parse at load still
runs `dbt --version`, through dagster-dbt's own resource.

**Measured on the replay sample** (`transform_sodra`, run month 2026-09): 58.9 s per run before,
30.9 s with dbt parsed once, 24.5 s with the chain in-process, and 18.5 to 19.6 s without
`dbt --version`. The first run after a reload is slower (about 25 s), all of it in the column and
row-count fetch after dbt. What is left: the run's process starting (5.5 s), dagster-dbt's
adapter before dbt starts (2.5 s), dbt itself (7.5 s, mostly its own start-up on the sample), and
that fetch (2.5 to 3 s, mostly dbt importing `agate` on its first query in this process). On live data dbt's build is
about 13 s, 8 s of it reading the archive's zips: the two checks and raw each read the ephemeral
`sodra_monthly_files` (see the Sodra source).

## Runs and lakes (2026-10-17)

**Live and replay are two code locations, not a mode in config.** `pipeline.mode` switched what
the same asset keys meant (which lake, which archive, which partitions, whether the archive
fetched), switching took a config edit and **Reload definitions**, and Dagster's history mixed
both lakes under one key. Now `.devcontainer/dagster/workspace.yaml` loads
`entrypoints/live.py` and `entrypoints/replay.py`, which build the same graph through
`pipeline_definitions(mode)`: the mode is which module loads the code, and both are loaded at
once. Each location runs in a process of its own, which is what lets each hold its own lake's
dbt variables (`set_dbt_environment` sets them process-wide). Production deploys `live` only.

**Every asset key starts with its lake, and each lake is a group.** Keys are
`<lake>/<layer>/<name>` (`live/raw/raw_sodra__monthly`, `replay/staging/stg_sodra__monthly`).
Dagster keeps one node per asset key across code locations (it warns about a duplicate and keeps
one), so with the same keys the two lakes would share one history; with the lake first, the asset
catalog shows one folder per lake. Live's keys first had no prefix, which put `replay` beside
`archive`, `raw` and `staging` in the catalog; they took one when the history was wiped
(below). Each lake is one Dagster group, `live` or `replay`, which the lineage view draws as one
box with archive → raw → staging left to right inside. Grouped by layer, as before, Dagster
merged each layer's group across the two locations, so each layer's box held both lakes' nodes
and nothing said which was which. The layer is still in each node's name (`archive_`, `raw_`,
`stg_`), and Dagster has no display name to say more. Pools are instance-wide, so they are
prefixed with the lake too (`live_transform_sodra`, `replay_transform_sodra`) and neither lake
queues behind the other.

**Each location keeps dbt's artifacts in `target/<lake>/`.** The two code servers parse the same
project with each lake's values; sharing `target/` each would overwrite the other's manifest and
partial parse, and re-parse. dagster-dbt takes each run's folder, and the partial parse it copies
into it, from `DBT_TARGET_PATH`, not from `DbtProject`, so the location sets both. Its
`dagster.duckdb`, its main state (`main/`) and the dev copy that reads its lake (`dev/`) are
there too. `leads-dbt` keeps `target/`.

**Replay's archive is an external asset.** The sample is read, never fetched into; as an
`AssetSpec` it stays in the graph and nothing can select or materialize it.
`archive_yearly_files` always downloads now, and its replay branch is gone.

**Two jobs, so a rebuild is never a hand-picked selection** (superseded 2026-10-17, see Snapshots define the partitions). `monthly_ingest` (live) is the
archive and everything after it, on a schedule built from the run-month partitions
(`0 6 21 * *`, Vilnius), which picks the run month starting at its tick. It runs unless turned
off, as it is the live pipeline. `rebuild_from_archive` is everything after the archive, so it
cannot fetch; it is replay's only job. The past-month guard stays for runs started from the asset
graph, and its message names the rebuild job.

**A run is labelled by what it did (`leads/trigger`)** (simplified the same day, see Snapshots
define the partitions). `schedule` (Dagster's schedule tag),
`automation` (its automation tag), `fetch` (an archive was materialized earlier in the run, read
from the run's events) or `rebuild`. The job name would mislabel a run started from the asset
graph. The label is a run tag and materialization metadata, as `leads/mode` is. A re-execution
keeps its parent's tags, so re-running a failed scheduled run is still `schedule`. Dagster's
lineage view cannot show a node by which job last wrote it; the label is one click away on the
node, and the runs list filters on it.

**The local history and lakes were reset when the keys changed** (2026-10-17). Every run,
backfill and asset event in the local Dagster instance came from before the split, mostly replay
runs under the keys live took over, so a failed archive backfill of 2026-08 from 2026-10-15
showed as live's. They were deleted, with the pools `transform` and `transform_sodra`. Both lakes
were emptied too (their catalog schemas dropped, their files deleted), since their tables were
built by the old steps; the archive was kept, as it is what the lakes are rebuilt from.

**Raw run months (snapshots, since) carry `archive_fetched_at`:** the newest `_file_fetched_at` of the run month's
rows (raw's meta names the column, `fetched_at_column`), so a slice shows which download it was
built from, whether its run fetched it or a rebuild read it.

**Staging and later are unpartitioned.** Staging is one table rebuilt whole every run, yet it was
partitioned, so its per-run-month status said nothing about the table: after a rebuild of
2026-08, its 2026-09 partition still showed the scheduled run while the table was the rebuild's.
Unpartitioned, its latest materialization is the table's. A source is now two dbt steps,
`transform_<source>_raw` (partitioned) and `transform_<source>_staging`, both in the pool
`transform_<source>`; the shared step is unpartitioned too. A rebuild of N run months is N raw
runs and one staging run (checked on two months), where it was N of each. The cost is a second
dbt invocation per run, a few seconds of start-up. A partitioned job may hold unpartitioned
assets: Dagster requires only its partitioned ones to share a definition, and one backfill
policy, so the archive asset has the raw steps' (one run month per run).

**Staging and later rebuild themselves (`rebuild_on_change`)** (now `eager()` plus code changes,
see Snapshots define the partitions). The condition is
`(any_deps_updated | code_version_changed).since_last_handled & ~any_deps_in_progress &
~in_progress`. dagster-dbt's code version is a hash of a model's SQL, so a deploy that changes a
model rebuilds it, and what follows it rebuilds once it has: editing SQL, checking it with
`leads-dbt-diff` and merging is the whole job. `any_deps_updated` ignores a parent built by a run
that also built the asset, so the jobs' runs are not followed by a second build, and a backfill
in progress holds it back (checked: none ran during a two-month rebuild). It is `eager()` without
its missing-data parts: `~any_deps_missing` would block staging for good, since it depends on
every raw run month and a passed one that never ran can no longer fetch. So a model that is
missing is not built by it; a new model waits for the next run of a job, or a launch by hand. Its
sensor is ours and runs unless turned off: Dagster's default automation sensor starts stopped.
Raw has no condition: rebuilding it rereads the archive, which stays a decision.

**`leads-dbt` needs `--lake`; `leads-dbt-dev` and `leads-dbt-diff` default to replay.** dbt run by
hand writes to the lake it is given, so nothing picks one for it. The dev tools only read a lake,
and replay is the one every checkout can build. Choosing a lake for a tool is not choosing what
the pipeline does, which config no longer does either.

**`leads-dbt-diff` compares dev's tables with the lake's.** What a model change did to the output
is the question review needs answered. The `diff` macro (`macros/diff.sql`) runs on the dev
target, which attaches the lake as `lake`. It reports rows on one side only and columns added,
dropped or retyped; with the model's meta `key` it counts each column's changed values in the
matched rows, with examples, and without one compares whole rows (`except all`). Values are
compared as text, so a retyped column also counts as changed. The launcher passes the models the
last dev command built (its `run_results.json`) and runs the macro with `--no-write-json`, so
that file is still the build's for the next diff. It compares built tables, not SQL: rebuild
after editing. It is a macro rather than Python so that it reuses the dev target's connection,
lake attach and graph, which a Python tool would have to set up again from the profile.

## Snapshots define the partitions (2026-10-17)

**A source's partitions are its complete snapshots.** A snapshot is what one fetch of a source
captured, and it can only be captured during its own period. Time-based partitions say the
opposite, that any period can be built at any time, so Dagster offered run months that were never
fetched (2026-08) and let a passed one be "fetched", and a guard failed the run instead. Now each
source has a dynamic partitions definition per lake (`<lake>_<source>_snapshots`) that holds
exactly the snapshots its archive has complete, added by the archive index (below). Nothing is
offered that has nothing to build from: a missing partition is a snapshot not built yet, which
always can be. The past-month guard (`require_current_run_month`), the run-month partitions and
`pipeline.first_month`, `run_day` and `run_hour` are gone.

**The fetch takes no partition; the clock picks its snapshot.** Each source has a cadence
(`SnapshotCadence`): a cron for when it is fetched and a key format. A snapshot is keyed by the
period from one tick to the next: Sodra's `sodra.snapshot_schedule` is `0 6 21 * *` and its key
`YYYY-MM` (a source constant, as its dbt models read it), so a fetch on 17 October is snapshot
`2026-09` and one on 21 October at 06:00 is `2026-10`. A source fetched daily would be `YYYY-MM-DD`.
Sources are expected to differ (publication dates, API call limits), which is why the cadence is
per source and why sources do not share one set of months. A second fetch in a period replaces
that period's snapshot. The fetch is an unpartitioned asset, `live/fetch/fetch_<source>`, upstream
of the archive, in a pool of its own so two fetches of a source never write one snapshot at once.

**A snapshot is complete once its manifest is written, last.** The fetch saves the files (with
their sidecars as before), then `RawArchive.complete()` writes
`<archive>/<source>/_snapshots/<snapshot>.json` (completed at, and each file's fetch). A fetch that
fails midway, as one of hundreds of API pages may, leaves files and no manifest, and nothing reads
them. The manifests are one flat folder per source, so finding the complete snapshots is one
listing, never a walk through every response file a daily source has fetched. A manifest's
version (its S3 ETag; locally a hash of it, as a directory's modification times are too coarse to
tell two writes apart) tells a snapshot completed again from one already seen. The archive port
is keyed by snapshot (a string) instead of `Month`; `load` and `exists`, which nothing used since
replay stopped fetching from the archive, are gone. The existing snapshots (the replay sample's and
the local live archive's 2026-09) were given manifests built from their sidecars.

**The archive index (`archive_index` sensor) keeps the partitions equal to the archive.** Every
minute, per source: a partition for each complete snapshot, a materialization of the source's
archive (with the manifest as metadata) for each new or completed-again snapshot, and the
partition deleted for each snapshot gone from the archive. It matches partitions against those
Dagster holds, not its own last look, so partitions lost with the instance come back. It is the
same in both lakes: live's snapshots come from its fetches, replay's from the committed sample,
where a month added with its manifest becomes a partition with no reload. The archive is an
external asset in both lakes, so no job or button can write a snapshot. A deleted snapshot's rows
stay in raw until raw is rebuilt from nothing; Google's storage limits will need them removed too.

**New snapshots build themselves.** Raw's condition (`build_new_snapshots`) is Dagster's
`eager()`: a partition the index adds is newly missing, and one it reports again is an updated
dependency, so both are built. `eager()` alone missed a snapshot registered before the
automation's first evaluation, as the replay sample's was at startup, 0.7 s ahead of it, and so
never built: it builds only what goes missing after its first look, and its `since_last_handled`
resets on that look. So raw also builds, on its first evaluation only, whatever is missing then
(`initial_evaluation & missing`, beside `since_last_handled`). That is the asset's first evaluation
ever, not each reload, so it covers a first deploy and a wiped instance, while a snapshot added
later is newly missing anyway; a build that fails is not retried every tick. Staging and after keep
`rebuild_on_change`, now `eager()` plus a change to the model's SQL: the missing-data guards left
out before are back, as no partition is missing for good any more, so staging waits until every
snapshot's raw is built. Staging has no catch-up: on a lake with no snapshot nothing upstream is
missing, and it would build from no raw; raw's first build starts it. One sensor,
`transform_automation`, evaluates both, running by default. A change to raw's SQL is still not
acted on: rebuilding every snapshot is `rebuild_<source>_from_archive`, a choice. The cost: Sodra's
month is a chain of runs, the scheduled fetch, then raw, then staging, each started by a sensor
tick, and it depends on two sensors staying on. On an isolated lake, with the fetch served
locally, the chain took 76 s: raw started 30 s after the fetch, staging 30 s after raw.

**Two jobs per source.** `fetch_<source>` (live) with its schedule `fetch_<source>_schedule` (on
by default), and `rebuild_<source>_from_archive`, everything after the source's archive for the
snapshots chosen, which offers only snapshots that exist. A job's partitioned assets need one
partitions definition, so the rebuild jobs are per source. `monthly_ingest` and
`rebuild_from_archive` are gone.

**Labels follow.** `leads/trigger` is `schedule` or `fetch` on a fetch, `automation` on what
Dagster built by itself (a new snapshot or a SQL change), and `rebuild` on a build started by hand.
A build no longer looks for an archive fetched earlier in its run, since no run both fetches and
builds. Raw's `archive_fetched_at` still says which download a snapshot was built from.

**dbt reads snapshots by key.** `snapshot(source)` (from `LEADS_SNAPSHOT`, which Dagster sets per
partition) replaces `run_month()`; on the dev target it defaults to the source's newest complete
snapshot. Raw built from nothing reads every complete snapshot, listed from the manifests, rather
than `*`, which would also read a fetch that never finished.

**A new source is one entry.** `archived_sources()` in `entrypoints/definitions.py` lists the
sources as `ArchivedSource` (name, cadence, fetch); the archive asset, partitions, fetch asset,
job and schedule, rebuild job, index and automation are all built from it. What is left per source
is what any design needs: its fetch (transport) and its dbt models (`read_csv` or `read_json` over
the snapshot folder).

## JAR source (2026-10-17)

**JAR is the second source, as the simplest left.** It is open data with no key, and the portal
exports a whole dataset as one CSV (`/<model>/:format/csv`: 146 MB and 547,382 entities in about
2 s), so it is Sodra's shape: Python fetches files into the archive, dbt reads them with
`read_csv`. The plan needs it next (build step 2, the declared side). Google needs a key and costs
money per call, Oxylabs' Maps target does not work (see `vmi-leads-data-sources/README.md`), and
the VMI register and Address Register are not in the plan's sources yet.

**The bulk export, not dlt's REST source.** The probe's `rest_api_source` with a Spinta paginator
worked, but paging 547 k rows at 1,000 a page is 548 requests against a portal that answers a few
percent of them with an HTML 500, for the same rows the export gives in one. Every snapshot is the
whole register, so there is no incremental state for dlt to keep. dlt stays only as the downloader's
retrying HTTP client. The probe (`dlt_jar_rest_api.py`) is removed.

**Three datasets a snapshot: entities, legal forms and statuses.** An entity refers to its form and
status by `_id`, so staging needs both classifiers (168 and 31 rows), and they are fetched with every
snapshot so that a reference is resolved in the classifier as it was then. The registered address
is not fetched: `pilnas_adresas` and `adresas._id` are empty on every row, and the address is four
datasets away (`Buveine` → `Adresas` → `Pastatas` → `Gatve`, `GyvenamojiVietove`), the last three
the Address Register's, which is a source of its own with its own cadence.

**Fetched monthly, on the 21st at 06:00, keyed `YYYY-MM`.** A source's cadence is set by how often it
publishes. The portal lists JAR as updated monthly; its changelog (`/:changes`, every change with
`_created`) shows updates every one to six months on no fixed day: 2025-05-15, 2025-07-23,
2025-09-03, 2026-03-09, 2026-08-20. So monthly is the finest grain worth fetching: daily would
archive the same 146 MB thirty times a month. With no publication day to follow, the day is the
leads': each month's leads then see the newest register, and JAR's snapshot of a month has the same
key as Sodra's. The cost is up to a month's lag after an update. If that matters, poll the export's
`Last-Modified` daily and fetch only when it changes; that is a sensor, not a schedule, and keying
snapshots by publication rather than by fetch period would change the archive's contract. A
snapshot is stored even when the register did not change since the last (146 MB a month), as Sodra's
are: it is the record of what that fetch saw, and the sidecars' SHA-256 shows the copies are equal.

**The fetch rejects a file without an `_id` column or a row.** The front proxy's error pages come as
HTTP 500, which the downloader retries; the check stops one sent as a success, or an empty export,
from completing a snapshot, since a complete snapshot builds itself and an empty register would
replace every company in staging.

**Raw keeps each export whole, as text, under its published header** (`raw_jar.entities`,
`legal_forms`, `statuses`), as raw Sodra does: incremental `delete+insert` on `_snapshot_month`, new
columns appended, and the sidecar's fetch on every row. The `archived_csv` macro reads one archived
CSV and its sidecars for any source whose files are plain CSV, as get.data.gov.lt's are; Sodra keeps
its own reader for its zips. No checks run before raw: nothing about the register's timing can make a
snapshot wrong, as an unpublished month does Sodra's.

**Staging keeps every snapshot: one row per company and snapshot month** (the plan's
`stg_jar__entities` grain). A month's leads read the register as of that month, and as each snapshot
is the whole register, the newest is just the newest `snapshot_month`. References are resolved in
the classifiers of the entity's own snapshot; resolved across snapshots, every classifier fetched
twice would match each entity twice (the unit test). A renamed or dropped column would turn into
nulls rather than fail, as raw keeps the old column, so every column the register always fills has a
`not_null` test, and `deregistered_on` one on deregistered entities (status 10), which always have
the date. Name normalisation is the matching step's (plan, step 4), not staging's.

**The replay sample is the register cut down to Sodra's sample.** `replay/jar/2026-09` holds the
1,847 entities whose code is a `jarCode` in `replay/sodra/2026-09` (every one is in the register), and
both classifiers whole, lines byte for byte as fetched by the source's own fetch on 2026-10-17.

## Groups (2026-10-17)

**Each source is a group within its lake's: `live/sodra`, `replay/jar`.** With a second source, one
box per lake mixed both sources' layers in one row. Dagster 1.13 reads `/` in a group name as nesting
(`parseGroupId` in the UI's `asset-graph/Utils.tsx`), so the lineage view draws a box per source,
fetch → archive → raw → staging inside, within the lake's box. Groups are still global across code
locations (the per-location flag is off by default), and the lake first in the name keeps the two
lakes' boxes apart, as before. Models that combine sources will be in the lake's group itself,
beside the source boxes. Asset keys did not change, so no history moved.

## VMI source (2026-10-17)

**The VMI taxpayer register is fetched daily, as it changes most days.** Its export is regenerated
after each change (`Last-Modified` 2026-10-16 00:00:08 GMT), and its changelog shows changes on 15
of September 2026's days, mostly in a burst at about 00:00 UTC. So it is the first daily source:
snapshots keyed `YYYY-MM-DD`, fetched at 06:00 Vilnius time, as the source `vmi_daily_changes`.

**A daily snapshot holds the day's changes from the changelog, not a daily export.** The export is
350 MB and 930,021 rows, about 10 GB a month fetched daily, and it is only ever the register as it
is now, so a day that has passed cannot be fetched. The changelog (`/:changes`) keeps every change
since 2024-02-07, each with its time: September 2026's 30 days held 57,003 changes in 18.6 MB
(21,446 on the busiest, none on 16), and any past day can be fetched again with the same result,
so a month of days (September 2026, to overlap Sodra's and JAR's samples) was fetched by a
backfill.
A patch holds only the fields it set, a field emptied as `null`, so the changes are kept as JSON:
in the CSV form an emptied field and an untouched one look the same.

**Snapshot D holds the changes made from D−1 06:00 to D 06:00** (Vilnius time), what was new when
D's period began, as Sodra's and JAR's snapshots are what their sources had published then. The
schedule's fetch at D 06:00 gets all of them. The changelog ignores query filters, so the window's
start is found by bisecting the changelog on `_created` (about 25 requests of one entry), and the
window is read forward from there in pages of 10,000, up to the first change made at or after its
end. The page that reaches past the end is asked for again with the count that fits: each page
holds the window's changes only, as the portal sent them, and is named after the id it starts
from. A day with no change has no page, only its manifest; the dbt reader lists the files that
exist rather than globbing a pattern that may match none.

**A source that keeps its history can be fetched for a passed period.** "The fetch takes no
partition; the clock picks its snapshot" (Snapshots define the partitions) holds for a source
captured as it is now. For one with history (`ArchivedSource.history_start`), the fetch asset is
partitioned by its cadence's periods, from `vmi.first_daily_changes` (2026-09-01) to the current
one, and its schedule (`build_schedule_from_partitioned_job`) fetches the period that begins at
the tick, the one the clock would pick. A backfill fetches past ones in one run, in order
(`BackfillPolicy.single_run`), completing each snapshot as it goes. The archive and raw are still
partitioned by the complete snapshots (the archive maps to its fetch by identity), so the rest of
the pipeline is unchanged.

**The baseline is a second source, `vmi_monthly_register`, fetched monthly.** The state of the register is
the newest export with the changes after it applied, so an export is needed. Fetched as a file of
some daily snapshots, it would give one source two kinds of snapshot and a fetch that reads the
archive to decide; as a source of its own it has one cadence and one kind of file, like every
other: `MokesciuMoketojas.csv`, the EVRK 2 classifier its activities refer to, and
`last-change.json`, the changelog's newest change at that moment. Monthly keeps the changes to
apply short and the archive near JAR's size; it is on the 21st with the other monthly fetches.

**An export is kept only when its ETag is the changelog's newest revision.** The portal sets an
export's ETag to the revision of the last change it holds (checked on JAR and VMI), and caches the
export for up to a minute (`cache-control: max-age=60`, `X-Cache: STALE` seen). An export from
before the newest change would be placed wrong in the changelog, and changes it lacks would never
be applied, so the fetch asks again, five times a minute apart, then fails. `Download` records the
ETag, and so do the sidecars and manifests.

**Staging: every change, and the register as it stands.** `stg_vmi__changes` is one row per change,
with the fields it set and the company of the row (from the change, the newest export holding the
row, or the row's insert). `stg_vmi__current_register` is the newest export with every change after its last
change applied in id order: `arg_max` over each field's JSON keeps an explicit `null` (an emptied
field) and skips a missing key (an untouched one); an insert sets every field, a delete drops the
row. Activities resolve in the EVRK 2 classifier exported with it. The register's columns, names
and types are one list (`macros/vmi.sql`), read by both.

**September 2026 is history, not states.** The only export there is was fetched on 2026-10-17, after
every September change, so in both lakes September is `stg_vmi__changes` and the register's state
is the export's. A state as of a past day needs an export from before it, which cannot be fetched
now, or the whole changelog from 2024-02-07 (27.4 M changes, about 9 GB of JSON), which was ruled
out. From the first daily fetch after the export on, the changes are applied.

**The changelog is read sparingly: one search a run, a request a second.** get.data.gov.lt
publishes no rate limit and never answered 429, but on 2026-10-17, after about 2,000 changelog
requests in an hour (a scratch fetch of September bisected both ends of every day, at up to 10
requests a second), it stopped answering changelog reads by id: they hung, on JAR's changelog
too, while `:changes/-1` and ordinary queries stayed fast. A backfill fetched day by day, a run
each, would do the same. So the changelog is read by `spinta.Changelog`, which searches only for
the first window's start and reads each window forward to the first change past its end, whose id
is the next window's start, and remembers that change, so a day that ends before it, with no
change, costs nothing. One is kept per process, and a backfill is one run, so September costs
about 25 searches and two requests for each of its 14 days with changes, about 55 in all. Its requests go
through a `ThrottledDownloader`, at most one a second (`spinta.REQUEST_INTERVAL_SECONDS`), with a
30 s timeout (`API_TIMEOUT_SECONDS`): dlt's helper retries a request that hangs, and the 300 s
meant for files would hold a fetch for five minutes each time. Both are passed to
`archived_sources()` as `api`, beside the file downloader. (Since 2026-10-18 both are settings of
VMI's client, `vmi.daily_changes_http`; see dlt removed.)

**A staging model of two sources can fail before both have a snapshot.** `stg_vmi__*` reads
`vmi_monthly_register`'s raw and `vmi_daily_changes`'. `eager()` waits while a dependency is missing a partition, but a
source with no snapshot yet has no partitions, so nothing counts as missing: the first export's
raw started staging before any day of changes was built, and it failed on the missing table. The
first day's raw then starts it again. Only a lake's first build meets it.

**The portal blocks every uncached request for a while after a burst, and its cache hides it.**
After the first burst, reads that reach the portal's application hung (a fresh changelog read, a
plain query), while its proxy answered any URL it had seen before from its cache (`X-Cache:
UPDATING` or `STALE`), at once. A probe repeating one URL looked healthy, and the relaunched
backfill's first day went through on cached searches. A check that the portal answers must use a
URL it has not seen. The block lasted more than 30 minutes; its threshold and length are not
published.

**Live's September came from a scratch fetch, after the block.** 2026-09-01 was fetched through
Dagster (the backfill's run, before the block caught it). 2026-09-02 to 2026-09-30 were copied
into the live archive from a fetch the source's own code made at 13:57 that day, before the
block: the archive index takes a snapshot whoever completed it, and the files are those that
fetch saw. Live then built them as it builds any new snapshot: 57,003 changes, as fetched.

**The two sources are named for what they fetch and when: `vmi_daily_changes` and
`vmi_monthly_register`** (first `vmi` and `vmi_export`, which said neither). One dataset fetched as
two sources is drawn as one box, `vmi`, holding a box for each source and the staging models that
combine them (`stg_vmi__changes`, `stg_vmi__current_register`): a source's `group` is its path in its
lake's group (`vmi/daily_changes`), and the dbt steps take their groups from the same map, a staging
folder of no source's name (`models/staging/vmi/`) being drawn in the group of its own name.
Renaming moved the live archive's folders and dropped the old tables, partitions, pools and asset
histories; both lakes were then rebuilt from the archive as they were.

**`row_id` is DuckLake's.** The register row's id was staged as `row_id` until the first build in
the lake failed: DuckLake reserves the name for data inlining. The dev target builds into plain
DuckDB, which allows it, so only a build in the lake shows it. It is `register_row_id`.

**get.data.gov.lt's API is one module, `sources/spinta.py`**, shared by JAR and VMI: the export
check (moved from JAR), the changelog's boundary search and pages, and the last change.

**The replay sample is cut to Sodra's sample's companies**: 6,621 register rows of 1,846 companies,
and their 491 September changes on 10 of 30 days. Cutting a JSON page means writing it again, so the
sample's pages are re-serialized (compact UTF-8), unlike the CSV samples' byte-for-byte lines.

## Address Register source (2026-10-17)

**The registered address is the seat's, resolved in the Address Register.** JAR leaves
`pilnas_adresas` and `adresas` empty on every entity. Its seats (`Buveine`, `rc/jar/buveines`) give
each entity's registered office as the `_id` of an address, and the Address Register (`rc/ar`)
says what that address is. A seat names its entity only by JAR's internal `_id`, so it is resolved
where the entities are: the seats are a fourth dataset of the `jar` source (45 MB), joined in each
JAR snapshot as forms and statuses are, giving `stg_jar__entities.registered_address_id` and
`registered_address_since`. The Address Register is a source of its own, `address_register`.

**Five of the register's datasets, not its address dataset.** A seat's `adresas._id` is the
address of a building or land plot (`Pastatas`, by its `aob_kodas._id`) for 115,901 seats, or of a
premises (`Patalpa`) for 102,086, 47%. Every premises names its building (`pastatas._id`), and
every building its street, settlement and municipality (`Gatve`, `GyvenamojiVietove`,
`Savivaldybe`). On 2026-10-17 every one of these references resolved. `Adresas` itself (317 MB,
2.35 M rows: the address's kind, numeric code and validity dates) is a hop the ids skip, so it is
not fetched; it is one line in `DATASETS` if the numeric address code is ever needed. Address points
with coordinates (`AdresoTaskas`) have no rows on the API. A snapshot is about 565 MB: buildings
317, premises 229, streets 12, settlements 7.

**Monthly on the 21st, with JAR.** The Registers Centre loads the register in the same batches as
JAR: the streets' whole changelog (61,737 changes, read by timestamp in 7 requests) shows changes on
2024-05-28, 2025-04-08, 2025-05-16, 2025-09-03, 2026-03-09 and 2026-08-18, JAR's own update days;
buildings last changed on 2026-08-20. Monthly is the finest grain worth fetching, and on JAR's day
each JAR snapshot has an Address Register snapshot of the same key to resolve its seats in. The
cost is JAR's too: an unchanged register is archived again each month (565 MB), which polling
`Last-Modified` would save.

**Two of the datasets are stale at the source.** The seats last changed on 2025-09-03 (one per
entity, 229,645 in all), so no entity registered since has one: 13,648 active entities registered in
2026 and 4,203 of 2025's. 205,442 of the 232,594 active entities (88%) have a seat with an address,
76,263 of them in Vilnius city. The premises last changed on 2024-05-28: `Adresas` holds 1,205,499
premises addresses, `Patalpa` 1,151,462. Every seat's premises resolves now, but a seat moved to a
newer premises would not. Neither can be fixed here; both are in the models' descriptions.

**Staging is one row per address and snapshot** (`stg_address_register__addresses`): buildings'
and premises' addresses in one table, keyed by `address_id`, the id a seat refers to, with
`building_address_id` (a premises' building's), so that two companies in one building can be told
from two on one street. Streets, settlements and municipalities are left-joined in the address's
own snapshot, so a reference that does not resolve leaves a null for a test to catch: municipality
and settlement are always set; a street is not (most villages have none), but every address in a
city has one, so `street_code` is tested there. Components are kept as published (the street in
the genitive, the postcode with `LT-`); no address line is composed, as the matching step compares
components. Entities and addresses are joined where sources are combined, not in staging:
`stg_jar__entities` carries the address's id only.

**JAR's 2026-09 snapshot was fetched again to add the seats.** `raw_jar__seats` reads `Buveine.csv`
from every JAR snapshot, and the live 2026-09 snapshot was taken before seats were fetched. A second
fetch in a period replaces its snapshot, so 2026-09 was fetched again on 2026-10-17 at 16:19 UTC,
with the Address Register's first snapshot. The entities came back with the same `Last-Modified`,
and the forms and statuses with the same SHA-256.

**The replay sample is the seats of the JAR sample's entities and the addresses they point at.**
`replay/jar/2026-09` was cut again from that fetch: the same 1,847 entities (the file's bytes are
unchanged) and their 1,787 seats. `replay/address_register/2026-09` holds the 1,309 addresses those
seats point at, the buildings their premises are in, and those buildings' streets and settlements,
lines byte for byte as fetched (648 KB). Every seat resolves. The Google matching step will want
more of Vilnius's addresses than the companies' seats; widen the cut then.

## DuckDB memory (2026-10-17)

**Each dbt process's DuckDB is capped at `transform.duckdb_memory_limit` (2 GB locally).** Unset,
DuckDB takes up to 80% of RAM per process. After JAR and the Address Register were fetched at once,
Dagster built both sources' raw tables side by side while a script cutting the replay sample held
the 546 MB of address files as Python objects, and the 14 GB dev machine, which has no swap, ran
out of memory and took the desktop down with it. The profile sets `memory_limit` from the
configuration (`LEADS_DUCKDB_MEMORY_LIMIT`); beyond it DuckDB spills to disk or fails the run,
which is the point. The fetches are not capped: each holds its snapshot's files in memory until it
completes (565 MB for the Address Register).

**Each dbt process spills into a folder of its own.** For an in-memory database DuckDB spills into
`.tmp` in the working directory, and names its spill files the same in every process
(`duckdb_temp_storage_DEFAULT-0.tmp`), so dbt processes running side by side, one per source and
lake, would write over each other's. The profile's `temp_directory` is
`target/duckdb_tmp/<dbt invocation id>`, unique to each dbt run.

## Run monitoring (2026-10-17)

**Dagster's run monitoring is on, to fail runs that died with their code server.** When the dev
container crashed, two runs died with every Dagster process. After the restart they were still
`STARTED`, an hour later, each holding its pool's only slot (`live_transform_jar`,
`live_transform_address_register`), so no later build of either source could start. When only a
run's own process dies, the code server that launched it fails the run at once; when the code
server dies too, nothing does.

**It works by time, not by checking the run's process.** The `DefaultRunLauncher` cannot tell
whether a run's process is alive (it has no `check_run_worker_health`), so the only check it allows
is the maximum runtime: the daemon fails any run still going after `max_runtime_seconds` (3600),
stopping it if it is alive, and frees an ended run's pool slots `free_slots_after_run_end_seconds`
(60) later. Failing a run does not free its slots by itself. A dead run therefore blocks its pools
for at most about an hour. Runs take minutes (the Address Register's raw and staging, 51 s; the
monthly VMI fetch, up to six minutes when the export lags its changelog); one that needs longer sets
the tag `dagster/max_runtime`.

**Tested on a throwaway instance first** (`dagster dev` with its own SQLite home and a pooled asset
that sleeps, the same launcher, coordinator and pool limit, the limits set to 90 s and 15 s). A run
holding the slot, its whole instance killed with `kill -9` and restarted, was still `STARTED` with
the slot, as in the crash, and a second run queued behind it. The daemon failed the dead run 102 s
after it started, freed its slot 16 s later, and the queued run started; that run, alive, was
stopped and failed when it passed 90 s too.

## Google Places source (2026-10-17)

**Google's listings come from the Places API, as the source `google_places`.** The plan left the
backend open between the Places API and a scraper. Oxylabs' Maps target failed on every try, its
Classic credentials are refused, and its generic scrape of a Maps page has no parser and no opening
hours, phones or websites (`vmi-leads-data-sources/README.md`, finding 9). The Places API has every
field the plan names. The plan's switch between backends (`google.backend`) is gone: each backend
answers in its own format (JSON pages here, HTML from a scraper), so each would be a source of its
own, archived and read by its own raw models, and both would feed one staging model,
`stg_google__listings`, in a folder of the dataset's (`models/staging/google/`), drawn in one box
`google` with the source in it (`google/places`), as the VMI register's two sources are. Choosing a
backend is choosing which fetch runs. Settings are `[google_places]` in `pipeline.toml`.

**Monthly on the 21st, keyed `YYYY-MM`.** Listings change continuously and Google publishes on no
schedule, so the cadence is the leads': each month's leads compare Google's month with Sodra's
declarations, and the review-growth signal is the change in review counts from one month's snapshot
to the next. Fetched daily, a snapshot would cost what a month costs, every day.

**Every request is billed by the fields it asks for, so a sweep is planned for free and then paid
for.** Google bills a Text Search page at the SKU of the most expensive field in its field mask
(2026-10-17): place ids only (Text Search Essentials IDs Only) cost nothing, without limit; the
rating, review count, opening hours, website and phone the leads need are Text Search Enterprise,
1,000 requests a month free and then USD 35 per 1,000 (Atmosphere fields, such as reviews, USD 40).
So a fetch first searches every tile with place ids only, which tells it what every search finds,
then chooses what to pay for, counts it, and fails without paying anything if it is more than
`google_places.max_paid_requests` (1,500). It then pays for as many pages of each search as the
plan had, so the count is what it pays. A search asked again finds the same places whatever its
field mask, but for a few at the margin: a plan of the Old Town made ten minutes after a fetch found
800 places, 773 of them among the 803 the fetch paid for.

**A search finds at most 60 places, and over a large tile far fewer, so tiles are split when wide,
not only when capped.** The probe found every query capped over the whole of Vilnius. Splitting
only capped tiles, "kavinė" found 712 places city-wide in 79 requests, but its tiles over the
centre stopped early: a 1.8 km tile over the Old Town and Naujamiestis found 13 cafés, below the
cap, so it was not split. Sweeps rooted at a 2.2 by 2.6 km box there (54.672–54.692 N,
25.255–25.295 E) found 553 places (the same rule) and 527 (a grid of 322 m tiles), 582 together;
the city-wide sweep had found 267 of them. So a tile with places is split while it is wider than
`max_tile_meters` (1 km), and a capped one while its quarters are at least `min_tile_meters`
(100 m) wide: "kavinė" then found 1,698 places city-wide, 502 of the 582. The smallest tile any
query needed was 230 m, and no tile was capped at the smallest size. A third page can carry a token
after the API dropped some of its 60 candidates (20, 20 and 18 places, and an empty fourth page,
in one of 721 "restaurant" searches): no fourth page is asked for, and such a search counts as
capped. The first live price failed on it, as the fetch then took a token on a third page for a
raised cap. Results stay ranked by
relevance: a tile finds places its quarters do not and the other way round, and a query finds
places outside its category (a park, a supermarket for "kavinė"), so coverage is good, not
complete, and types say what a place is. An empty tile is not split; none of the sweeps checked
whether a wide empty tile hides places.

**Only the searches that hold every place found are paid for, across queries.** Paying for every
tile with places cost 538 pages for "kavinė"'s 1,698 places. The plan knows each search's places,
so the fetch pays for a set of searches that holds all of them, chosen greedily by new places per
page (a set cover): 244 pages for "kavinė", and 516 for the 3,527 places of "kavinė", "cafe",
"kirpykla" and "autoservisas" together, against 658 paid one query at a time. Those four took
3,111 free requests. The ten configured queries, priced over Vilnius on 2026-10-17 (the price
asset, see Google's search areas), found 5,858 places for 927 paid requests, within the free 1,000
if nothing else on the billing account uses the SKU (USD 32 if all were billed), and 7,690 free
ones in 24 minutes; the fetch counts the exact number each time. The cost of the cover:
only the pages paid for are kept, so which queries found a place is not known, and staging has no
such column.

**Ten queries, the categories' words in Lithuanian and English.** The plan asks for both. English
adds some: "cafe" added 96 places to the 1,698 of "kavinė" (with 1 km tiles). Each query's place
in the cost is small, as a place several find is paid for once.

**The area is Vilnius city municipality's bounding box** (OpenStreetMap: 54.5689–54.8324 N,
25.0245–25.4815 E), not the probe's rectangle (54.62–54.78 N, 25.10–25.40 E), which cut off
Grigiškės, Naujoji Vilnia and the north and south: on the wider box "kavinė" and "autoservisas"
found 9% and 12% more (splitting capped tiles only). The box holds some of Vilnius district;
`stg_google__listings.locality` says where a listing is. (Since the same day the areas are data,
not configuration: see Google's search areas.)

**The plan is not archived; each paid page's sidecar keeps its request.** The plan is thousands
of pages of ids a month, and the places it found are those the paid pages hold. A page is archived
as the API sent it, named `places-<query>-<tile>-<page>.json`, the tile by its path of quarters
from the whole area (`r`, then 0 south-west, 1 south-east, 2 north-west, 3 north-east). The query
and rectangle a page answered are not in the page, so `Download` gained `request` (what a GET of
the URL would not ask: the body and field mask, never the key), which the sidecar records. The
manifest does not: a page token is 0.8 to 1.6 KB, and a sweep's thousand pages would make the
manifest megabytes. For the same reason Dagster's metadata gives a snapshot of more than 100 files
as its count and size (`LISTED_FILES_LIMIT`), not every file. `archived_json_entries` reads any
JSON page's list (`list_key`, `places` here) and, if asked, the sidecar's request; raw takes the
query and rectangle from it.

**Staging is one row per listing and snapshot.** A place on several paid pages is kept as last
fetched. The address is Google's components as published (the street written out, in the
nominative, the postcode without `LT-`), for the matching step to compare with the Address
Register's; weekly opening hours are summed from the periods, 168 for a place open around the
clock (a period with no close); `cid` is taken from `googleMapsUri`, as a scraper's listings carry
it (an unsigned 64-bit number: the sample's largest is above 2^63). Every listing found so far was
operational (the sample's 803 and the probe's 180). The plan's normalised name and category from a
type-map seed are left to the matching and scoring steps, which define them.

**A fetch may run for three hours, and its schedule starts stopped.** About 7,700 free and 900
paid requests, at about five a second (the price's plan of Vilnius took 24 minutes), take half an
hour, and more with every area searched, close to run monitoring's hour, so the fetch job sets `dagster/max_runtime` to 10,800 s
(`ArchivedSource.fetch_max_runtime_seconds`). The tag is the job's, so a fetch started from the
asset graph instead has the instance's hour. Every sweep is paid for, so `fetch_google_places_schedule`
starts stopped (`ArchivedSource.schedule_running`), unlike every other source's: a deploy, or a
reload of this branch, must not start the first paid sweep. Turn it on once the first sweep was
wanted.

**dlt does not load Google either.** This was the source to decide dlt by (dlt, 2026-10-15). The
pages are archived and dbt reads them with `read_json`, as VMI's changelog pages are; dlt's REST
source would page, but a search has at most three pages, and the plan and cover are the source's
own. So dlt is now only the HTTP client (its requests helper's retries, POSTs included) and the
archive's fsspec configuration. Replacing those is a change of its own (done 2026-10-18, see dlt
removed).

**Google's terms are not met yet.** Only place ids may be stored indefinitely; the archive and raw
keep every field as long as every source's, and so does the replay sample, which would publish
Google's content if committed to a public repository. The plan says retention is a recorded
decision, and it is not made: deleting a snapshot's fields after Google's limit would also remove
the previous month's review counts that review growth needs. Decide before production, and before
committing `replay/google_places`.

**The replay sample is a small real sweep, and the live archive has no snapshot yet.**
`replay/google_places/2026-09` is the source's own fetch on 2026-10-17 of "kavinė", "kirpykla" and
"autoservisas" over the Old Town box above: 116 free and 81 paid requests, 81 pages (one search
paid a page more than planned, before the fetch stopped at the planned pages), 803 listings, 5.3
MB. It is not cut from a city-wide sweep, as none was paid for: the first, priced at 927 paid
requests, is the user's to start.

## Google's search areas (2026-10-17)

**Where Google is searched is data, drawn in the graph, not a rectangle in the configuration.**
With the area in `pipeline.toml`, nothing in Dagster said that the sweep covers Vilnius, and
widening it meant editing configuration with no idea of the cost. Now the fetch reads its areas
from the dbt model `int_google_search_areas`, one row per municipality, built from three seeds and
Sodra: `municipalities` (the Address Register's 60 codes and names, Sodra's spelling of each, and
the bounding box of its boundary in OpenStreetMap), `activity_categories` (the plan's category
seed, by EVRK prefix in both versions) and `google_search_scope` (the municipalities to search,
each `price` or `fetch`). The lineage reads Sodra's staging and the seeds → the areas → Google's
price and fetch → Google's archive, raw and staging. The seeds are drawn in a box of their own in
each lake, `static_data` (`<lake>/static_data`): data kept by hand in the repository, which no
pipeline updates, of three kinds: reference data copied once (municipalities), a domain mapping
(activity categories) and an operating decision (the scope).

**The data proposes, a person enables, the price shows the cost, the budget refuses.** The areas
model counts, for every municipality, Sodra's employers in the leads' categories in its latest
month, and ranks them (Vilnius 1,548, Kaunas 685, Klaipėda 313 in 2026-08): where a search would
find the most of what the leads compare. A municipality is searched only once the scope seed lists
it, so data from the whole country never starts a country-wide sweep. Listed as `price`, it is
planned for free and priced; as `fetch`, the monthly fetch pays for it too, under
`google_places.max_paid_requests`. Only Vilnius is listed, as `fetch`: the assignment's scope.

**The price became the plan the fetch pays for, and the plan and the fetch are one step.** A
price asset that wrote nothing was a calculation drawn as an asset, beside a fetch that planned
again on its own, so the graph never showed that the price is what pays. Now
`plan_google_places` plans every listed area with place ids only, which costs nothing, and shows
as metadata each area's places and paid requests alone, and the next fetch's (the fetched areas
together, a place in two paid once) in requests and USD; a blocking check on it, `within_budget`,
passes when those are at most `google_places.max_paid_requests`. `fetch_google_places` is drawn
after it and pays for exactly that plan. Both are one subsettable `multi_asset`
(`build_planned_fetch_asset`), so the fetch pays for the plan made minutes before in the same
step, from memory: a plan read back from storage would need a rule for when it is too old, and
results move from one hour to the next. The plan's condition is `eager()` plus a catch-up on its
first evaluation (`plan_on_change`), so a change to the scope seed, once the definitions are
reloaded, rebuilds the seed, the areas and then the plan alone, paying for nothing; its sensor of
its own (`plan_google_places_automation`) carries the fetch's three-hour `dagster/max_runtime`,
which the dbt runs' sensor must not. The fetch job selects both, so every fetch records the plan it
paid for and its check; the fetch has no condition, its schedule starts stopped, and it refuses a
plan over the budget before paying, in the step itself as well as in `pay_for_plan`, since a
blocking check stops only later steps. The snapshot keeps the plan, `plan.json`: every search, the
places it found and whether it was paid for, which says every query that found a place where the
paid pages say one. A plan of Vilnius is 7,690 free requests (2026-10-17: 5,858 places, 927 paid
requests, within the free 1,000), and the areas model rebuilds when Sodra's staging does, so the
plan runs about once a month besides the scope's changes and the fetches.

**Searches are sent many at once, and Google's quota is waited out, not counted.** Sent one at a
time, Vilnius's plan took 24 minutes. Requests are I/O, so a thread pool (`google_places.workers`,
16) suffices: neither asyncio nor processes would add anything, and dlt's requests client keeps a
session per thread. The plan's searches are independent but for a tile's quarters, which wait for
it, so they are a queue that grows as tiles split; a search's pages follow each other's tokens. The
plan is sorted back into area, query and tile order, so the same answers buy the same searches
(the greedy cover breaks ties by that order). The Places API's limit is per minute and per project,
not per hour, so nothing counts it: a request over it is answered 429 and sent again after 2, 4, 8,
16, 32 and then 60 seconds, ten times, about five minutes (`HttpPlacesApi`), where dlt's default
gives up within 15 seconds unless Google sends Retry-After. Google does not say whether a 429 is
billed; at worst a retried paid request costs one more. A payment that fails starts no further
search.

**Demand comes from Sodra, as the VMI register cannot place today's businesses in these
categories.** The register's activities name the municipality where they are carried on
(`vv_savivaldybe`), with the Address Register's codes, which would have suited. But on 2025-01-01,
when EVRK 2.1 came in, VMI ended every activity coded 56.10 (restaurants), 96.02 (hair and beauty)
and 45.20 (car repair), the codes EVRK 2.1 split or moved, and the export holds no successor: the
register's activities refer to the EVRK 2 classifier only. Of 5,910 companies with 56.10, 18 rows
are still open, all annulled; 56.30 (bars), unchanged in EVRK 2.1, carried on. This matters again
for the matching step's activity-code fit. Sodra codes its 2026 rows in EVRK 2.1 and writes the
employer's municipality by name; the names match the Address Register's with miesto as m. and
rajono as r. (the seed holds both), except 2,222 employers of 2026-08 with none (Nenustatyta).

**Areas are municipalities, as bounding boxes.** A municipality is the finest unit every source
names. The Address Register's municipality dataset has no geometry on the API, so the boxes are
OpenStreetMap's (Nominatim, each the one administrative boundary of that name; Vilnius's equals
the box the source used before). A box holds parts of its neighbours, and a city's lies inside its
district's: places in two areas are paid for once, but their free plans overlap. DuckDB 1.5 has a
core `GEOMETRY` type (spatial adds the functions) and DuckLake stores it, so with outlines instead
of boxes the plan could skip tiles outside the municipality; not done.

**The Google fetch reads the lake.** It is the first fetch that reads a table the pipeline built.
`DuckLakeReader` (ingest) attaches the lake read-only, through the catalog, bucket and secret dbt
uses, capped at `transform.duckdb_memory_limit`; the SQL that attaches it moved from the
orchestration's tool environments to `leads_lake.connection`, which the lake MCP server's startup
SQL now uses too. A fetch declares what it reads as `ArchivedSource.fetch_deps`, which draws the
edge and orders the runs.

**A rebuild never fetches or prices.** Each `rebuild_<source>_from_archive` selected everything
downstream of the source's archive. Google's fetch now reads a model built from Sodra's staging,
so a rebuild of Sodra would have selected the paid fetch, and Google's archive, raw and staging
after it. The rebuild jobs now leave out every fetch and price they reach and what those lead to;
the test of the jobs' assets checks it. A model that reads both, as matching will, rebuilds
itself once Sodra's staging has.

**The replay sample's pages were renamed** with their area's code (`places-13-...`), as pages are
named since areas were added; their bytes, sidecars and manifest records are unchanged but for the
names.

## dlt removed (2026-10-18)

**dlt is gone; httpx with tenacity is the HTTP client, and s3fs opens the archive.** No source
loaded through dlt (Google was the source to decide it by, see Google Places source). What was left
was its `requests` helper and its fsspec configuration, plus the DuckLake destination and the
Dagster translator, which nothing used. Every source archives what it fetched byte for byte and
dbt parses it (`read_csv`, `read_json`), so dlt's paginators, normalisation, schema inference and
incremental state have nothing to do: the archive is the record, and a load that bypassed it would
be a second one. The sources that page do so in ways dlt's paginators do not: Spinta's changelog
is read by bisection on ids, and Google's searches have at most three pages each, under a paid
plan. Removing dlt, dagster-dlt and pyarrow (only the dlt-era Sodra filter used it) leaves 150
packages in the lock, down from 161. Reconsider dlt for a source that is a large conventional REST
API, paged by cursor and read incrementally, whose nested JSON wants child tables, and whose
responses need not be kept.

**Sources get a plain `httpx.Client`; retries are in its transport.** `build_http_client`
(`adapters/http/client.py`) is the only place a client is built: httpx with the pipeline's user
agent, redirects followed and the source's timeout, over a `RetryingTransport` in which tenacity
retries 429, 5xx, timeouts and dropped connections, waiting what the server's Retry-After asks for
or a doubling backoff, capped either way. The adapters (`HttpDownloader`, `HttpPlacesApi`) use
httpx's own API, so a source that needs a new kind of call adds nothing to the client. A first
version wrapped httpx in a class of its own, which had to re-implement every method it offered.
The transport is where httpx itself puts retries (`HTTPTransport(retries=)`, connect errors only).

**An attempt reads the whole body.** The transport receives a response before its body is read,
so a server that went silent halfway through one would fail after the retries were over. Each
attempt reads the raw body from the response's stream and hands the client a new in-memory
response, which it decodes once (`Content-Encoding`) and reads and closes as any other, `elapsed`
included. The cost: `client.stream()` is buffered too. Every source holds its downloads whole
(`Download.body`), so nothing streams today; a file too big for memory would need a client
without this transport.

**An error status raises in a response hook, with the start of the body.** The hook sees every
redirect on its way, so it raises for 4xx and 5xx only (httpx's `raise_for_status` raises for a
3xx too). It names the status, method and URL and the first 500 characters of the body, for
every source; before, only `HttpPlacesApi` added the body.

**Each source's client is configured by its own table, `<source>.http`** (`pipeline.toml`:
`timeout_seconds`, `attempts`, `backoff_seconds`, `max_wait_seconds`, `min_interval_seconds`), with
the reasons for its values beside them, as the sources' schedules, `workers` and
`max_paid_requests` are. Every setting is required: there are no defaults in code. VMI's two
sources have `vmi.daily_changes_http` and `vmi.monthly_register_http`, after the section's other
`daily_changes_` and `monthly_register_` keys. `load_http_client_settings` lives in the HTTP adapter
and the composition root calls it, so the sources' settings never name an adapter's type. The
tables load with the definitions, so a malformed one fails the load. The values are dlt's defaults
for files (300 s, five attempts in about 15 s) and what the code held before: the VMI changelog
30 s and one request a second, Google ten attempts over about five minutes, which a test
of the local config holds to more than a minute of quota. `Configuration.get_float` reads a
number written as an int or a float.

**Rate limits are per server, but spacing is per source.** JAR, the Address Register and VMI all
fetch from get.data.gov.lt, each through its own client, which spaces only its own requests. Only
VMI's changelog is spaced today, so nothing changed; if two spaced sources may run at once, key
the tables by server instead (`[http.data_gov_lt]`).

**No connection limit is configured.** The transport's pool is httpx's default, 100 connections,
more than any source has in flight (`google_places.workers`, 16). dlt's requests client needed
one (`max_connections = 64`) for the sessions it kept per thread; httpx's pool is thread-safe, and
one client serves a sweep's threads.

**Pacing moved from a `Downloader` wrapper into the transport.** `ThrottledDownloader` spaced the
starts of downloads; the transport's `Pacer` spaces every attempt, retries included, across
threads.

**httpx follows redirects only when asked.** requests did so by default; the client sets
`follow_redirects=True`, and a test holds it.

**The connection pool is built on the first request.** Building httpx's transport loads the CA
bundle (about 0.2 s, 2026-10-18); a client with a transport of its own is built in under a
millisecond. Built with the definitions, six would be built at every load and every run's
reconstruction, though most runs fetch nothing.

**A client with its own transport reads no proxy from the environment.** httpx reads
`HTTPS_PROXY` and the like only for the transport it builds itself. Nothing here uses a proxy; a
deployment behind one must give it to the inner `httpx.HTTPTransport`.

**s3fs is configured from `LakeStorage` directly** (`s3fs_options`): key, secret, endpoint,
region, botocore's checksums only when required (for s3mock), and no listing cache. dlt dropped
`lake.s3.url_style`; it is now passed as botocore's `addressing_style` (`vhost` becomes
`virtual`), so s3fs uses the production example's virtual-host style as well. Checked against
s3mock: the live archive lists, and a scratch snapshot was written, read back and deleted.

**A dbt source other than `archive` is rejected when the definitions load.** Such a source was
keyed as the raw table dlt would load. Nothing loads one now, so it would be an asset that no step
fills.

## Lineage in Marquez (2026-10-18)

**Each code location sends its own lineage, through a sensor of ours, not dagster-openlineage.**
The plan named dagster-openlineage (0.2.1) in its storage-wrapper mode. Read against Dagster
1.13.26, the wrapper delegates only `EventLogStorage`'s abstract methods, so the Postgres storage's
own `supports_global_concurrency_limits` falls back to the base class's `False`: every pool (one
writer per lake table) would silently stop limiting. `get_maximum_record_id` would raise, and
`dispose` and `optimize_for_webserver` would do nothing. Its sensor mode avoids that, but tails the
whole instance, so defined in both locations it sends every event twice, and by default reads 30
events every five minutes. Both modes send a materialization with no inputs (the adapter's
`upstream_asset_keys` is never passed), so Marquez would draw each asset alone, unconnected; both
give every asset of a Dagster run that run's id, which OpenLineage reserves for one job's run; and
a runless materialization (the archive index's) has no run id, which Marquez refuses. The official
`openlineage-dagster` stops at Dagster 1.6.9.

**The sensor reads each of its location's assets' materializations, by asset.**
`fetch_materializations` per asset key, not the deprecated instance-wide `get_event_records`.
Reading by key keeps a location to its own assets (keys start with the lake), so the two sensors
never send the same event, and it includes the archive index's runless reports, without which the
fetch → archive edge would be missing. The cursor is the last storage id sent per asset; at most
500 events go per tick, so a first tick's history (about 100 per lake) fits in one, and a larger
one in several.

**An asset is an OpenLineage job of its own, reading its parents.** The job and its output dataset
are both named by the asset key, in the namespace `leads`. One job per dbt step would give a step's
every output the union of its inputs. Each materialization is one run, its id a UUID5 of the
Dagster run, asset and partition, so a resent event is the same run in Marquez.

**Failures are not sent.** Dagster OSS 1.13.26 defines `ASSET_FAILED_TO_MATERIALIZE` but nothing
emits it: the local instance had 18 step failures and none of these events. Inferring failures
from planned materializations of failed runs was left out; Dagster shows them.

**openlineage-python for the event model, the pipeline's httpx client for sending.** The client
library's typed events and facets serialize to the spec; requests go through `build_http_client`,
configured by `lineage.http` like every source. 1.53.0, not 1.54.0 (six days old). It brings
pydantic's httpx2 fork, which nothing here uses.

**Marquez refusing an event (4xx) skips it; Marquez unreachable or failing (5xx) stops the tick.**
A refused event would be refused again and hold back every later one; an outage passes, and the
next tick resends from the cursor.

**Column lineage is on** (`fetch_column_metadata(with_column_lineage=True)`; it was off, with no
reason recorded). It is why Marquez is here: Dagster OSS cannot show it. Checked first without a
build, by running dagster-dbt's lineage builder over each replay model's compiled SQL and its
parents' recorded columns: every staging and intermediate column traces (141 of 141 across seven
models). Raw does not: its SQL reads the archive's files (`_meta_file`, `_data`), which sqlglot
cannot resolve, so dagster-dbt logs a warning per raw model per build and attaches none. Column
lineage starts at raw, as planned. Rebuilding replay's staging and intermediate models then gave
Marquez 177 column edges, all linked, `int_google_search_areas.declared_month` back to raw's
`Mėnuo (month)`.

**Column lineage names columns as their tables have them.** dagster-dbt lowercases every column it
names in lineage (sqlglot's normalization), while raw keeps the source's headers (`Draudėjo kodas
(code)`), so its lineage named columns no table has. The sensor renames each to its table's own
spelling, from the parent's last recorded schema, matched case-insensitively. Whether Marquez would
have linked the lowercase names was not tried.

**Column descriptions come from the asset's definition, types from the build.** dagster-dbt keeps
two column schemas: the definition's, from the dbt YAML, described but untyped (`?`, as types are
written only in the SQL); and each build's, read from the table, typed but undescribed. Dagster's
UI merges them; the sensor sent only the build's, so Marquez had no descriptions. It now describes
each built column from the definition, matched by lowercase name. Datasets sent before were resent
once (each asset's latest event, by stepping the sensor's cursor back), which adds a dataset
version in Marquez for the same run.

**Each dbt build keeps its compiled SQL (`compiled_sql`), which Marquez shows with the run.** How a
column is computed is the model's SQL. Marquez 0.51 renders a job's `sql` facet on its runs, and
has a column lineage view, but shows no per-column transformation: OpenLineage's column lineage can
carry one (`transformations`, a type and a description), which its column view never displays, so
none is sent. The SQL is read from the build's `target/compiled/` (refs resolved, ephemeral models
inlined, a raw model's snapshot paths), so it is what built that version; it holds paths, no
credentials. Models built before have none until their next build.

**A table's checks are its quality in Marquez.** dagster-dbt makes each dbt test an asset check,
whose result names the materialization it checked. The sensor sends a materialization with the
results of its run that checked it, the last of each check, as a `dataQualityAssertions` facet.
OpenLineage defines that facet as an input dataset's (a job asserting what it read); Marquez reads
a dataset's quality from the dataset's own facets, and a dbt test checks the table its model has
just written, so the facet goes on the output dataset.

**A materialization is sent once its run has ended.** dbt tests a model after building it, so a
materialization sent mid-run would go without its checks. An asset's materializations wait from
the first whose run is still going; a long run (Google's sweep) delays its assets' lineage until
it ends. A check evaluated in a later run of its own is not sent: it is in Dagster.

**The Datasets list's Quality column stays N/A.** Marquez 0.51's list endpoint returns every
dataset's `facets` empty (the schema too), while its dataset endpoint returns them; the list page
reads quality from the list. The dataset's own page shows it. Nor does the lineage graph's node
carry facets, so its status dot stays blank.

**Raw has no checks.** The two singular Sodra tests (`tests/sodra_*.sql`) check
`sodra_monthly_files`, an ephemeral model, which is no Dagster asset, so dagster-dbt attaches
them to none; they fail raw's step when they fail, but appear in neither UI.

**Marquez 0.51 shows some facets elsewhere.** The documentation facet does not fill a dataset's
description field (it is in the facets panel), and the nominal time is listed in a run's `args`, not
its nominal time fields.

**Retired asset keys are not sent.** Materializations of keys no longer in a location's graph (the
VMI register's `archive_vmi`, `raw_vmi_export__*` and the like, 65 in replay) have no node to read
parents from.

## Fresh lakes (2026-10-18)

**DuckDB's spill folder's parent is created before dbt runs.** The profile's `temp_directory` is
`target/duckdb_tmp/<invocation id>`; DuckDB creates that folder when it first spills, not its
parent. On a fresh checkout the first build too large for memory (VMI's register, 350 MB) failed
with "Failed to create directory"; smaller builds never spilled, so nothing showed it before.
`ensure_temp_directory` creates it wherever dbt is launched: the code location's load, `owl-dbt`,
the dev target and the lake MCP server.

**Seeds build themselves (`build_static_data`).** They had the shared step's `rebuild_on_change`,
which has no catch-up, and a seed reads nothing, so it is missing from its first evaluation on,
which `eager()` leaves. Nothing built them on a fresh lake, and `int_google_search_areas`, then
Google's plan, waited for them for good (live's were built by hand on 2026-10-18). They now have
raw's catch-up too, which is safe as they have no parents. It is the asset's first evaluation ever,
so an instance that evaluated the seeds before (replay's on this machine) does not catch up: build
them once by hand there.

**A model waits until every parent has data (`deps_have_data`).** `any_deps_missing()` holds back
only for a parent's partitions that exist, and a source with no snapshot has none, so VMI's
staging, which reads both VMI sources' raw, started on the register's first snapshot and failed on
the daily changes' missing table (DECISIONS, VMI source). dbt cannot order this: the two are
separate invocations, and a unit test reads its parents' columns from the database whatever the
YAML declares (dagster-dbt passes none; only `format: sql` fixtures skip it, and the model would
fail next). So `eager_or` also requires `all_deps_match(~missing() | will_be_requested())`, which a
parent with no partitions fails, as there is nothing to match. A parent requested on the same
tick counts, as in `any_deps_missing()`, or a chain of missing models (`stg_vmi__changes`, then
`stg_vmi__current_register`) would take a tick per model. Built from Dagster's own conditions, it
runs in the daemon; a Python condition would need the beta `use_user_code_server`.

**Intermediate models take their folder's group.** `int_google_search_areas` was in the lake's
group itself, alone outside every box. It is in `models/intermediate/google/` and drawn in
`<lake>/google`, beside Google's source, whose plan reads it, as a staging folder of no source's
name is drawn in a group of its own name. The shared step's models in a folder of their layer get
that folder's group; its keys did not change, so no history moved.

## Streaming downloads (2026-10-18)

**A file too big for memory is streamed into the archive, never held whole.** Every source held
its downloads whole (`Download.body`), and the retrying client reads each body whole by design
(dlt removed). The Employment Service's vacancy export is about 2.5 GB, and the machine has well
under 1 GB to spare, so `StreamingDownloader.stream(url, write)` hands the response to `write` as
an `OpenedDownload` (URL, ETag, Last-Modified and the body as an iterator of 1 MiB chunks), and
`RawArchive.save_stream` writes those chunks to the snapshot's file as they arrive, hashing as it
goes, and returns a `StreamedDownload`: a `Download`'s record (size, SHA-256) without its body.
Manifests, sidecars and the fetch's metadata take either (`FetchedFile`). On S3 s3fs uploads the
file in parts as it is written, so one block is held at a time. Nothing goes through local disk:
a first design (on the financial statements branch) streamed to a temporary file and copied it,
which needs the free disk space a big export takes and writes it twice.

**`write` decides, before reading, whether the body is wanted.** The portal's cache can send an
export from before the newest change (VMI source), for a big one even months old (the vacancies'
cached copy on 2026-10-18 was from 2026-07-09). With the ETag in hand before the body, a source
can leave a stale copy unread and ask again later, instead of downloading 2.5 GB to throw away.
`archive_streamed_exports` (`spinta/exports.py`) is the streaming `archive_exports`: each export
saved as it arrives, its first 64 KiB checked for a header with `_id` and a row
(`checked_head`), so an error page sent as a success still never completes a snapshot.

**A transient failure retries the whole download from its start, mid-body included.** The
streaming client (`build_streaming_http_client`) has no `RetryingTransport`, since a body half
read cannot be asked for again from inside the request. `retrying_whole_downloads` retries the
whole attempt instead, with the source's `<source>.http` policy: a timeout or dropped connection
before the body or while it is read, or a 429 or 5xx (raised by the client's hook), waiting what
Retry-After asks for or the policy's backoff, and each attempt paced by `min_interval_seconds`.
`write` is called again with a new body, so `save_stream` opens the file from empty each time;
anything else `write` raises (a failed check) fails the download at once. get.data.gov.lt drops
and hangs on large exports, and the vacancies' export is sent at about 1.6 MB/s, about 25
minutes, so a single drop would otherwise lose the fetch. The client is built per download, not
with the definitions, as building one loads the CA bundle (0.2 s).

**A streamed file replaces the archived one only once its body has arrived whole.** Closing an
s3fs file completes its multipart upload even when the `with` block exits on an exception, so
writing straight to the file let a body that broke off replace a complete one (on s3mock a
complete file became the 7 MiB that had arrived). If every attempt failed the snapshot's file was
partial, and a complete snapshot fetched again could be left with a file its manifest's size and
SHA-256 no longer describe. `save_stream` now writes `<name>.partial` beside the file and moves it
into place after the last chunk; on any error it removes the partial file and re-raises. A failed
cleanup does not replace the download's error, which is what the retry decides on: the next
attempt overwrites what is left. The sources' globs end in their extension, so none reads a
`.partial`.

**The move is a server-side copy, not an aborted upload.** Writing the file itself with
`autocommit=False` and committing it, or discarding it (aborting the multipart upload) on error,
would replace it in one step with no copy, and on S3 it would be the better fix. But s3mock 5.2.3's
AbortMultipartUpload deletes the object already at that key, so locally a body that broke off
would delete the complete file instead of keeping it. S3 has no rename: s3fs's `mv` is a copy and
a delete, a single CopyObject up to 150 MiB and above that a serial multipart copy in 50 MiB parts
(UploadPartCopy, so S3's 5 GB limit on a single copy does not apply), with no bytes through the
machine. On s3mock a 400 MiB file's move took 6.5 s, against 11 s to upload it; the vacancies'
550 MB gzipped export costs about 9 s and a dozen requests. A failed attempt still uploads its
last block (at most 50 MiB) as it closes, then deletes it. On a local archive the move is a
rename. Two costs are left: a process killed mid-download leaves an incomplete multipart upload on
the `.partial` key (S3 keeps its parts until a lifecycle rule aborts it), and once the archive's
bucket is versioned (`storage.toml`) each deleted `.partial` stays as a noncurrent version, a
second copy of every streamed file, unless the delete names its version or partial files move
under a prefix a lifecycle rule can expire.

**A name ending in `.gz` is gzip-compressed as it is written** (level 6, zlib's default:
Spinta's CSV exports shrink to about a fifth, 2.5 GB of vacancies to about 550 MB, at about
40 MB/s). Its size and SHA-256 are still of the body as sent, so two fetches of one version
compare equal however the file is stored. DuckDB reads `.csv.gz` as it reads `.csv`. The archive
otherwise keeps files byte for byte; compression is the source's choice, by the name it saves
under.

**`unquoted` moved to `spinta/exports.py`**, beside the exports whose ETags it reads, for both
the VMI register's check and the streaming sources'.

## VMI tax arrears source (2026-10-18)

**VMI's tax arrears are a source of their own, `vmi_tax_arrears`: the list of companies that owe
tax.** The dataset is `vmi/ja_nepriemokos` (data.gov.lt 1202, "Juridinių asmenų mokestinė
nepriemoka"), whose one model is `NepriemokosSuma`. On 2026-10-18 its export held 33,528 rows in
7.1 MB of CSV, one per company: `ja_kodas` is unique. Each row has VMI's record id (`id`), when VMI
created and last updated the record (`sukurta`, `atnaujinta`), the company's code, name and
municipality (`sav_kodas`, the Address Register's code; 12,971 rows are Vilnius city's, 13), and
four amounts in EUR. The catalog lists it as updated daily, from 2023-03-06, and says the amounts
are statistics taken from VMI's tax accounting system. They are not judged by VMI's rules for
deciding whether a taxpayer is in debt (order VA-67), and they leave out debts under enforcement
documents.

**There is no threshold: a company is on the list while it owes anything, and leaves it when it
owes nothing.** Amounts start at 0.01 EUR (median 10.98, p75 1,078, p90 7,639, p99 118,638, max
17.4 M; 325 M EUR in all), and records appear the day they are created. No row and no change in
the whole history has zero arrears. A company that pays off is deleted, and when it owes again it
comes back on a new record, with a new `id` and `sukurta`. So `sukurta` is when the company came onto
the list, and 2023-03-06, the dataset's start, means at least that long. The four amounts add up
on every row of the export and every row of the monthly model: `nepriemoka` = `pradelsta_nepriemoka`
+ `atideta_nepriemoka` − `atideta_pradelsta`. The overdue amount includes the deferred amount that
is overdue. 1,225 rows owe nothing overdue, only deferred amounts (2,824 have a deferral).

**Its history is only in the changelog, which starts with the list's first load, so the source is
the changelog from its first change.** An export is the list as it stands. The changelog
(`/:changes`) starts at change 1, made 2024-11-18 21:10 UTC, when the list was first loaded into an
empty model, and holds every change since: 3,278,756 by 2026-10-18 06:02 UTC. Folding it from
change 1 gives the list at any moment since. That history is what the risk models need: months in
arrears, whether a debt grows, and what a company owed before it closed. An export, even a daily
one, would start the history at the first fetch. So there is no baseline export, unlike
`vmi_monthly_register`: the snapshots from the first on are the state. A snapshot missing from the
start would leave rows without the insert that names their company, which `company_code`'s
`not_null` test on the changes would catch.

**Monthly, on the 21st at 06:00 with the other monthly sources, keyed `YYYY-MM`, with history from
2024-11.** The list is reloaded on most mornings between 06:00 and 08:45 UTC: from 2025-11-01 to
2026-09-19 there were changes on 166 of 323 days, and none from 2026-06-08 to the 2026-06 tick.
The export's `Last-Modified` and ETag are those of the newest change, as with JAR and VMI. The
changelog keeps every change with its time, so a snapshot's period only decides how fresh the
newest state is. The leads are monthly: a snapshot at their tick has the list of the 20th, as the
21st's reload comes after 06:00 Vilnius time. Daily snapshots would give the history about 700
partitions and 700 raw runs, and 30 runs a month for nothing the leads read. The cost is that the
newest state can be up to a month old (on 2026-10-18 it is the 2026-09 tick's). Snapshot M holds
the changes made from the tick of M−1 to that of M, as `vmi_daily_changes`' do for days. Its fetch
takes a period (`history_start` is `vmi_tax_arrears.first_snapshot`, 2024-11, whose window from
2024-10-21 holds change 1). It reuses `spinta.Changelog` unchanged, with its own few-line archive
function, like `vmi.daily_changes`'.

**The publisher changed how it writes changes several times, and the fold handles every way.**
From November 2024 to January 2025 every change was a delete of the row and an insert of a new one:
328,068 changes in snapshot 2024-12 alone. Then there were patches; from June to October 2025 upserts,
which hold every field for a row new to the list and only the fields they set for one it holds
(550,534 in all); then patches again. The first loads (2024-11-18 to 11-19) loaded the list twice,
with duplicates, and deleted one copy. In 2024-11 to 2026-09 there are 3,135,341 changes:
1,151,955 patches, 747,168 deletes, 685,684 inserts and 550,534 upserts. Spinta's row id (`_id`) is
therefore not a company's: a company's rows over time are joined by `ja_kodas`, which only inserts
and upserts of new rows carry. In the first loads' 11 rows (2024-11-18 to 11-26), the code and name
are published encrypted (`BELLASO::...`) or hashed. They have no company, and all were deleted by
11-26.

**Two artefacts of VMI's reloads are folded away.** At the 2025-07 tick, 1,880 companies held two or
three rows. The older records were left over from the upsert era and deleted the next morning
(all 1,899 on 2025-07-22), so the monthly model keeps each company's newest record (latest
`atnaujinta`, then highest `id`). In March 2026 VMI deleted rows on 03-12 and 03-13 and inserted
them again as new records up to 03-23, across the tick: the 2026-03 tick found 17,686 companies
where its neighbours found about 30,000, and 11,877 companies were on the list in February and
April but not at March's tick, all on records created in March. So `months_listed` counts the
snapshots in a row in whose month the company was on the list at any time, not only at the tick,
and the previous amounts are those of the last tick in that run that found it. With it, 2026-04's
companies new to the list fell from 17,340 to 2,722. `sukurta` cannot tell this apart: the reload
reset it.

**One amount is an error at the source.** At the 2025-08 tick an individual enterprise (305636578)
owed 935.8 M EUR, three times the rest of the list together, and was gone by the next tick. The
amounts are VMI's as published; risk models should not sum them unbounded.

**Pages of 20,000 changes, read with a 16 MB buffer.** A page of inserts is up to 9.4 MB (one of
patches about 3 MB); a month is 4 to 17 pages, the whole history 168 pages and 996 MB of JSON.
`archived_json_entries` set `maximum_object_size` to 64 MB, and DuckDB holds twice that per thread
reading a page, so eight threads reading a month's pages asked for 1 GB and failed (tested on
2026-10-18). The macro now takes `maximum_object_size` as a parameter, with the same default, which
this source sets to 16 MB. It is a shared macro; nothing else changed in it.

**Raw read from nothing needs three settings of its own to stay near a gigabyte.** Built from
nothing, raw reads all 168 pages in one query. At the worktree's 1 GB limit it passed 1.5 GB of
memory. DuckDB guesses the pages fewer than their sidecars, so it built the join's hash table on
the pages (every change's JSON) rather than on the sidecars. JSON parsing holds memory beyond the
limit, more the more threads read. Keeping the pages' order buffers what is read. The model's
hooks keep the join's sides as written (`disabled_optimizers = 'build_side_probe_side'`), read at
two threads and drop insertion order, and reset all three after. At a 700 MB limit it peaked at
1.3 GB and took 28 s. The changes model and the monthly model, at two threads, peaked at 1.1 GB
each. At config/local's 2 GB limit expect about 2.5 GB for raw's first build. A raw build of one
month's snapshot, the usual run, reads 4 to 17 pages.

**A build that spills cannot run another query in the same dbt process.** dbt-duckdb sets the
profile's settings, `temp_directory` included, on every new cursor. Once DuckDB has spilled,
setting it again fails ("Cannot switch temporary directory after the current one has been used"),
so the tests after a model that spilled fail, though the model is built. On the dev target the
models were built and tested in separate invocations. Under Dagster a step builds and tests its
models in one invocation, so a build of this source that spills fails its tests there. Not fixed: it is the profile's, shared by every source.

**Staging: every change typed, and the list at each tick per company.** `stg_vmi_tax_arrears__changes`
has one row per change. It holds the fields the change set, typed (amounts `decimal(18, 2)`), and
`changed_fields` to tell a field set empty from one not set. Each row has its company from the
change or the row's insert. `stg_vmi_tax_arrears__monthly` has one row per company on the list at a
snapshot's tick (company_code, snapshot_month), the grain of `stg_jar__entities`. It folds the
changes up to each tick: each field's last value set, wrapped so that an emptied field is kept. It
adds `months_listed`, the previous amounts and their changes (the trend), and `list_changed_at`,
the newest change before the tick. In the live dev build from 2024-11 to 2026-09 it has 728,906
rows of 93,249 companies, 17,686 to 40,912 a tick. At 2026-09's tick 35,397 companies were on the
list (13,686 in Vilnius), owing 328 M EUR. 10,946 of them had been on it for all 23 months.

**The code is JAR's but for foreign companies.** 10-digit codes (471 on 2026-10-18, 432 of them
starting with 9) are VMI's own for foreign companies. JAR does not hold them, which is why the DSA
dropped its reference from `ja_kodas` to JAR in 2023 (manifest commit `a540597`). Of 2026-09's
35,397 companies, 34,897 are in `stg_jar__entities`' newest snapshot (98.6%): all 34,898 9-digit
codes but one, and none of the 499 10-digit ones. 1,032 of them are deregistered in JAR. Every
municipality code is in the municipalities seed.

**In Vilnius, 736 companies in the leads' categories were on the list at 2026-09's tick.** The
category comes from Sodra's newest month (EVRK 2.1) or else the company's latest main activity in
the VMI register (EVRK 2). There are 385 cafés and restaurants, 204 car repairers and 147 hair and
beauty businesses; 504 of them are Sodra employers. 209 owe at least 1,000 EUR overdue and 51 at
least 10,000. 500 have been on the list six months or more, and 170 of those owe more overdue than
at the tick before. Their arrears: p25 2.19, median 246, p75 2,162, p90 8,705, p99 83,288 EUR, max
1.73 M; 4.6 M EUR in all. Cafés' median is 670 EUR, car repairers' 93 and hair and beauty's 4.62.

**The history was fetched in paced runs, 259 requests in all.** The source's own code fetched
2024-11 to 2026-09 on 2026-10-18 into the live archive. It took three runs, each one search for
its first window (about 20 requests of one change) and then each month's pages. The runs paused 15
to 45 s between months, and the first was stopped after four months to free the shared lock.
Every request was answered in under 18 s; the portal never blocked.

**The replay sample is one in eight of the sample's companies, with their whole history.** The
history of all 1,847 JAR sample companies is 92,952 changes and 29 MB: 1,503 of them have been on the
list. So the sample keeps the 251 whose code is divisible by 8, every change of their 2,682 rows in
all 23 snapshots: 13,483 changes, 4.4 MB. 184 companies are in its monthly model, 94 at 2026-09's
tick. Pages are re-serialized after the cut, as `vmi_daily_changes`' are. A page with none of
their changes is dropped, from the manifest too. Sidecars and manifests keep the fetch as
recorded.

## VMI taxes paid source (2026-10-18)

**The taxes each company paid to VMI are a source of their own, `vmi_taxes_paid`.** The dataset is
`vmi/ja_mokesciai` (data.gov.lt 673, "Juridinių asmenų sumokėti mokesčiai"), whose one model is
`Moketojas`. Its catalog entry says the sums are the taxes and levies of Article 13 of the Law on
Tax Administration that legal entities paid into VMI's budget revenue collection accounts, the
current year's counted cumulatively from January to the end of the reporting month, updated once a
month. So `suma` is every tax VMI administers (VAT, corporate income tax, the personal income tax
an employer withholds, excise and the rest), not Sodra's contributions. It is net of refunds: a
company's year to date falls from one month to the next in 24,708 of 2,022,950 company-months
(1.2%). There is no threshold beyond paying something: the smallest sum is 0.01 EUR and none is zero
or negative, so a company that paid nothing that year has no row. On 2026-10-18 the export held
742,760 rows, one per payer and year from 2021 to 2026, each past year loaded in one batch (2025's
130,766 rows on 2026-01-26) and a year's December row its total: 19.58 bn EUR for 2025.
`praeitu_metu_suma` and `einamu_metu_suma` are empty on every row and every change. Ponama's 2025
is 117,547.28 EUR, as on the portal.

**The company code is published: `mm_kodas._id` is the JAR entity's `_id`.** The DSA has referred
`mm_kodas` to `rc/jar/iregistruoti/JuridinisAsmuo` since 2025-04-18 (manifest commit `5944790`;
before, to the VMI register), so a lookup in `MmKodas` or the VMI register finds nothing. Of the
export's 742,760 rows, 734,719 (98.9%) refer to an entity, and every one is in
`raw_jar.entities`' 2026-09 snapshot. The catalog's distribution "ja_mokesciai klonas su JA kodu"
is the same model with the reference resolved by the portal
(`select(..., mm_kodas.ja_kodas, savivaldybe.pavadinimas, ...)`): its codes are the same on every
row, but the export took 3.5 minutes against 4.6 s, so the fetch takes the plain changes and
staging resolves them in JAR. `savivaldybe._id` is the Address Register's `Savivaldybe` (all 60
resolve; 13 is Vilnius city) and `apskritis._id` its county, which the municipality implies.

**A row that refers to no entity is matched by its exact name, JAR's first, then the VMI
register's.** Names are compared with every kind of double quote as `"` and whitespace as one space
(`normalized_name`), nothing else, and a name two companies share matches neither. The rows without
a reference are mostly foreign companies (no legal form; 18,356 of the unmatched company-months are
registered in Vilnius) and the state and municipalities as such ("Juridinis asmuo per se"), which
JAR does not hold. The rest are companies newer than the portal's JAR at the load: 1,026
company-months match a JAR name (2026-05's load had 444) and 3 the VMI register's, which holds
companies registered since JAR's last publication (36 such rows are in the 2026-09 load, not fetched
yet). The municipality is not matched on: JAR's seats have not changed since 2025-09-03, so the
newest companies have none, and the VMI register holds where activities are carried out, not the
seat. In all, 2,022,950 of 2,043,019 company-months (99.0%) have a code, 725,278 of Vilnius's
743,669 (97.5%).

**Its history is the changelog: each monthly load inserts every company's row whole and deletes the
previous load's.** A load inserts (upserts until 2025-09) one new row per company with every field,
the year to date as of a month, and deletes the rows of the month before; a January load adds the
past year's total. The rows are new each time (a new `_id` and VMI `id`), so a company is followed
from month to month by its code. From 2025-06 to 2026-09 every month from 2025-04 to 2026-08 was
loaded exactly once, each with one `atnaujinta`, and each month's rows were deleted exactly when the
next month's arrived; the newest of each year (2025-12, 2026-08) stand. Loads reach the portal one
to three weeks after VMI's date: 2026-03's (`atnaujinta` 2026-04-13) on 2026-04-27 to 05-09, some
spread over several days, and two in one month when one is late (2026-07's on 2026-09-07, 2026-08's
on 09-18). The users' comments on the catalog asked for July 2026 for weeks before it came. Besides
loads, the only patches point `mm_kodas` at a JAR entity (603 on 2026-03-09, 807 on 2026-09-01). The
load of 2026-01-26 also deleted 107,397 rows inserted before 2025-05-21, which are not known here.
No past year was reloaded in the history; the export says 2021 was last loaded on 2024-03-27.

**Monthly, on the 21st at 06:00, keyed `YYYY-MM`, with history from 2025-06.** Snapshot M holds the
changes made from the tick of M−1 to that of M, as `vmi_tax_arrears`' do, and its fetch takes a
period (`vmi_taxes_paid.first_snapshot`). Each month's leads then see every load made by the 21st;
a load after it waits a month (2026-09's, made on 2026-10-17, is in snapshot 2026-10, so on
2026-10-18 the newest month staged is 2026-08). The history starts at 2025-06, whose window opens
on 2025-05-21: the changelog starts on 2024-02-14 (change 92,785), but from 2024-08 to 2025-04-23
it holds about 35 M patches pointing `mm_kodas` from the VMI register to JAR, too many to read, and
loads before them refer to the register's rows. The first month staged, 2025-04, was loaded on
2025-06-03. It reuses `spinta.Changelog` unchanged, with its own few-line archive function.

**No export is fetched, so the years before 2025 are not staged.** Each load holds every row whole,
so the changelog alone gives every month since 2025-04 and the years then current. An export would
add only 2021 to 2024's totals, at 235 MB a month for rows that do not change, and fetched as a file
of a source with history it would make two kinds of snapshot (VMI source). If the risk models want
those years, one export is enough.

**Pages of 20,000 changes, read with a 16 MB buffer, and raw as `vmi_tax_arrears`' raw is read.** A
page of a load's inserts is about 7 MB; a month is one or two loads, 12 to 26 pages, and the
history 4,045,031 changes in 210 pages and 1.62 GB of JSON. `archived_json_entries` takes
`maximum_object_size` as that source added it, set to 16 MB here, and the raw model has its three
hooks (the join's sides as written, two threads, no insertion order). Built from nothing at a 700 MB
limit, raw read the whole history in 44 s and peaked at 1.26 GB. The usual build is one month's
snapshot.

**Staging types each load's inserts and applies only the patches' fields.** A first version folded
every change field by field (`arg_max` over each field's JSON, as `stg_vmi__current_register`
does), which ran out of memory at 700 MB over 4 M changes. Each row is inserted once (`row_id`'s
unique test) and patches are few, so the inserts are typed as they are and a patch's fields
replace theirs. The company match joins on the distinct unmatched names only: written as a join
condition with the reference's null test, DuckDB had not finished after ten minutes. The monthly
model builds in 24 s, peaking at 1.2 GB. Builds that spilled then failed dbt's next query with
"Cannot switch temporary directory" (vmi_tax_arrears source); the models were built and tested in
separate invocations.

**Two models: per company and month published, and per company and year.**
`stg_vmi_taxes_paid__monthly` has one row per load's row: the company (code and how it was found),
name, legal form, municipality, `tax_year`, `through_month`, `year_to_date_eur`, and `paid_eur`, the
year to date less the company's previous month published that year, from `paid_from_month` (1 for a
company's first month in a year, so 2025-04 covers January to April). 2,043,019 rows of 150,088
companies; companies are unique per year and month. A payer with no code cannot be followed from
load to load (two foreign payers of one name and municipality were in one load), so it has no
`paid_eur`. `stg_vmi_taxes_paid__yearly` is each company's newest month each year, `is_full_year` for
a December: 269,836 rows, 130,766 of them 2025's totals. Months paid in total about 1.5 bn EUR,
2.1 to 2.3 bn in June and December.

**In Vilnius, 95% of the leads' employers have taxes paid to 2026-08, and 91% of those whose wages
Sodra hides.** Of the 1,548 Vilnius employers in Sodra's 2026-08 in the leads' categories (EVRK 2.1),
1,473 have a 2026-08 row and 1,519 one in some month: cafés and restaurants 733 of 759, car repair
495 of 532, hair and beauty 245 of 257. Of the 640 with three or fewer insured, whose wages and
contributions Sodra does not publish, 580 have one. Their January-to-August medians: cafés 27,074
EUR, car repair 11,839, hair and beauty 2,558. A company with no row paid nothing that year, or
its row refers to no entity under a name that matches none.

**The history was fetched in four paced runs, 331 requests in all.** The source's own code fetched
2025-06 to 2026-09 on 2026-10-18 into the live archive, a search for each run's first window (about
25 requests of one change) and then each month's pages, one request a second. Every request was
answered; the portal never blocked. A run held at most two loads' pages, peaking at 485 MB.

**The replay sample is one in eight of the sample's companies, with their whole history**, as
`vmi_tax_arrears`' is: the 251 entities in `replay/jar/2026-09` whose code is divisible by 8, and
every change of their rows, 7,093 changes in 208 pages and 2.9 MB over all 16 snapshots (2025-10
has no load, so no page). 244 of them paid something, 3,777 rows of the monthly model. Pages are
re-serialized after the cut; sidecars and manifests keep the fetch as recorded.

## UZT vacancies source (2026-10-18)

**The Employment Service's vacancies are a source, `uzt_vacancies`, for the job-ad risk models.**
`datasets/gov/uzt/ldv/Vieta` has one row per vacancy registered with Užimtumo tarnyba since 1990
(3,230,766 on 2026-10-18, posted 1990-04-06 to 2026-10-16), by legal entities, farmers and people
with a business certificate, with the pay offered, the profession, the workplace and the
employer's JAR code. A company advertising a cook at 1,800 EUR while declaring an average wage
near the minimum to Sodra (`job_ad_wage_gap`), or hiring month after month while its insured
count stays flat, is what the leads look for.

**A monthly snapshot of the whole export, not the history through the changelog.** Each export
holds every vacancy as it stands, its validity as it ended or was last extended and its final
status, so one snapshot is the whole history the risk models need. The changelog starts only on
2024-11-18, when the dataset was first loaded, and its 25 M change ids are mostly bulk loads: the
first insert of every row (November 2024), patches of every seat and requirement (January 2025),
an upsert of everything (May 2025), every row deleted and inserted again with new `_id`s
(2026-01-21 and 22), professions reclassified (April 2026) and the pay comments rewritten
(2026-10-14, about a million patches). Reading it would be 15 to 20 GB of JSON for little. What
three months change, the export of 2026-07-09 against 2026-10-18's by `darbo_vietos_id`: 6,190
vacancies added and 179 gone, and of the 3,224,576 in both, 1,175,360 revised: 1,068,908 pay
comments and 129,993 seats (the bulk patches), 10,140 statuses (3,760 from published to
archived), 9,977 filled flags, 7,610 validity ends (648 extended), 6,241 pays and 6,043
professions. The data changes every night at about 01:30 to 02:40 UTC; monthly, on the 21st at
06:00 with the other monthly sources, is enough for monthly leads.

**The export is fetched in pages of 400,000 rows, each streamed into the archive gzipped
(2026-10-19).** First it was streamed whole (Streaming downloads): 2.33 GB as sent, 533 MB as
`Vieta.csv.gz`, from the portal's application at about 1.6 MB/s, about 25 minutes, and from its
cache in 139 s. 25 minutes is about when the portal drops a response (JAR financial statements
source), and the backend sends at 0.6 to 1.6 MB/s, so the whole export only worked because the cache
held it, and the cache can hold a cut-off copy as if it were whole. It is now fetched as the
financial statements are, by the shared `spinta.pages.archive_pages`: the count (`?count()`), then
pages of `uzt_vacancies.page_rows` (400,000) rows, `Vieta.csv.gz`, `Vieta.2.csv.gz`, ..., each
checked for its rows, the next page's token on its last row and the counted ETag, the pages adding
up to the count; raw reads `Vieta*.csv.gz`. The model pages by `darbo_vietos_id` and then `_id` (the
token is `["DV-01-000000006", "<_id>"]` base64-encoded). Fetched on 2026-10-19 into a scratch
snapshot (since deleted): 3,231,273 rows, the count exactly, in nine pages, eight of 400,000 rows
(283 to 326 MB, 191 to 207 s, 1.44 to 1.71 MB/s) and one of 31,273; 2.42 GB as sent in 26 minutes of
transfer, 537 MB stored. The largest page would take 9 minutes at 0.6 MB/s, well under the 25. The
whole fetch at that rate takes 70 minutes, and a stale answer waits five, so the fetch job's
`dagster/max_runtime` is two hours (`uzt_vacancies.fetch_max_runtime_seconds`).

**An export is archived only when its ETag is the changelog's newest revision, and a stale one
is left unread.** On 2026-10-18 the plain export (`/:format/csv`) was the cache's copy of
2026-07-09 (3,224,755 rows, posted up to 2026-07-08), sent as STALE and then UPDATING for more
than a quarter of an hour. The fetch reads the newest change, then the export's headers; a copy
from before that change is closed before its body, and asked for again after
`uzt_vacancies.export_wait_seconds` (300), up to `export_attempts` (6) times, as the VMI register
is.

**Paged, the count is held to the changelog's newest revision before any page is asked for
(2026-10-19).** `archive_pages` takes the version the export must be: a count of another, or a page
of another than was counted, raises `ChangedExportError`, and the vacancies' fetch asks for it all
again, from the changelog's newest change, after `export_wait_seconds`, up to `export_attempts`. A
stale count costs one small request; a stale page (the first page's URL is the same every month, so
the cache may hold last month's) is read before its ETag is compared, as the statements' check does,
which from the cache takes seconds. A page cut off still fails the fetch, as there. The cache holds
the changelog's newest change too: on 2026-10-19 at 07:52 UTC `/:changes/-1` answered with
2026-10-18's (`18acf9bb`), while the count was the night's (`e9bee609`, Last-Modified 01:33:45);
after the five minutes' wait both were `e9bee609` and the fetch went on.

**The employer's contact people are never fetched.** The export's `select()` names every column but
the contact person's name, phones and e-mail (`darbdavio_kontaktinis_asmuo`, `darbdavio_tel_nr`,
`darbdavio_mob_nr`, `darbdavio_el_pastas`) and Spinta's `_base`, so neither the archive nor the lake
holds anyone's contact details. Natural persons are published as "Fizinis asmuo", never by personal
code. Staging also leaves out the free-text description and requirements, which may name people and
which nothing reads; they stay in raw. A column the portal adds is not fetched until it is listed
(`COLUMNS`). Paged (2026-10-19), the selection keeps them out too: Spinta takes `select()` with
`limit()` and `page()` (a probe's second page went on from the first's last row), but a selection
drops `_page.next`, which the paging reads, and naming `_page.next` in it broke the response off
after its headers. Naming `_page` gives the column, filled on the last row as in a full export. So
the selection starts with `_type` (rows are counted by it) and holds `_page` (`page_selection`
refuses one that does not).

**Staging keeps the euro years: vacancies posted from 2015.** Raw keeps the whole export; the
1,546,981 vacancies posted before 2015 state pay in litas, mostly not at all, and are older than
Sodra's files. The 55 of 2015 in litas are converted at the fixed 3.4528. 1,683,785 vacancies are
staged per snapshot.

**Pay is staged as published and per month.** `vid_darbo_uzmokestis` is the pay offered or its
lower bound, `maks_darbo_uzmokestis` its upper bound (never below it), both per month or per
hour; `prelim_darbo_uzmokestis` is the service's month's pay for the hours advertised: an hourly
rate times 168 (40 hours a week, 10,815 of 2025-26's), 84, 126 and so on. A pay under 50 EUR is
per hour (82,558 vacancies; of the 19,014 posted in 2025-26 under 50, only 18 have a month's figure
equal to it and 1,759 none, while nearly all of 50 to 300 equal it: part-time monthly pay). `monthly_pay_from_eur` and `_to_eur` are
the month's figures either way. The amounts are gross: in 2025, 13,927 monthly offers were within
2% above the minimum wage (1,038 EUR), and 7,373 of 114,017 below it, most of them part-time. 20%
of staged vacancies state no pay, nearly all from 2015 to 2017.

**The workplace is split into the Address Register's components.** "Savanorių pr. 192, Kaunas
44151, Lietuva" gives the street as an address writes it (Savanorių), its kind (pr.), the
building's number (192, a premises after a dash dropped), the settlement and the postcode; the
municipality's code comes from the municipalities seed by its published name. Of the 17,627
vacancies in Vilnius city posted since 2025-10-01, 14,822 name a street, all but 5 a street of
the register's, and 9,784 a number, 9,721 (99.4%) a building of the register's. 19,733 vacancies
(nearly all since 2023) give no place at all (", Lietuva").

**Many workplaces are not the employer's registered office (`workplace_is_seat`).** The seat
(`darbdavio_bustine`) is parsed the same way and compared on street, number and settlement (a
village only on street and number, as the seat writes it in the genitive). Of the vacancies
posted since 2025-10-01 where both name a street and a number, 24,653 of 58,265 (42%) are
elsewhere; in Vilnius, 5,285 of 9,720 (54%). Matching a venue by its company's seat misses
these; the vacancy's workplace is a second address for the matching step.

**98.6% of the last 13 months' vacancies resolve to JAR** (`company_code` in the 2026-09
`stg_jar__entities`): 97,469 of 98,846 posted since 2025-10-01, and 1,642,101 of 1,683,785 since
2015 (138,316 of those to companies deregistered since). The rest are natural persons, farmers
(their farm's code) and pre-JAR codes of six to eight digits, none given a `company_code`.

**`int_uzt_company_months`: each company's vacancies by month, profession category and
workplace municipality.** A vacancy counts in each month from its first day to its last, up to
the export's month (one archived early may still say it runs until 2028), with its places and
pay: the lowest, median and highest month's pay offered. Professions get the leads' categories
from a seed, `profession_categories`: a prefix of the six-digit profession code (built on
ISCO-08) for cooks, chefs, kitchen helpers, waiters, bartenders and baristas (cafes_restaurants),
hairdressers and beauticians (hair_beauty), and car mechanics, body repairers and tyre fitters
(car_repair); masseurs, sauna and solarium attendants are left out of 5142, and 821905 (tyre
fitters before April 2026's reclassification, now wood product assemblers too) is left out. It
is built from the newest snapshot: 1,129,694 rows of 73,399 companies.

**What it shows for the leads**, Vilnius companies in the leads' categories in Sodra's 2026-08
(activity_categories, EVRK 2.1), with vacancies open from 2025-08 to 2026-08: cafés and
restaurants 134 of 759 (113 for the category's professions, 106 with the workplace in Vilnius),
car repair 40 of 532 (30), hair and beauty 8 of 257 (6). In the months a company advertised its
category's professions and Sodra published its average wage (more than 3 insured), the median
pay offered was above that average in 295 of 501 company-months for cafés and restaurants (the
median offer 1.06 times the declared average), 35 of 59 for car repair (1.22) and 8 of 8 for hair
and beauty (2.14); 79 of 103, 12 of 19 and 2 of 2 companies had at least one such month. Sodra's
average includes part-timers and the ads are gross for the hours advertised, so a gap is a sign
to weigh, not a finding.

**Since July 2026 the publisher loads few vacancies.** About 12,000 vacancies a month were
posted until June 2026; the 2026-09 snapshot has 3,468 for July, 2,395 for August, 1,115 for
September, and 31 open on the day of the export, against 3,648 open in the export of 2026-07-09.
The newest inserts are mostly vacancies already archived. Recent months are incomplete until it
catches up; the staging and intermediate models say so.

**The raw build fits in 700 MB with the VMI sources' settings.** Reading the 533 MB of gzip into raw
took 58 s on the dev target with DuckDB at 700 MB (`OWL_WT_MEMORY_LIMIT`), two threads, no insertion
order kept and the join's sides as written (model hooks); the process peaked at 1.19 GB. The run
then failed with "Cannot switch temporary directory after the current one has been used", after the
model had been created, the profile's spill-directory bug; builds and tests were run as separate
invocations. Built from nothing on 2026-10-19, raw read the paged scratch snapshot and 2026-09 (6.5
M rows) in 148 s, peaking at 1.17 GB at 700 MB; staging built both in 48 s at 500 MB, peaking at 816
MB, and the tests passed: 3,231,273 rows in the paged snapshot, one per `darbo_vietos_id`, 1,684,292
staged (1,653,776 with a company code), against 2026-09's 1,683,785.

**The replay sample is one in eight of the sample's companies** (code divisible by 8, as the VMI
tax samples): 4,559 vacancies of 160 employers, records byte for byte, gzipped as the archive
stores them (530 KB).

## JAR financial statements source (2026-10-18)

**One source, `jar_financial_statements`, of the register's four filing datasets.** The income
statements (`rc/jar/pelno_ataskaitos/PelnoAtaskaita`) and balance sheets
(`balanso_ataskaitos/BalansoAtaskaita`) are long: a row per line of a filed statement, keyed to the
filer by JAR's internal entity `_id` (`juridinis_asmuo._id`), with the period, the filing date, the
set of statements (`template_id`), the form (`standard_id`), the line (`line_type_id`) and its
amount. The non-filers (`fa_dokumentu_nepateike/Nepateikes`) list an entity and a year not filed,
the late filers (`fa_veluojantys/VeluojantisPateikti`) an entity only. All four refer to the entity
by `_id` alone, so `stg_jar__entities` gained one column, `entity_id`, its `_id`. On 2026-10-18:
6,485,210 and 9,626,523 rows (2.9 and 4.2 GB as CSV), 113,065 and 19,866.

**Each export is fetched in pages of 500,000 rows, each checked whole.** The whole export cannot be
fetched reliably. The portal's cache sent the income statements cut off at 3,178,077 of their
6,485,210 rows, ending on a whole row with a properly ended response, on every request since the
2026-03 load (`X-Cache: STALE`); 1.42 GB is about what the backend sends in 25 minutes. Asked past
the cache (`?sort(_id)`, a key the cache does not hold) the export came at 0.6 to 1.6 MB/s and the
connection was dropped at 2.36 GB after 25 minutes with curl, and on all five of the streaming
downloader's whole attempts (2 h 7 min in all). So the backend drops a response after about 25
minutes, and the cache keeps a dropped response as if it were whole. Spinta's CSV takes `limit(n)`
and `page("<token>")`, the token being the `_page.next` it fills on a response's last row
(`["<_id>"]` base64-encoded: paging is by `_id`), and `count()`. The fetch asks for the count
(`?count()`, a CSV of one row, with the dataset's ETag), then for pages of 500,000 rows (about 220
MB, 130 to 290 s each on 2026-10-19), each saved to its own file: `<dataset>.csv.gz`, then
`<dataset>.2.csv.gz`, ... (raw reads `<dataset>*.csv.gz`). A page must hold as many rows as asked
for (fewer only on the last), end with a whole row holding the next token, and be of the ETag
counted; the pages must add up to the count. A page dropped midway is retried whole by the
downloader; no page comes near the 25 minutes. A cut-off page, or the register reloading during the
fetch (a new ETag), fails the fetch, leaving no manifest. A snapshot is 35 pages and four counts; at
the slowest rate seen, 0.6 MB/s, 7 GB take over three hours, so the fetch job's
`dagster/max_runtime` is four hours (`jar_financial_statements.fetch_max_runtime_seconds`).
Responses made now have a `_base` column (empty) that the cached copies from 2026-03 do not; raw
reads columns by name. The paging moved to the shared `spinta.pages` on 2026-10-19, unchanged, for
the vacancies too (UZT vacancies source), which add a selection and the version the count must be.

**Rows are counted where a line starts with the first row's `_type`.** A balance sheet form's name
spans lines inside its quotes, so lines are not rows; no quoted field starts a line with
`datasets/gov/rc/jar/...,`. Spinta's columns up to `_page.next` are never quoted, so the token is
read by position from the last 64 KiB kept of the stream.

**The archive's 2026-09 snapshot was mended, not fetched again whole.** As first archived on
2026-10-18 (19:25–19:29 UTC, whole exports through the cache), the balance sheets, non-filers and
late filers were whole: their rows equal the portal's `count()` (9,626,523, 113,065, 19,866) and
each last row holds a token. The income statements were the cache's cut-off copy, and the failed
fetch past the cache then wrote over that file: `RawArchive.save_stream` on S3 commits what was
written when the download fails (s3fs completes the upload as the file is closed), so the file held
5,391,873 rows of an unfinished download while the manifest still named the first fetch's size and
SHA-256. On 2026-10-19 (05:55–06:32 UTC) the income statements alone were fetched in pages with the
source's own page saver and checks, run in steps of under ten minutes, and the manifest written from
those 13 pages and the earlier records of the other three: 4.2 GB less from the portal. A later
snapshot is fetched in pages throughout. The streaming archive should write to a temporary name, or
discard on failure, so that a failed download cannot replace a complete snapshot's file; that is the
shared streaming API's, so it is left to it.

**Quarterly, on the 21st of March, June, September and December.** The register loads the filings in
batches: the income statements' changelog (sampled by id, `:changes/<id>`: inserts) holds a load on
2025-05-27, an addition on 2025-09-04 and a reload of everything on 2026-03-17 and 18; the exports'
Last-Modified is 2026-03-16 to 18, the late filers' 2025-09-03. Each export is the whole history
since 2015, about 7 GB (1.35 GB gzipped), so monthly snapshots would mostly archive the same 7 GB
again. A quarterly fetch sees a batch up to three months late, which the yearly cycle of filings
(due by the end of May) can bear; JAR's monthly snapshot of the same month resolves the entities.

**Staging keeps each line once.** The 2026-03 reload did not delete the 2025 rows, so of the 16.1 M
rows 8,082,358 lines are distinct by entity, period, filing date, set, form and line: 7,857,967
published twice and 85,704 three times, every copy with the same amount
(`stg_jar_financial_statements__lines.published_copies`). Amounts are whole euros (Maxima LT's 2024
revenue is 2,208,938,000), a loss, a deficit or negative equity negative. The 16 M rows are grouped
per statement on narrow columns (the entity's `_id` cast to a UUID; names, one per id, joined
after): grouped with the names, the model ran out of DuckDB's 700 MB in three seconds. Statements
run from fiscal year 2015 to 2025, the 2025 ones few (11,150 company years against 2024's 150,549)
as the register had loaded only those filed by 2026-03-03.

**A seed maps the 24 published lines to measures** (`financial_statement_measures`): sales revenue,
a non-profit's total income, profit before tax, net profit, non-current, tangible non-current,
current and total assets, equity and liabilities. The register publishes a filing's summary lines
only: no cost of sales, gross profit, staff costs or income tax. One measure can be two line ids in
different forms (sales revenue is `ISLT00345` in the short and small forms, `ISLT00001` in the
others), never two lines of one filing with different amounts, but for one case: non-profits' forms
since 2024 (FS05xx, FS06xx) publish both their income (`PAJAMOS`) and their sales revenue, 4,773
filings with different amounts, so a non-profit's income is its own measure, `total_income`, not
revenue. A line the register adds fails no build: staging warns until the seed maps it.

**One row per company and fiscal year** (`int_jar_financial_statements__company_years`, 1,214,761
rows): the measures of the period ending last in the year, as last filed, the group's consolidated
statements left out. Of two periods ending in one year (a moved year end, 746 years) the later
counts. The export holds a later filing of the same period almost only for 2024 (336 of its years; 9
for all earlier years), so restatements are mostly not visible. Income tax is profit before tax less
net profit; total assets, reported by 1.5% (non-profits' and some partnerships' forms), is otherwise
non-current plus current assets. Coverage of all company years: net profit 100%, equity 99.4%,
liabilities 98.8%, assets 99.6%, sales revenue 73.7% (87.5% with non-profits' income), profit before
tax 78.6% (micro companies' short form has none). Each year carries `consecutive_loss_years` and
`consecutive_negative_equity_years`, runs of filed years in a row up to it: a year not filed, or
with a profit, ends a run.

**Every filer resolves to a JAR code.** All 202,571 entities with lines, and every non-filer and
late filer, are in `stg_jar__entities`' newest snapshot (relationships tests), which keeps
deregistered entities, so `company_code` is never empty.

**The filing record is one row per entity** (`int_jar_financial_statements__filing_compliance`,
208,314): years filed, years listed as not filed and whether on the late list. The non-filers list
an entity again for a year whenever its form or status changed between loads: 113,065 rows are
76,342 entity years, 6.8% of which have statements after all, filed late (3,916 of 2023's 15,594).
The late filers' list names no year (`veluojantis_iki` and `ja_reg_data` are empty on every row) and
was last changed on 2025-09-03.

**Persistent losses among the leads (2026-09 snapshot, fiscal years to 2024).** The leads' companies
are those with an activity in `activity_categories` in Vilnius city: in Sodra's latest month (1,548)
or as the VMI register's main activity running at the end of 2024 (1,444; VMI ended most EVRK 2
activities at the switch to EVRK 2.1), 2,282 together, all in JAR. Of the 1,795 with 2024 statements
and no legal status (not in liquidation or bankruptcy), 728 made a loss in 2024, 200 their third or
more in a row (cafés 91, car repair 68, hair and beauty 41), 79 their fifth or more; 645 had
negative equity, 154 of them after three or more loss years and 59 after five. Compared with the
same year's peers (net margin in the bottom quarter of the category's that year, and a loss), 92
have three such years in a row and 24 five. The base rate is high and steady: 29 to 50% of each
category's filers made a loss in every year since 2015, and 32 to 52% had negative equity. 2020 and
2021 were not exceptional for Vilnius cafés (47% and 42% lost money, against 43 to 47% in 2015 to
2018 and 29% in 2023); 2024 was (46%, after 29%). A run of losses is therefore weak evidence alone;
against peers, and with negative equity, it narrows to under a hundred companies.

**Raw builds fit in a dev process only as a first build of a snapshot.** Built in pages, the income
statements' raw peaked at 1.26 GB RSS with DuckDB at 700 MB. The balance sheets' single 4.2 GB file
rebuilt over an existing snapshot (`delete+insert`) passed 1.5 GB at 700 MB and ran out of DuckDB's
memory at 400 and 560 MB; built afresh (`--full-refresh`) it peaked at 1.12 GB with 600 MB. A later
snapshot comes in pages. The company years first grouped with count-distinct checks and reached 1.5
GB; with min-max checks and narrow keys they build in 17 s at under 1 GB.

**The replay sample is one in eight of the sample's companies.**
`replay/jar_financial_statements/2026-09` holds the lines, non-filers and late filers of the 251
entities in `replay/jar/2026-09` whose code is divisible by 8, cut from the live snapshot byte for
byte (1.8 MB, 22,575 statement rows); 225 of them filed statements.

## dbt spill directory (2026-10-19)

**DuckDB's temp directory is set once, as dbt opens the database (`config_options`), not with
the per-cursor `settings`.** dbt-duckdb 1.11 passes `config_options` to `duckdb.connect` and runs
`SET` for every entry of `settings` on each new cursor, one per node and thread. Once DuckDB has
spilled it refuses to set `temp_directory` again, even to the folder it already uses ("Cannot
switch temporary directory after the current one has been used"), so the node after the first
spill failed and skipped the rest. Under Dagster a source's step builds and tests in one
invocation, so a source big enough to spill built its models and reported a failed step. On the
live dev target with a 300 MB limit, `dbt build -s raw_sodra__monthly+1` built raw (spilling)
and failed at the next node, skipping staging and its four tests; with `config_options` all seven
pass, spilling up to 618 MB into `target/duckdb_tmp/<invocation id>/`. Through dbt-duckdb's own
environment, an in-memory database (Dagster's runs) failed and now passes the same way. The
folder is still per invocation (DuckDB memory), as the profile is rendered per dbt run.
`memory_limit` stays in `settings`, as DuckDB lets it be set again.

## A snapshot is what its manifest lists (2026-10-19)

**Raw reads the files a snapshot's manifest lists, not what its folder holds.** A second fetch in a
period replaces the period's snapshot by writing over its files, then its manifest. A file the
second fetch no longer writes stayed beside the new ones, and the raw models' globs read it: a paged
export (`spinta/pages.py`) fetched again with fewer pages left the earlier fetch's last pages
(`Vieta.3.csv.gz` beside a new two-page `Vieta.csv.gz`, `Vieta.2.csv.gz`), so raw held rows of two
fetches; the same holds for VMI's changelog pages, a Google sweep making fewer searches, or a Sodra
year no longer fetched. The manifest, written last, already says which snapshots are complete; now
it also says which files make one. `listed_archived_files` (`macros/archive/paths.sql`) reads the
snapshots' manifests (`read_text`, `json_keys` of `files`) and keeps the names matching the model's
glob; `archived_csv`, `archived_json_entries` and Sodra's zips read those files and their sidecars
only. Parsing cannot query, so it still gets the globs. In the live archive (95 complete snapshots
of 10 sources) and the replay sample, every file in a snapshot's folder was listed and every listed
file was there, so no build reads other files than before.

**The fetch deletes nothing.** Clearing the snapshot's folder before writing it again would let a
second fetch that fails (a paged export after an hour of pages, a sweep after hundreds of searches)
leave the period's manifest naming files that no longer exist: today a failed retry leaves the
complete snapshot in place, its files at worst written over, each whole, by the retry's. Removing
the unlisted files after the manifest is written would be safe, but it is only storage, a delete the
archive otherwise never makes (and, once the bucket is versioned, a noncurrent version kept anyway),
written once per archive adapter; the leftovers are at most an earlier fetch's surplus files in a
period, and nothing reads them. Left: a fetch made again writes over a complete snapshot's files
before its new manifest, so raw built during that fetch reads some of each; the manifest's SHA-256s
would tell, but are not checked.

## Risk models (2026-10-19)

**A suspicion is a risk model: a named, versioned detector stating one hypothesis, with its
legitimate explanations written down.** The name is the user's old workplace's for a business
calculation made of many stored procedures. Each is one folder and one dbt group,
`models/risk/<id>/`: private steps `rm_<id>__<step>` (`access: private`, so nothing outside the
group reads them) and one public output `rm_<id>` with an enforced contract. Its hypothesis,
legitimate explanations, family, weight and sources are a row of the registry seed
`risk_models.csv`, so the logic can be reviewed without knowing accounting, and its thresholds
are vars `rm_<id>_*` in `dbt_project.yml`, never literals. They run nationally, every company in
Lithuania by month, on official data; matching Google's listings in Vilnius comes later. README,
Adding a risk model, is the guide for adding one.

**Every output has one contract, one row per company and month.** `company_code`,
`period_month`, `risk_model`, `risk_model_version`, `raw_value`, `peer_group`, `peer_count`,
`peer_percentile` (0 to 100, higher more suspicious), `fired`, `evidence` (JSON, as each model's
numbers differ and the union needs one type), `explanation` and `source_as_of`
(`map(varchar, date)`, when each source's row was published). The period is a month, as a
`date`, its first day, judged on data up to and including it: Sodra, the backbone, declares by
month, the leads are monthly and every source's snapshot is at most monthly. A model on yearly
data repeats the latest year filed by each month. A month, not a snapshot, so the same month
rebuilt from a later snapshot is the same row.

**A company a model cannot judge gets no row; one it can judge but not compare gets a row with an
empty percentile.** Without data there is no measure, and a row of nulls would only be filtered
out again; so "the models with data" for a company is the rows it has. With the measure but
fewer than 20 peers, the value and the model's own threshold still say something, and the
analyst sees the value: such a row counts for the tier when it fires but adds nothing to the
score. In 2026-08, 2,505 of rm_wage_floor's 39,836 rows have no percentile.

**dbt reads the registry through a variable, `OWL_RISK_MODELS`, set by the launchers from the
CSV.** `fct_score_components` must `ref()` every enabled model, and dbt records a model's refs
while it parses: a ref made only at run time fails (`RefBadContextError`), and while parsing dbt
can neither read a seed nor see the graph (`graph` is filled after the whole project is loaded,
`manifest.build_flat_graph()`). So `dbt_environment()` reads the seed's enabled rows into
`wage_floor:1,...` beside the project's other variables, for every way dbt starts here (`owl-dbt`,
`owl-dbt-dev`, Dagster, the MCP servers, `owl-dbt-vscode`), and the macro `risk_model_components`
refs each. Editing the CSV changes the variable, and dbt parses again. Rejected: a list in the
union model or in `dbt_project.yml` (a second place every step-3 branch edits and conflicts in),
and a union found at run time without refs (no lineage, no build order, no defer to the lake).
The cost: plain `dbt` without it fails to parse, loudly, rather than score nothing; the archive
readers' test, which runs dbt by hand, sets it as the launchers do. A registry row naming no
model fails the parse; `risk_models_match_registry` catches the other way, a model with no enabled
row, and a model that is not public, contracted and in its own group, or a weight not positive.

**Versions run side by side, and the model's `latest_version` is scored.** A v2 is
`rm_<id>_v2.sql` and `- v: 2` in the YAML, with a registry row of its own: both versions build
(tables and Dagster assets `rm_wage_floor_v1`, `_v2`: the asset key now carries a model's version,
or two versions would be one key) and both are in `fct_score_components`, where `is_scored` is
true for the latest version only, so v2's rows can be compared with v1's for a month before its
YAML makes it the latest. dbt's own notion, not a registry column: `ref('rm_<id>')` follows it
too. A unit test has no graph, so there every version counts as scored.

**Risk models are drawn in a `risk` box, each in a box of its own.** The shared step draws a model
in a folder of its layer in its folder's group (`intermediate/google/` beside Google's source);
the translator nests the `risk` layer's folders one level more (`NESTED_LAYERS`), so
`models/risk/wage_floor/` is `live/risk/wage_floor`. The profile is in `live/companies` and the
marts in `live/scoring`. Rebuilding any source the profile reads (Sodra, JAR, the Address
Register, both VMI sources) rebuilds the profile, the risk models and the scoring after it.

**The company-month profile, `int_company_months`, is every company in the country by month.**
4,534,368 rows, 260,162 companies, Sodra's 20 months (2025-01 to 2026-08): each entity of JAR's
newest snapshot in every month it was registered on any day of, and the 53 Sodra employers JAR
does not hold. JAR publishes only its current state: a status counts from `status_since` and is
unknown before; registration dates place entities in time. In liquidation or bankruptcy is
statuses 5, 6, 7, 9, 11, 13 and 26 (`risk_wind_down_status_codes`): 4,057 companies in 2026-08.
The activity is Sodra's (EVRK 2.1), carried through months it declares nothing in, for 114,735
of 2026-08's 232,647 companies; else the VMI register's main activity (EVRK 2, the latest
started, as VMI ended most at the switch to EVRK 2.1) for 80,985; none for 36,927. The version is
kept, as one code means different things in each. The municipality is Sodra's for 116,558, else
JAR's seat's for 95,352, none for 20,737.

**Peers are the same activity class and month, in the municipality if it has 20, else in the
country.** The class (four digits of one EVRK version: 56.11 restaurants) is the finest level that
keeps most groups big enough: in 2026-08, 94% of Sodra's employers with a published wage were in a
class of at least 20 nationally, 85% of Vilnius's in one of at least 20 in Vilnius. A division (two
digits) would compare cafés with hotels. Wages differ by place, so the municipality comes first.
`peer_percentile` (the macro) is `percent_rank`: the share of the other peers strictly less
suspicious, ties sharing the lowest rank, so a value most peers share is never high; a direction
says which way is suspicious. In 2026-08 rm_wage_floor compared 18,343 companies within their
municipality and 21,224 in the country (2,236 of those with too few peers even there); 269 had
no activity class.

**The score is gated, weighted and tiered.** Gates come first, each failure kept in
`exclusion_reasons`: not registered in JAR at the month's end, fewer than six months declared to
Sodra, in liquidation or bankruptcy. Then a company fewer than two models had data for is
`insufficient_evidence`, never a lead; with one model so far, every eligible company is. Tier A
needs fired models in two families, B one, C none (B and C are this framework's; the plan says A).
The score is the registry-weighted mean of the percentiles there are, 0 to 100; the explanation
joins the two models adding most to it; every row carries the scored versions (`wage_floor@1`).
Thresholds are vars (`risk_min_peers`, `risk_min_sodra_months`, `risk_min_models_with_data`,
`risk_tier_a_min_families`, `risk_wind_down_status_codes`). Nationally the six-month gate excludes
most of 2025-03 to 2025-05, as the archive's Sodra starts in 2025-01: 121,146 excluded rows of
713,357; in 2026-08, 183 (170 under six months, 13 being wound down). The lists per company were
ordered aggregates first, which ran out of 700 MB on 713 k one-row groups (each keeps a sort);
a window rank and sorted unordered lists fit.

**rm_wage_floor, the worked example, looks for wages declared at the minimum while peers pay
more.** The minimum wage seed (`minimum_wages`: 730, 840, 924, 1,038 and 1,153 EUR for 2022 to
2026; the Visuotinė lietuvių enciklopedija for 2022 to 2025, the Government's draft resolution
TAP-25-1227 for 2026, reported adopted unchanged) prices each month. Sodra's average wage is
published for employers with more than 3 insured. In 2026-08, 26% of those averaged below the
minimum (part-time staff, which a full-time wage cannot be), and averages bunch just above it
(854 employers at 1.00 to 1.02 times, against about 600 in each neighbouring 0.02). Of 44,265
employers with a wage in 2026-03 to 2026-08, 7,993 were at most 1.05 times the minimum in all six
months, but half of those had a median under 0.8: part-timers, the first legitimate explanation.
So the model counts months in a band, 0.95 to 1.05 times the minimum (an employer paying the
minimum, a part-month hire or a part-timer included): 401 in all six. Its measure, `raw_value`, is
how far the six months' median wage is from the minimum, either way (|median / minimum - 1|),
lower more suspicious. The share of months in the band was tried first: 0 for 81% of companies,
so one month in it ranked a company above 76% of its peers. It fires on all six months in the
band and a percentile of at least 75, so a sector where most pay the minimum does not fire. It
needs three published months of the six for a row.

**Nationally it fired for 216 to 376 companies a month in 2026 (376 of 39,836 in 2026-08).** Of the
401 in the band all six months, 376 fired; the other 25 had too few peers. Months from 2025-06 fire
(the first with six months behind it); 2026-01 to 2026-05 fire less (216 to 242, against 296 to
376): the minimum rose 11% in January, and a window spanning the change holds months in the band
under each year's minimum only if the wage rose with it. In Vilnius in 2026-08: cafés and
restaurants 545 rows, 6 fired, median wage 1.04 times the minimum; car repair 268, 7 fired, 1.15;
hair and beauty 69, 2 fired, 0.79 (mostly part-time; and 185 of Vilnius's 257 hair and beauty
employers have 3 insured or fewer, so no published wage at all). All are compared within Vilnius but
2 cafés, which have too few peers in their class.

**A national build takes about 45 s.** On the live dev target, sources prebuilt, the profile,
rm_wage_floor's three models and both marts built in 43.2 s of dbt work (58 s wall with dbt's
start and the lake's attach), at a 700 MB DuckDB limit, the dbt process peaking at 1.4 GB RSS:
the profile 29.4 s (4.53 M rows), rm_wage_floor__months 2.1 s (817,028), __windows 3.1 s
(713,357), rm_wage_floor 5.4 s (713,357), fct_score_components 1.4 s and fct_company_risk 5.8 s
(713,357 each). Their 62 tests take 20 s. The timings come from dbt's `run_results.json`
(`execution_time` per node). To keep them, an `on-run-end` hook could insert dbt's `results`
(unique id, status, execution time, rows affected, invocation id) into `audit.dbt_node_runs` in
the lake, or the Dagster step could read `run_results.json` after each build and write it, as
it already copies the manifest (`keep_main_state`); the second keeps dbt's runs free of audit
writes that two steps could conflict on. Not done.

**Left open.** The B and C tiers, the weights (all 1.0) and the minimum of two models are this
framework's choices to confirm. The band (0.95 to 1.05) and the 75th percentile for
rm_wage_floor are calibrated on one month of data, and the percentile has not yet held a company
back (in 2026-08 every company in the band all six months with peers was at the 78th percentile
or above). Sodra's history in the archive starts in 2025-01, so the first five months are excluded
by the six-month gate. JAR's status before its current one is unknown, so a company wound down
in 2026-08 counts as eligible in earlier months. The 2026 minimum wage is from the draft
resolution; the adopted act in e-TAR was not checked.
