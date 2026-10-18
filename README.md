# Owl: Off-books Work Locator

Monthly pipeline that ranks Vilnius businesses which look busy on Google but declare little to Sodra.
Plan and code standards: [Pipeline Plan](https://claude.ai/code/artifact/2803bbda-8626-4a47-ae98-256daea0289b).

## Local development

Requirements: Docker with Compose v2 and VS Code with the Dev Containers extension.
Everything for local work lives in `.devcontainer/`; Compose is not used outside it.

1. Open the repo in VS Code and choose **Reopen in Container**.
   - On the host, `initializeCommand` builds the dev image `owl-dev:local`.
   - Compose starts the dev container with Postgres, s3mock and Marquez.
   - `post-create.sh` runs `uv sync --frozen`, installing the workspace into the container's system
     Python (3.14.7), and links `dagster.yaml` into `$DAGSTER_HOME`. There is no virtual environment.
     Downloads are cached in the `uv-cache` volume, so rebuilding the container is quick.
2. In the container terminal:

   ```sh
   pytest                      # all tests; -m unit for the in-process ones only (below)
   ruff check . && ruff format --check .
   owl-dbt --lake replay debug  # dbt on a lake (live or replay), secrets resolved from config
   owl-dbt-dev build -s state:modified+  # your model changes, in a dev copy (below)
   owl-dbt-diff              # what those changes did to the tables (below)
   owl-dbt-vscode            # the dbt VS Code extension's environment, then reload the window
   owl-dagster-dev           # Dagster webserver and daemon; UI on http://localhost:3000
   ```

GitHub from the container: `git` over HTTPS uses your host's credentials, which VS Code forwards.
`gh` keeps its own login, so run `gh auth login` once; the `gh-config` volume keeps it across
rebuilds. The `dev` user has passwordless `sudo`.

Without VS Code:

```sh
docker build --tag owl-dev:local .devcontainer
docker compose -f .devcontainer/compose.yml up -d
docker compose -f .devcontainer/compose.yml exec dev sh .devcontainer/post-create.sh
docker compose -f .devcontainer/compose.yml exec dev owl-dagster-dev
```

### What runs where

| Service | Image | Reached at |
|---|---|---|
| `dev` | `python:3.14.7-slim` + uv | container shell; Dagster UI on http://localhost:3000 |
| `postgres` | `postgres:18-alpine` | `postgres:5432` (databases `dagster`, `ducklake_catalog`, `marquez`, `app`) |
| `s3mock` | `adobe/s3mock:5.2.3` | `s3mock:9090`, bucket `owl-lake` |
| `marquez` / `marquez-web` | `marquezproject/marquez(-web):0.51.1` | `marquez:5000`; UI on http://localhost:3001 |

Only the two UIs are published to the host. Every container reaches the others by service name.

### Configuration

Config is TOML in `config/<env>/`, mounted read-only at `/etc/owl/` and read by the
configuration service in `services/config/`. Commented blocks in each file show how production
would differ.

Local dev credentials (Postgres, s3mock) are plain values in the config. Real secrets are
references (`{ secret_name = "..." }`) that resolve when first read, from the one secret store
each config directory declares (`secret_store.toml`); locally, files in `config/local/secrets/`,
which is git-ignored. Replay needs none. To run live against the Places API, put the key in
`config/local/secrets/google_places_api_key`; it is read only when a Google fetch sends its first
request.

Logs go to stderr at INFO and to `logs/owl.log` at DEBUG, rotated at 10 MB across 10 files.

Each dbt process's DuckDB takes at most `transform.duckdb_memory_limit` (2 GB locally) and spills
beyond it into `services/transform/target/duckdb_tmp/<dbt invocation id>/`. Several run at once, one
per source and lake, and unset each could take 80% of the machine's memory.

Dagster and dbt only accept credentials through environment variables, so they are started through
`owl-dagster-dev` and `owl-dbt`, which ask the configuration service and set those variables for
that one process. Under Dagster, dbt's variables are set by the definitions instead, so
**Reload definitions** applies a configuration change to dbt too.

`owl-dagster-dev` runs `dagster dev` against the repo, so code changes appear after
**Reload definitions** in the Dagster UI.

### Live and replay

Dagster loads two code locations side by side (`.devcontainer/dagster/workspace.yaml`), built by
the same code into separate lakes, so the sample's rows never mix with live ones:

| Location | Reads | Builds into | Asset keys and group |
|---|---|---|---|
| `live` | the sources, fetched into the archive (`lake.archive_path`) | `lake.metadata_schema` | `live/<layer>/<name>`, group `live/<source>` |
| `replay` | the committed sample (`pipeline.replay_path`); calls nothing | `lake.replay` | `replay/<layer>/<name>`, group `replay/<source>` |

Production deploys `live` only. Nothing switches between them: each lake is one box in the
lineage view (its group), holding a box per source (a group nested in the lake's, as Dagster reads
`/` in a group name), fetch → archive → raw → staging left to right inside; models that combine
sources go in the lake's box itself. Each lake is also one folder in the asset catalog, with its
own history and pools. Node names say the layer (`fetch_`, `archive_`, `raw_`, `stg_`); only live
has a fetch, and the archive is drawn dashed, as nothing can materialize it.

### Lineage (Marquez)

Marquez's UI is on http://localhost:3001; pick the namespace `owl` at the top. Each code location
has a sensor, `lineage`, running from the start, which sends Marquez every materialization of the
location's assets as an OpenLineage event, within half a minute of its run's end
(`owl_dagster_orchestration/lineage.py`). Its first tick sends the instance's history.

- **Graph:** an asset is a job and a dataset of the same name, its key (`live/raw/raw_sodra__monthly`);
  the job reads the asset's parents. So Marquez draws Dagster's graph, fetch → archive → raw →
  staging → models, one per lake. Search a table by name and open its lineage.
- **Per table:** its columns, each typed as built and described as the dbt YAML describes it, and
  the table's description (in the facets panel: Marquez 0.51 does not copy it to the description
  field).
- **Column lineage:** each staging and later column, from the columns it is computed from, back to
  raw's. Open it from the Datasets list (Column Lineage → VIEW) or from a dataset's column table
  (the split-screen icon beside a column); click a column for its description. dagster-dbt works it
  out with sqlglot after each build, so a model has it from its next build on. Raw has none: it
  reads the archive's files, which sqlglot cannot see into, and each raw build logs a warning
  saying so.
- **How a column is computed:** the SQL dbt compiled for the build (refs resolved), on the run in
  the job's page (open the model's job, then a run). Every dbt build keeps it as `compiled_sql` in
  Dagster too. Marquez shows which columns feed a column, not the expression: OpenLineage can carry
  a transformation per column, but Marquez 0.51's UI does not show one, so none is sent.
- **Quality:** each table's checks (its dbt tests, as dagster-dbt makes them checks, and any other
  asset check) from the run that built it, each passed or failed. Marquez shows them on the
  dataset's page (QUALITY, the passed and failed counts, then each check), healthy if all
  passed. The Datasets list's Quality column stays N/A: Marquez 0.51's list API returns no facets,
  so the column has nothing to read. Raw, archive and fetch have no checks.
- **Per run:** a materialization is a completed run of the asset's job, with the rows it wrote and
  the snapshot it built as its nominal time (Marquez lists that under the run's `args`). It is
  sent once its run has ended, so that it carries the run's checks.
- **Not sent:** failures. Dagster OSS records which step failed, not which of its assets did; see
  them in Dagster.

`[lineage]` in `config/local/orchestration.toml` says where Marquez is. If it is down, the sensor's
tick says so and the next one carries on where it stopped. To send everything again, clear the
sensor's cursor in Dagster: each run's id is derived from the Dagster run, so Marquez gets the same
runs, not new ones.

### Snapshots

Every source goes through the raw archive. What one fetch of a source captures is a snapshot,
kept as fetched at `<archive>/<source>/<snapshot>/`, and complete once the fetch has written its
manifest, `<archive>/<source>/_snapshots/<snapshot>.json`, last. A snapshot is keyed by the
period it was fetched in, which each source sets with its schedule, as often as the source
publishes:

| Source | Fetches | Schedule | Key |
|---|---|---|---|
| `sodra` | the yearly employer files covering the last 13 months | 21st at 06:00 (`sodra.snapshot_schedule`), as Sodra publishes the previous month by the 20th | `YYYY-MM` |
| `jar` | the register's entities, legal forms, statuses and seats (registered offices), each whole | 21st at 06:00 (`jar.snapshot_schedule`): the register publishes monthly at most, on no fixed day | `YYYY-MM` |
| `address_register` | the Address Register's building and premises addresses, streets, settlements and municipalities, each whole (about 565 MB) | 21st at 06:00 (`address_register.snapshot_schedule`): loaded in the same batches as JAR, every one to six months | `YYYY-MM` |
| `vmi_daily_changes` | the VMI taxpayer register's changelog: the changes made in the day before the snapshot's tick | daily at 06:00 (`vmi.daily_changes_schedule`): the register changes most days, mostly at about 03:00 | `YYYY-MM-DD` |
| `vmi_monthly_register` | the whole VMI register, the EVRK 2 classifier, and the changelog's newest change at that moment | 21st at 06:00 (`vmi.monthly_register_schedule`), the baseline `vmi_daily_changes`' changes are applied to | `YYYY-MM` |
| `google_places` | Google's listings from the Places API in the search areas to be fetched (`int_google_search_areas`; Vilnius): each query swept over tiles of at most 1 km, planned for free with place ids, then the searches holding every place found paid for (Vilnius priced at 927 requests on 2026-10-17; 1,000 a month are free) | 21st at 06:00 (`google_places.snapshot_schedule`), with the leads; **its schedule starts stopped**, as each sweep is paid for | `YYYY-MM` |

A second fetch in the same period replaces that period's snapshot.

A source's partitions are its complete snapshots, so every partition has something to build from.
A source captured as it is now cannot be fetched for a period that has passed: its fetch takes no
partition, the clock picks its snapshot. A source that keeps its history, as the VMI register's
changelog does, can: its fetch is partitioned by its periods from a first one (`vmi.first_daily_changes`)
to the current one, the schedule fetches the current one, and a backfill fetches past ones, such as
a month of days, in one run. Adding a snapshot to the replay sample, with its manifest, adds a partition with
no reload.

| What | Does | Started by |
|---|---|---|
| `fetch_<source>` (live) | fetches the source now into the archive, as the current period's snapshot (for `vmi_daily_changes`, the period chosen) | its schedule, or you (a backfill for several of `vmi_daily_changes`' days) |
| `archive_index` sensor | adds a partition for each complete snapshot, and reports it as the archive's | Dagster, every minute |
| `transform_automation` sensor | builds raw for each new snapshot, then staging and after; rebuilds staging and after when their SQL changes | Dagster |
| `rebuild_<source>_from_archive` | rebuilds raw for the snapshots you choose, then staging and after once | you, from the job's page (a backfill for several) |

So each source's build is a chain of runs: the scheduled fetch, then (within a minute or two)
raw for the new snapshot, then staging. Sources build side by side, each in its own pools.
Raw is partitioned by snapshot; staging and later models are one table each, rebuilt whole, so
their latest materialization is the table's.

Every run and materialization is labelled `owl/trigger`: `schedule` or `fetch` (by hand) on a
fetch, `automation` on what Dagster built by itself, `rebuild` on a rebuild by hand. Each raw
snapshot also shows `archive_fetched_at`, the download it was built from. Filter the runs list by
`owl/trigger` to see, say, every rebuild.

A new source is one `ArchivedSource`, built in its own module in
`owl_dagster_orchestration/sources/` and listed in `archived_sources()`
(`entrypoints/definitions.py`): its name, its schedule and key format, and its fetch
(`owl_ingest/sources/<source>/`, with its settings in `config/<env>/pipeline.toml`), plus its
dbt models in `models/raw/<source>/` (schema `raw_<source>` in `dbt_project.yml`) and
`models/staging/<source>/`. Its partitions, archive,
fetch job and schedule, rebuild job, automation and group all follow from that. `jar` is the
smallest example: three CSV exports, read by the `archived_csv` macro. The VMI register is the
example of one dataset fetched as two sources, drawn together in one box (`ArchivedSource.group`):
`vmi_daily_changes`, a source with history (`history_start`) whose changelog pages are read by the
`archived_json_entries` macro, and `vmi_monthly_register`, the baseline the changes are applied to
by the staging models in `models/staging/vmi/`. `google_places` is the example of a paid API: a
client of its own (`ports/places_api.py`), a plan in place of a fetch (`ArchivedSource.plan`), a
schedule that starts stopped, a longer `dagster/max_runtime` for its job, and a plan that reads a
model of the lake (`fetch_deps`). The plan and the fetch are two assets made in one step
(`build_planned_fetch_asset`): `plan_google_places` plans the sweep for free and shows what it
pays for, with a blocking check `within_budget` against `google_places.max_paid_requests`;
`fetch_google_places` pays for that plan, made in the same step, and refuses one over the budget.
The fetch job selects both, so a fetch always pays for a plan made minutes before. The snapshot
keeps the plan it was paid for, `plan.json`: every search, the places it found and whether it was
paid for. Both the plan and the payment send `google_places.workers` searches at once, without
counting Google's per-minute quota: a request over it is answered 429, then waited for and sent
again, for about five minutes (`HttpPlacesApi`).

### Where Google is searched

Google is searched in the municipalities listed in `services/transform/seeds/google_search_scope.csv`,
each `price` or `fetch`; only Vilnius (13) is listed, as `fetch`. Seeds, data kept by hand, are
drawn in each lake's `static_data` box, and build themselves on a fresh lake. `int_google_search_areas`
(`models/intermediate/google/`, drawn in the lake's `google` box beside Google's source) lists
all 60 municipalities with their bounding boxes and how many employers Sodra counts there in the
leads' categories, ranked, so the next one worth searching is the top row without a mode. To see
what Kaunas would cost, add `19,price` to the seed and **Reload definitions** (live): the seed, the
areas and `plan_google_places` rebuild by themselves (the plan alone, in a run of its own), and the
plan's metadata shows each area's places and paid requests, and the next fetch's in USD against
`google_places.max_paid_requests`, having paid for nothing. Change it to `19,fetch` to have the next
fetch pay for it. A rebuild of any source's archive never fetches or plans.

### Tests

Each package keeps its tests in `tests/unit/` (in-process only) or `tests/integration/` (a temporary
directory, DuckDB, a local HTTP server, or a throwaway Dagster instance running real runs; none
needs the compose services). `conftest.py` marks each test by its directory, so `pytest -m unit` or
`pytest -m integration` runs one kind, and a test in neither directory fails collection.

Tests build settings with `owl_testing` (`libs/testing`, a dev dependency only):
`build_config({"sodra.history_months": 1})` is a configuration like `config/local` with the dotted keys
replaced, and `owl_testing.ingest` holds a `Download` builder and in-memory fakes of the ingest
ports. dbt's own tests stay in `services/transform/tests/`, where dbt reads them.

### Iterating on models

Load the sources once through Dagster, then work on the SQL with `owl-dbt-dev`. It runs dbt on the
`dev` target: models build into a DuckDB file of their own, and a lake is attached read-only, so
nothing you do there touches the lake or waits for a Dagster run. A model you do not select is read
from the lake (dbt's `--defer`), as that location's last build left it. `--lake` picks the lake,
`replay` unless given; each lake has its own dev copy in `services/transform/target/<lake>/dev/`.

```sh
owl-dbt-dev build -s state:modified+                # what you changed since Dagster's last build
owl-dbt-dev build -s stg_sodra__monthly+            # one model and everything after it
owl-dbt-dev show -s stg_sodra__monthly --limit 20   # look at the result
owl-dbt-dev build -s raw_sodra__monthly            # raw, read from the archive
owl-dbt-dev --lake live build -s state:modified+   # the same against the live lake
```

The dbt VS Code extension (lineage, hover, compile, preview) runs on the same `dev` target, but it
cannot resolve the profile's `env_var()`s by itself. `owl-dbt-vscode` (`--lake` as above) writes
them into `dbt.environmentVariables` in the machine's VS Code settings, kept out of the repository
as they hold the lake's credentials; then run **Developer: Reload Window**. Run it again after
changing the config. The extension parses into `services/transform/target/<lake>/vscode/`, apart
from `owl-dbt-dev`'s files.

Then `owl-dbt-diff` compares each model the last `owl-dbt-dev` command built with the lake's
table: rows on one side only, columns added, dropped or retyped, and on a model whose meta names
its `key` (staging: `insurer_code, declared_month`), how many rows changed in each column, with
examples. A change should show only what it meant to: adding a column ends in `only columns
added`; anything else ends in `CHANGED`. `owl-dbt-diff stg_sodra__monthly` names models instead,
and `--lake` works as above. It compares what is built in dev, so rebuild after editing the SQL.

```text
stg_sodra__monthly: "dev"."staging"."stg_sodra__monthly" (dev) against "lake"."staging"."stg_sodra__monthly" (lake), matched on insurer_code, declared_month
  columns added:   insured_count_2 INTEGER
  columns dropped: none
  columns retyped: none
  rows: 30,352 in the lake, 30,352 in dev; 0 only in the lake, 0 only in dev
  values changed in matched rows: none
  => only columns added
```

Once merged, **Reload definitions** (a deploy, in production) is all that is left: the models whose
SQL changed get a new code version, and the `transform_automation` sensor rebuilds them and
everything after them. A new model is built by the next new snapshot, or by hand.

Raw models read the archive folder of a snapshot. On `owl-dbt-dev` that is the source's newest
complete snapshot unless `OWL_SNAPSHOT=<key>` names another; `owl-dbt` always needs it, and
Dagster sets it per partition.

Once a model is built in dev, the models after it read the dev copy, not the lake's. To read the
lake's again, add `--favor-state`, or start over by deleting `target/<lake>/dev/`.

### Agents (MCP servers)

`.mcp.json` gives coding agents (Claude Code, and any MCP client that reads it) four servers. Each
starts through a launcher that asks the configuration service for its settings, as `owl-dbt`
does, and runs a pinned release with `uvx`, outside the project's environment. All of them can
write: they are for local work only.

| Server | Launcher | Upstream | Gives an agent |
|---|---|---|---|
| `dagster` | `owl-mcp-dagster` | [dagster-mcp](https://github.com/fabdendev/dagster-mcp) (community) | runs, their logs and failures, assets, schedules, sensors, backfills; launching, terminating, materializing, reloading a code location. Needs `owl-dagster-dev` running |
| `lake` | `owl-mcp-lake` | [mcp-server-motherduck](https://github.com/motherduckdb/mcp-server-motherduck) | SQL on DuckDB in memory, with both lakes attached writable as `live` and `replay` (`select * from replay.staging.stg_sodra__monthly`), capped at `transform.duckdb_memory_limit` |
| `dbt` | `owl-mcp-dbt --lake replay` | [dbt-mcp](https://github.com/dbt-labs/dbt-mcp) (dbt Labs) | `build`, `run`, `test`, `show`, `list`, `compile`, lineage, as `owl-dbt --lake replay`: it writes to the replay lake |
| `dbt-dev` | `owl-mcp-dbt --dev --lake replay` | dbt-mcp | the same as `owl-dbt-dev`: builds into the dev copy, defers to the lake |

For the live lake, add a server with `--lake live`. The lake's DuckLake catalog is in Postgres, so
the `lake` server writes beside Dagster's runs and dbt's without locking them out. Its startup SQL
holds the lake's credentials and is written to `services/transform/target/mcp/lake_init.sql`,
readable only by you.

Notes:
- The `lake` server pins its own DuckDB, which must stay the project's (`duckdb` in
  `services/dagster-orchestration/pyproject.toml`): another version may not read the catalog. Bump
  `mcp.lake.package` in `config/local/mcp.toml` with it.
- dbt-mcp declares Python below 3.14, which uv does not enforce; it runs on the project's 3.14.
- dbt-mcp's lineage tools read `services/transform/target/manifest.json`, which the lake target
  writes; on `dbt-dev` they show the same models, from that parse.
- dbt raw models need `OWL_SNAPSHOT` on the lake target, which an agent cannot pass: build raw
  through Dagster, or on `dbt-dev`, which reads the newest snapshot.
- Each dbt command an agent starts may take up to an hour (`mcp.dbt.command_timeout_seconds` in
  `config/local/mcp.toml`).
