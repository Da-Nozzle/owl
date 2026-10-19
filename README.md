# Owl: Off-books Work Locator (Working Title)

Monthly pipeline that ranks Vilnius businesses which look busy on Google but declare little to Sodra.
Why it is built the way it is: [DECISIONS.md](DECISIONS.md).

## Getting started

Requirements: Docker with Compose v2 and VS Code with the Dev Containers extension. Other IDEs can
in theory connect to the dev container, but its extensions and settings will not work there.

1. Open the repo in VS Code and choose **Reopen in Container**. The first build installs everything;
   later rebuilds are quick.
2. In the container terminal, start Dagster:

   ```sh
   owl-dagster-dev
   ```

3. Open the UIs:
   - Dagster: http://localhost:3000
   - Marquez (lineage): http://localhost:3001, namespace `owl`

Without VS Code:

```sh
docker build --tag owl-dev:local .devcontainer
docker compose -f .devcontainer/compose.yml up -d
docker compose -f .devcontainer/compose.yml exec dev sh .devcontainer/post-create.sh
docker compose -f .devcontainer/compose.yml exec dev owl-dagster-dev
```

GitHub from the container: `git` over HTTPS uses your host's credentials, which VS Code forwards.
`gh` keeps its own login, so run `gh auth login` once.

## Running the pipeline

Dagster loads two code locations side by side, each building its own lake:

- **`replay`** builds from the sample committed in `replay/` and calls no source, so it needs no
  keys. Once Dagster is up, its sensors find the sample's snapshots and build raw, then staging and
  after, by themselves within a few minutes. Follow it in **Runs**.
- **`live`** fetches the real sources. Each source has a `fetch_<source>` job and a schedule; the
  open-data sources (Sodra, JAR, Address Register, VMI) need no keys. Run a fetch job from its page
  to fetch now; what it fetched is then built the same way as replay.

Google Places is paid for, so its schedule starts stopped. To use it, put the API key in
`config/local/secrets/google_places_api_key` (the file must have exactly that name and hold only
the key), then run `fetch_google_places`. It plans the sweep for free first and refuses to pay for
more than `google_places.max_paid_requests` (`config/local/pipeline.toml`). Which municipalities
are searched is set in `services/transform/seeds/google_search_scope.csv` (only Vilnius,
`13,fetch`).

To rebuild a source from what is already archived, without fetching, run
`rebuild_<source>_from_archive`.

## Configuration

Config is TOML in `config/local/`, mounted read-only at `/etc/owl/`. Local credentials (Postgres,
s3mock) are plain values there; real secrets are files in `config/local/secrets/`, which is
git-ignored. After changing config, click **Reload definitions** in Dagster.

Logs go to stderr at INFO and to `logs/owl.log` at DEBUG.

### Machine size

This was tested on a machine with 15 GB of usable RAM and an 8-core CPU. With the current local
settings it will not work on less. On a more powerful machine, raise the settings below: the
solution should scale well vertically.

| Setting | Where | Local value | What it controls |
|---|---|---|---|
| `transform.duckdb_memory_limit` | [config/local/orchestration.toml:15](config/local/orchestration.toml#L15) | `2GB` | DuckDB's memory in each dbt process (and in the Google planner and the `lake` MCP server). Beyond it DuckDB spills to disk, so a low limit is slow rather than fatal, up to a point. |
| `transform.dbt_threads` | [config/local/orchestration.toml:17](config/local/orchestration.toml#L17) | `4` | Models one dbt process builds at once, all sharing that process's memory limit. |
| `concurrency.runs.max_concurrent_runs` | [.devcontainer/dagster/dagster.yaml:29](.devcontainer/dagster/dagster.yaml#L29) | `1` | Dagster runs at once; the rest wait in the queue. At 1, when both lakes need rebuilding, they rebuild one after the other. |
| `google_places.workers` | [config/local/pipeline.toml:46](config/local/pipeline.toml#L46) | `16` | Google searches in flight at once. Google's per-minute quota limits this more than the machine does. |

Production deploys `live` only. Nothing switches between them: each lake is one box in the lineage
view (its group), holding a box per source (a group nested in the lake's, as Dagster reads `/` in a
group name), fetch → archive → raw → staging left to right inside; models that combine sources go in
a box of their folder's (`companies`, the company-month profile; `scoring`, the marts), each risk
model in a box of its own inside `risk` (`live/risk/wage_floor`). Each lake is also one folder in
the asset catalog, with its own history and pools. Node names say the layer (`fetch_`, `archive_`,
`raw_`, `stg_`); only live has a fetch, and the archive is drawn dashed, as nothing can materialize
it.

Leave `concurrency.pools.default_limit` ([dagster.yaml:27](.devcontainer/dagster/dagster.yaml#L27))
at 1. It is not a performance setting: it stops two runs from writing the same table at once,
which DuckLake does not allow.

After changing a TOML file, click **Reload definitions** in Dagster. After changing
`dagster.yaml`, restart `owl-dagster-dev`.

## Development

```sh
pytest                               # all tests; -m unit or -m integration for one kind
ruff check . && ruff format --check .
owl-dbt --lake replay debug          # dbt on a lake (live or replay), secrets resolved from config
owl-dbt-dev build -s state:modified+ # your model changes, in a dev copy
owl-dbt-diff                         # what those changes did to the tables
owl-dbt-vscode                       # the dbt VS Code extension's environment (also runs as postbuild)
```

Tests live in each package's `tests/unit/` (in-process) or `tests/integration/` (temporary files,
DuckDB, a local HTTP server or a throwaway Dagster instance). None needs the compose services. dbt's
tests are in `services/transform/tests/`.

### Changing models

| Source | Fetches | Schedule | Key |
|---|---|---|---|
| `sodra` | the yearly employer files covering the last 13 months | 21st at 06:00 (`sodra.snapshot_schedule`), as Sodra publishes the previous month by the 20th | `YYYY-MM` |
| `jar` | the register's entities, legal forms, statuses and seats (registered offices), each whole | 21st at 06:00 (`jar.snapshot_schedule`): the register publishes monthly at most, on no fixed day | `YYYY-MM` |
| `jar_financial_statements` | the financial statements filed with JAR since 2015 (income statements' and balance sheets' summary lines), the non-filers and the late filers, each whole, fetched in pages of 500,000 rows as the portal drops a response after about 25 minutes (about 7 GB, stored gzipped, about 1.35 GB; the fetch may run four hours) | 21st of March, June, September and December at 06:00 (`jar_financial_statements.snapshot_schedule`): loaded in JAR's batches every three to six months, each export the whole history | `YYYY-MM` |
| `address_register` | the Address Register's building and premises addresses, streets, settlements and municipalities, each whole (about 565 MB) | 21st at 06:00 (`address_register.snapshot_schedule`): loaded in the same batches as JAR, every one to six months | `YYYY-MM` |
| `vmi_daily_changes` | the VMI taxpayer register's changelog: the changes made in the day before the snapshot's tick | daily at 06:00 (`vmi.daily_changes_schedule`): the register changes most days, mostly at about 03:00 | `YYYY-MM-DD` |
| `vmi_monthly_register` | the whole VMI register, the EVRK 2 classifier, and the changelog's newest change at that moment | 21st at 06:00 (`vmi.monthly_register_schedule`), the baseline `vmi_daily_changes`' changes are applied to | `YYYY-MM` |
| `vmi_tax_arrears` | VMI's list of companies with tax arrears, through its changelog: the changes made from the tick before to the snapshot's own; from 2024-11, whose window holds the list's first load, so the snapshots hold its whole history (23 months, 3.1 M changes, 1.0 GB of JSON by 2026-09) | 21st at 06:00 (`vmi_tax_arrears.snapshot_schedule`): the list is reloaded every morning, but the changelog keeps every change, and the leads are monthly | `YYYY-MM` |
| `vmi_taxes_paid` | the changelog of the taxes legal entities paid to VMI (`vmi/ja_mokesciai`): the changes made in the month before the snapshot's tick, each monthly load's year-to-date sums inserted whole | 21st at 06:00 (`vmi_taxes_paid.snapshot_schedule`): VMI loads about monthly on no fixed day; any month from 2025-06 (`vmi_taxes_paid.first_snapshot`) can be fetched again | `YYYY-MM` |
| `google_places` | Google's listings from the Places API in the search areas to be fetched (`int_google_search_areas`; Vilnius): each query swept over tiles of at most 1 km, planned for free with place ids, then the searches holding every place found paid for (Vilnius priced at 927 requests on 2026-10-17; 1,000 a month are free) | 21st at 06:00 (`google_places.snapshot_schedule`), with the leads; **its schedule starts stopped**, as each sweep is paid for | `YYYY-MM` |
| `uzt_vacancies` | the Employment Service's vacancies since 1990, each as it stands, without the employer's contact people, fetched in pages of 400,000 rows as the portal drops a response after about 25 minutes (about 2.4 GB, stored gzipped, about 540 MB; the fetch may run two hours) | 21st at 06:00 (`uzt_vacancies.snapshot_schedule`): changed every night, but each export holds every vacancy's history | `YYYY-MM` |

A second fetch in the same period replaces that period's snapshot; a file it no longer fetches
may stay in the folder, but raw reads only the files the manifest lists.

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
by the staging models in `models/staging/vmi/`. `vmi_tax_arrears` is a monthly source with
history and no baseline: its snapshots from the changelog's first change on are the whole list's
history, which `stg_vmi_tax_arrears__monthly` folds into the list at each tick. `vmi_taxes_paid`
is one too: each of VMI's monthly loads inserts every row whole, so its changelog alone holds
every month since its first snapshot. `google_places` is the example of a paid API: a client of
its own (`ports/places_api.py`), a plan in place of a fetch (`ArchivedSource.plan`), a
schedule that starts stopped, a longer `dagster/max_runtime` for its job, and a plan that reads a
model of the lake (`fetch_deps`). The plan and the fetch are two assets made in one step
(`build_planned_fetch_asset`): `plan_google_places` plans the sweep for free and shows what it
pays for, with a blocking check `within_budget` against `google_places.max_paid_requests`;
`fetch_google_places` pays for that plan, made in the same step, and refuses one over the budget.
The fetch job selects both, so a fetch always pays for a plan made minutes before. The snapshot
keeps the plan it was paid for, `plan.json`: every search, the places it found and whether it was
paid for. Both the plan and the payment send `google_places.workers` searches at once, without
counting Google's per-minute quota: a request over it is answered 429, then waited for and sent
again, for about five minutes (`HttpPlacesApi`). `uzt_vacancies` is the example of an export
too big to hold in memory or to fetch in one response: fetched in counted pages
(`spinta/pages.py`, as `jar_financial_statements` is), each streamed into the archive as it
arrives and stored gzipped (`StreamingDownloader`, `RawArchive.save_stream`; DECISIONS.md,
Streaming downloads and UZT vacancies source).

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

### Adding a risk model

Every suspicion is a **risk model**: a named, versioned detector that states one hypothesis of
undeclared work or income, with the legitimate explanations for the same numbers written down.
Risk models run over every company in Lithuania, by month, on official data;
`fct_company_risk` combines them into tiers. `rm_wage_floor` (`models/risk/wage_floor/`) is the
worked example: copy it. Why it is built this way is in DECISIONS.md, Risk models.

**Files.** A model `<id>` (lowercase, `wage_floor`) is one folder, `models/risk/<id>/`, drawn in
Dagster as `<lake>/risk/<id>`:

| File | What |
|---|---|
| `_<id>.yml` | the dbt group `<id>`; every model's docs and tests; the unit tests |
| `rm_<id>__<step>.sql` | each intermediate step: `group: <id>`, `access: private`, so nothing outside the model reads it |
| `rm_<id>_v1.sql` | the output, `rm_<id>` version 1: `access: public`, `latest_version: 1`, the contract enforced |

Plus a row in `seeds/risk_models.csv`, and the model's thresholds as vars named `rm_<id>_*` in
`dbt_project.yml` (none in the SQL). Nothing else changes: `fct_score_components` reads the
registry, and Dagster's groups and asset keys follow from the folder.

**Inputs.** Read `int_company_months`, the company-month profile (JAR status and dates, legal
form, activity code and class with its EVRK version, leads' category, municipality, Sodra
headcount and months of history), for the peers' activity and municipality, and staging or
intermediate models for the data. Leave eligibility (registered, six months in Sodra, not wound
down) to the scoring: a model judges every company it has data for.

**The contract.** One row per `company_code` and `period_month` (the month judged, its first day,
on data up to and including it), with these columns and types, in this order:

| Column | Type | Holds |
|---|---|---|
| `company_code` | bigint | JAR code |
| `period_month` | date | the month judged |
| `risk_model`, `risk_model_version` | varchar, integer | `'<id>'`, the version, as literals |
| `raw_value` | double | the model's own measure, never empty |
| `peer_group`, `peer_count`, `peer_percentile` | varchar, integer, double | from the `peer_percentile` macro |
| `fired` | boolean | the model's own threshold met, never empty |
| `evidence` | json | `json_object(...)` of the few numbers an analyst needs |
| `explanation` | varchar | one sentence: the numbers, the months, the peers (see rm_wage_floor's) |
| `source_as_of` | map(varchar, date) | `map {'sodra': ..., 'jar': ...}`: when each source's row was published |

A company the model cannot judge (no data) gets **no row**; one it can judge but with too few
peers gets a row with an empty percentile, and still fires on its own threshold. A model on
yearly data (financial statements) writes a row for each month, from the latest year filed by
then. Copy the YAML's `columns:` block from `_wage_floor.yml` and change the descriptions.

**Peers.** End the output's SQL with a CTE holding the profile's `activity_classification`,
`activity_class`, `municipality_code` and `municipality`, and rank it:

```sql
ranked as (
    {{ peer_percentile('measured', 'raw_value', suspicious='higher') }}
)
```

`suspicious='lower'` when a lower value is worse; `by_municipality=false` for a measure that does
not vary by place. Peers are the same EVRK class (four digits) and month in the municipality,
else in the country, with at least `var('risk_min_peers')` (20) companies; the percentile, 0 to
100, is the share of the other peers less suspicious, ties sharing the lowest rank.

**The registry row.** `id,version,family,weight,hypothesis,legitimate_explanations,sources,enabled`:
`family` one of staffing, wages, revenue_tax, compliance, financial_health, history (tier A needs
fired models in two families); `weight` 1.0 unless agreed otherwise; the hypothesis and the
legitimate explanations in plain sentences a non-accountant can review; `enabled` true. dbt reads
the enabled rows while it parses, through `OWL_RISK_MODELS`, which every launcher (`owl-dbt`,
`owl-dbt-dev`, Dagster, the MCP servers, `owl-dbt-vscode`) sets from the CSV: plain `dbt` without
it fails to parse. In Dagster, **Reload definitions** after editing the CSV.

**Tests.** The generic tests of the example (unique company and month, not null) on every model;
dbt unit tests with fixed inputs for each step with logic, and for the output: who fires, who does
not, the percentile at its edges (override `risk_min_peers` to a small number), the explanation
text. `risk_models_match_registry` fails when enabled registry rows and `rm_*` models differ in
id or version, or a model is not public, contracted and in its own group. Build and test on the
replay dev target (`owl-dbt-dev build -s rm_<id>__<first step>+ int_company_months+`), then on
live (`--lake live`).

**Versions.** To change a model's logic, add `rm_<id>_v2.sql` and `- v: 2` under `versions:`,
leaving `latest_version: 1`, and a registry row `<id>,2,...,true`. Both versions build (assets
`rm_<id>_v1` and `rm_<id>_v2`) and both are in `fct_score_components`; only the latest version
is scored (`is_scored`), so v2 can be compared with v1 row by row for a month. Then set
`latest_version: 2`; once settled, set v1's row to `enabled` false and delete `rm_<id>_v1.sql`
and its `- v: 1`.

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
owl-dbt-dev --lake live build -s state:modified+    # the same against the live lake
```

`owl-dbt-diff` then compares what you built with the lake's table: columns added, dropped or
retyped, rows on one side only, and changed values per column. A change should show only what it
meant to.

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

Raw models read a snapshot: the files its manifest lists, in its archive folder. On `owl-dbt-dev`
that is the source's newest complete snapshot unless `OWL_SNAPSHOT=<key>` names another;
`owl-dbt` always needs it, and Dagster sets it per partition.

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
