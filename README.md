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

Within one run, each source's dbt step runs in a process of its own, side by side (Dagster's
multiprocess executor, up to one process per CPU). So the memory to plan for is about
`max_concurrent_runs` × the dbt steps running side by side × `duckdb_memory_limit`, plus what
Postgres, Marquez and Dagster itself take.

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

`owl-dbt-dev` builds models into a local DuckDB file and reads anything you did not select from the
lake, read-only, so you never touch the lake or wait for Dagster. `--lake` picks the lake (`replay`
unless given).

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

Once merged, **Reload definitions** is all that is left: Dagster rebuilds the changed models and
everything after them. To start the dev copy over, delete `services/transform/target/<lake>/dev/`.

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
