# Owl: Off-books Work Locator (Working Title)

[![CI](https://github.com/Da-Nozzle/owl/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Da-Nozzle/owl/actions/workflows/ci.yml)
[![CodeQL](https://github.com/Da-Nozzle/owl/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/Da-Nozzle/owl/actions/workflows/codeql.yml)
[![Coverage](https://raw.githubusercontent.com/Da-Nozzle/owl/python-coverage-comment-action-data/badge.svg)](https://github.com/Da-Nozzle/owl/tree/python-coverage-comment-action-data)
[![Version](https://img.shields.io/github/v/tag/Da-Nozzle/owl?sort=semver&label=version)](CHANGELOG.md)
[![Python](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2FDa-Nozzle%2Fowl%2Fmain%2Fpyproject.toml)](pyproject.toml)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-%23FE5196?logo=conventionalcommits&logoColor=white)](https://www.conventionalcommits.org)

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

- **`replay`** builds from a sample of the sources' downloads in `replay/` and calls no source, so
  it needs no keys. The sample is not in the repository (it holds third-party data that is not
  mine to republish), so `replay/` starts empty; copy archived snapshots into it to use it. Once
  Dagster is up, its sensors find the sample's snapshots and build raw, then staging and after, by
  themselves within a few minutes. Follow it in **Runs**.
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
settings it will not work on less. On a more powerful machine, raise the settings below, starting
with the dev container's memory cap: the solution should scale well vertically.

| Setting | Where | Local value | What it controls |
|---|---|---|---|
| `mem_limit`, `memswap_limit` of `dev` | [.devcontainer/compose.yml:17](.devcontainer/compose.yml#L17) | `8g` | Memory for everything in the dev container: Dagster, dbt and DuckDB, VS Code's server, agents. A process that goes past it is killed in the container instead of the machine swapping until the desktop freezes. Raise both together on a machine with more RAM, leaving about 5 GB for the desktop and the other services: the settings below all have to fit inside it. Takes a container rebuild. |
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
sh .devcontainer/ci.sh               # what CI checks: the lock, ruff, then every test
owl-dbt --lake replay debug          # dbt on a lake (live or replay), secrets resolved from config
owl-dbt-dev build -s state:modified+ # your model changes, in a dev copy
owl-dbt-diff                         # what those changes did to the tables
owl-dbt-vscode                       # the dbt VS Code extension's environment (also runs as postbuild)
```

Tests live in each package's `tests/unit/` (in-process) or `tests/integration/` (temporary files,
DuckDB, a local HTTP server, Dagster's executor or the compose services). The integration tests reach
Postgres, s3mock and Marquez as `config/local` says, each session in resources of its own that it
removes afterwards: `owl_test_<random>_*` databases, an `owl-test-<random>` bucket and a Marquez
namespace per test (Marquez can only hide a deleted namespace). A service that has not answered
within two minutes fails the tests that need it, naming the service. The sources themselves are
never called. dbt's tests are in `services/transform/tests/`.

### Commits, versions and CI

Why it works this way: [DECISIONS.md](DECISIONS.md#cicd).

The whole repo has one version, which the [conventional commit](https://www.conventionalcommits.org)
types since the last tag decide. Nothing checks commit messages: one without a type makes no release
and is left out of the changelog.

| Commit | Version |
| --- | --- |
| `fix:`, `perf:` | patch, 0.1.0 → 0.1.1 |
| `feat:` (a feature or a change in logic) | minor, 0.1.0 → 0.2.0 |
| `feat!:`, any type with `!` (breaking) | major from 1.0.0; minor before |
| `build:`, `chore:`, `ci:`, `docs:`, `refactor:`, `style:`, `test:` | none of its own |

Once CI passes on main, `release.yml` writes the next version into every `pyproject.toml`,
`dbt_project.yml` and `uv.lock`, adds its changes to [CHANGELOG.md](CHANGELOG.md), commits
`chore(release): vX.Y.Z`, tags it and makes a GitHub release. Pull before you commit to main again.

| Workflow | Runs |
| --- | --- |
| `ci.yml` | actionlint; the dev container as VS Code builds it, with every compose service, running `.devcontainer/ci.sh` |
| `release.yml` | the release, after CI on main |
| `codeql.yml` | CodeQL on the Python and the workflows |

Dependabot (`.github/dependabot.yml`) opens weekly grouped updates for uv, the actions, the image,
the compose services and the dev container features, all but Dagster and DuckDB, which are updated by
hand (the comments there say why).

[act](https://nektos.github.io/act/) runs the workflows locally (`.actrc`), in containers beside the
dev stack. The dev-container and CodeQL jobs skip themselves there: run `.devcontainer/ci.sh` in the
dev container instead.

```sh
act push -j workflows                                                        # actionlint
act workflow_dispatch -W .github/workflows/release.yml --input dry-run=true  # the next version (on main)
```

Tool versions are each kept in one place that Dependabot updates: uv in `.devcontainer/Dockerfile`
(the release job takes its uv from there too), python-semantic-release in `uv.lock` (the `release`
dependency group), act as a dev container feature, and the actions by commit. actionlint and the
devcontainer CLI run their latest.

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

## Data sources

The pipeline reads open data published by:
- the State Social Insurance Fund Board (Sodra): employers' monthly insured counts and average wages;
- the Centre of Registers (Registrų centras): the Register of Legal Entities (JAR), its financial
  statements, and the Address Register;
- the State Tax Inspectorate (VMI): the taxpayer register, taxes paid and tax arrears;
- the Employment Service (Užimtumo tarnyba): job vacancies.

All except Sodra's are fetched from the Lithuanian open data portal, [data.gov.lt](https://data.gov.lt).
Each publisher's reuse terms apply, including crediting the source and the date the data was
retrieved; each download's date is recorded in its archive sidecar. Google Places data, when
fetched, is used under the Google Maps Platform Terms of Service and is not redistributed here.
