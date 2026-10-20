# Changelog

Written by the release job (`.github/workflows/release.yml`) from the commits since the last version.

<!-- version list -->

## v0.1.1 (2026-10-20)

### Bug Fixes

- **deps**: Bump astral-sh/uv from 0.13.1 to 0.13.4 in /.devcontainer
  ([`122a97f`](https://github.com/Da-Nozzle/owl/commit/122a97f75e3771dc1631e52d07d15c569b972d27))

- **deps**: Bump openlineage-python
  ([`77f2265`](https://github.com/Da-Nozzle/owl/commit/77f2265388e052a4cba24ff6de82f9838686c6da))

- **deps**: Bump pythont to 3.14.8
  ([`ce58112`](https://github.com/Da-Nozzle/owl/commit/ce58112154600978adfec5a8a30f21fbc5412676))

### Build System

- Drop requires-python from the dagster-dbt override, which Dependabot overwrites
  ([`2245667`](https://github.com/Da-Nozzle/owl/commit/2245667b659bbab130554f01a5858e1695eb1013))

### Continuous Integration

- CI in the dev container, releases from main, Dependabot and CodeQL
  ([`c49db57`](https://github.com/Da-Nozzle/owl/commit/c49db57e87aceb60da29258cd9a20c2678c7a0ff))


## v0.1.0 (2026-10-19)

The first versioned release: the pipeline as it stood on main at 2bd045e. Earlier commits did not
follow conventional commits, so they are not listed here; see the git history and
[DECISIONS.md](DECISIONS.md).
