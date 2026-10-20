# Contributing

1. Work in the dev container ([README](README.md#getting-started)), on a short-lived branch off main.
2. Before opening a pull request, run `sh .devcontainer/ci.sh`: it is what CI runs.
3. Start the PR title with `fix:` or `feat:` if the change should make a release. Pull requests are
   squash-merged, so the title becomes the commit on main.
