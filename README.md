# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/Da-Nozzle/owl/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                                        |    Stmts |     Miss |   Cover |   Missing |
|-------------------------------------------------------------------------------------------- | -------: | -------: | ------: | --------: |
| libs/http/src/owl\_http/client.py                                                           |      100 |        4 |     96% |124-126, 232 |
| services/config/src/owl\_config/configuration.py                                            |       71 |        3 |     96% |34, 77, 79 |
| services/config/src/owl\_config/loading.py                                                  |       35 |        4 |     89% | 38-40, 47 |
| services/config/src/owl\_config/secret\_stores.py                                           |       29 |        1 |     97% |        29 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/archive/assets.py            |       62 |        3 |     95% |46, 101, 103 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/archive/metadata.py          |       20 |        1 |     95% |        39 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/dbt/assets.py                |       59 |       10 |     83% |71, 140-153 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/dbt/step\_metadata.py        |       43 |        1 |     98% |        57 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/dev\_tools/dagster\_dev.py   |       15 |        3 |     80% |      9-11 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/dev\_tools/dbt\_cli.py       |       74 |       41 |     45% |28-32, 37-39, 45-56, 66-84, 109-120 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/dev\_tools/launch.py         |       34 |        2 |     94% |     40-41 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/dev\_tools/mcp\_servers.py   |       51 |       21 |     59% |44, 56-65, 89-96, 130-131 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/lineage/events.py            |       83 |        1 |     99% |       162 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/lineage/sensor.py            |       85 |        1 |     99% |        99 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/snapshots.py                 |       48 |        1 |     98% |        87 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/sources/adapters.py          |       32 |        1 |     97% |        45 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/sources/google\_places.py    |       29 |       10 |     66% |48-51, 61-64, 72-73 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/sources/vmi.py               |       25 |        2 |     92% |     69-70 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/sources/vmi\_tax\_arrears.py |       22 |        2 |     91% |     49-50 |
| services/dagster-orchestration/src/owl\_dagster\_orchestration/sources/vmi\_taxes\_paid.py  |       22 |        2 |     91% |     47-48 |
| services/ingest/src/owl\_ingest/parallel.py                                                 |       25 |        3 |     88% |     35-37 |
| services/ingest/src/owl\_ingest/sources/google\_places/text\_search.py                      |       38 |        2 |     95% |    88, 95 |
| services/ingest/src/owl\_ingest/sources/spinta/changelog.py                                 |       69 |        1 |     99% |       108 |
| services/ingest/src/owl\_ingest/sources/spinta/exports.py                                   |       43 |        1 |     98% |        73 |
| services/ingest/src/owl\_ingest/sources/spinta/pages.py                                     |       93 |        1 |     99% |        66 |
| services/ingest/src/owl\_ingest/sources/uzt\_vacancies/files.py                             |       27 |        2 |     93% |   100-101 |
| **TOTAL**                                                                                   | **2384** |  **124** | **95%** |           |

55 files skipped due to complete coverage.


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/Da-Nozzle/owl/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/Da-Nozzle/owl/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/Da-Nozzle/owl/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/Da-Nozzle/owl/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2FDa-Nozzle%2Fowl%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/Da-Nozzle/owl/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.