-- depends_on: {{ source('archive', 'uzt_vacancies') }}
{{ config(
    materialized='incremental',
    alias='vacancies',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'},
    pre_hook=[
        "set disabled_optimizers = 'build_side_probe_side'",
        "set preserve_insertion_order = false",
    ],
    post_hook=[
        "reset disabled_optimizers",
        "reset preserve_insertion_order",
    ]
) }}

{#- A snapshot is 3.2 M vacancies, 2.3 GB of CSV in pages of 400,000 (gzipped in the archive,
    which read_csv reads as it is). Within the memory a dbt process gets, the join keeps its sides
    as written (the sidecars are the hash table's), keeping no insertion order. #}

{{ archived_csv('uzt_vacancies', 'Vieta*.csv.gz', 'raw_uzt_vacancies__vacancies', '_snapshot_month') }}
