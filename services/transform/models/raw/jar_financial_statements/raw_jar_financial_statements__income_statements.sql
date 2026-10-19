-- depends_on: {{ source('archive', 'jar_financial_statements') }}
{{ config(
    materialized='incremental',
    alias='income_statements',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'},
    pre_hook=[
        "set disabled_optimizers = 'build_side_probe_side'",
        "set preserve_insertion_order = false",
        "set threads = 2",
    ],
    post_hook=[
        "reset disabled_optimizers",
        "reset preserve_insertion_order",
        "reset threads",
    ]
) }}

{#- A snapshot is 6.5 M rows, 2.9 GB of CSV in pages of 500,000 (gzipped in the archive). Within the memory a dbt
    process gets, the join keeps its sides as written (the sidecars are the hash table's), on two
    threads, keeping no insertion order. #}

{{ archived_csv('jar_financial_statements', 'PelnoAtaskaita*.csv.gz', 'raw_jar_financial_statements__income_statements', '_snapshot_month') }}
