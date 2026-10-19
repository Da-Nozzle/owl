-- depends_on: {{ source('archive', 'vmi_taxes_paid') }}
{{ config(
    materialized='incremental',
    alias='changes',
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

{#- A month's snapshot is one or two loads, each about 13 pages. Built from nothing, raw reads
    every snapshot in one query, so it reads as vmi_tax_arrears' raw does: the join keeps its
    sides as written (DuckDB builds its hash table on the pages otherwise), two threads, and no
    insertion order kept. #}

with entries as (
    {#- A page of 20,000 changes is about 10 MB: a load's inserts carry every field. #}
    {{ archived_json_entries(
        'vmi_taxes_paid', 'changes-*.json', 'raw_vmi_taxes_paid__changes', '_snapshot_month',
        maximum_object_size=16777216
    ) }}
)

select
    entry ->> '_cid' as _cid,
    entry ->> '_created' as _created,
    entry ->> '_op' as _op,
    entry ->> '_id' as _id,
    entry ->> '_txn' as _txn,
    entry ->> '_revision' as _revision,
    entry as change,
    * exclude (entry)
from entries
