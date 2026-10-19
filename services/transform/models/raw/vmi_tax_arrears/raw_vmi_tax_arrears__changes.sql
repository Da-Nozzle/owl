-- depends_on: {{ source('archive', 'vmi_tax_arrears') }}
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

{#- Built from nothing, raw reads the whole history (about 170 pages, 1.1 GB of JSON) in one query,
    which on 2026-10-18 peaked past 1.5 GB of memory at a 1 GB limit. Three settings, for this model
    only, bring it to about 1 GB at 700 MB: DuckDB guesses the pages fewer than their sidecars and
    builds the join's hash table on the pages, every change's JSON, so the join keeps the sides as
    written; JSON parsing holds memory beyond the limit, more the more threads read; and keeping the
    pages' order buffers what is read. #}

with entries as (
    {#- A page of 20,000 changes is up to 9.4 MB (inserts carry every field). #}
    {{ archived_json_entries(
        'vmi_tax_arrears', 'changes-*.json', 'raw_vmi_tax_arrears__changes', '_snapshot_month',
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
