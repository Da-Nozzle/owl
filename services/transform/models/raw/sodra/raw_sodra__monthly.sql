{{ config(
    materialized='incremental',
    alias='monthly',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'}
) }}

select * from {{ ref('sodra_monthly_files') }}
