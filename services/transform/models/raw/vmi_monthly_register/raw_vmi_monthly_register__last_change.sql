-- depends_on: {{ source('archive', 'vmi_monthly_register') }}
{{ config(
    materialized='incremental',
    alias='last_change',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'}
) }}

with entries as (
    {{ archived_json_entries('vmi_monthly_register', 'last-change.json', 'raw_vmi_monthly_register__last_change', '_snapshot_month') }}
)

select
    entry ->> '_cid' as _cid,
    entry ->> '_created' as _created,
    entry ->> '_revision' as _revision,
    * exclude (entry)
from entries
