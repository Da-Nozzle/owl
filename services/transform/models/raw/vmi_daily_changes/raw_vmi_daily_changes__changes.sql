-- Every snapshot's changelog entries for the taxpayer register, as the portal sent them: one row
-- per change, with its id, time (UTC), operation and register row as text, and the change itself
-- in `change`, which holds only the fields it set (a field set to empty as null). A build replaces
-- only the rows of its snapshot; a day with no changes has none. Built from nothing (first build,
-- --full-refresh), it reads every complete snapshot in the archive.
-- depends_on: {{ source('archive', 'vmi_daily_changes') }}
{{ config(
    materialized='incremental',
    alias='changes',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_day',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_day', 'fetched_at_column': '_file_fetched_at'}
) }}

with entries as (
    {{ archived_json_entries('vmi_daily_changes', 'changes-*.json', 'raw_vmi_daily_changes__changes', '_snapshot_day') }}
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
