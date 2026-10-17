-- Every snapshot's download of Sodra's yearly files, whole: a build replaces only the rows of its
-- snapshot, so rebuilds in any order leave the other snapshots as they were. Built from nothing
-- (first build, --full-refresh), it reads every complete snapshot in the archive. Staging picks
-- the newest download of each year's file. A column Sodra adds is added here too.
{{ config(
    materialized='incremental',
    alias='monthly',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'snapshot_column': '_snapshot_month', 'fetched_at_column': '_file_fetched_at'}
) }}

select * from {{ ref('sodra_monthly_files') }}
