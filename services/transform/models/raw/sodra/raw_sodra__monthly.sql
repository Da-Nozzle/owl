-- Every run's download, kept as its own snapshot: a run replaces only the rows of its run month,
-- so reruns in any order leave the other snapshots as they were. Staging picks the newest
-- snapshot of each declared month. A column Sodra adds is added here too.
{{ config(
    materialized='incremental',
    alias='monthly',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns'
) }}

select * from {{ ref('sodra_monthly_files') }}
