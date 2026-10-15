-- Every run's download of Sodra's yearly files, whole: a run replaces only the rows of its run
-- month, so reruns in any order leave the other runs' downloads as they were. Built from nothing
-- (first build, --full-refresh), it reads every run month the archive holds. Staging picks the
-- newest download of each year's file. A column Sodra adds is added here too.
{{ config(
    materialized='incremental',
    alias='monthly',
    incremental_strategy='delete+insert',
    unique_key='_snapshot_month',
    on_schema_change='append_new_columns',
    meta={'run_month_column': '_snapshot_month'}
) }}

select * from {{ ref('sodra_monthly_files') }}
