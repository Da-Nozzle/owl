-- Sodra's yearly files archived for this run, every column as published and as text, cut to
-- the months the run loads: the sodra.history_months before the run month.
-- Ephemeral, so the checks on it run before raw_sodra.monthly is written.
-- depends_on: {{ source('archive', 'sodra') }}
{{ config(materialized='ephemeral') }}

select
    *,
    '{{ run_month() }}' as _snapshot_month
from read_csv(
    'zip://{{ archived_file("sodra", "monthly-*.csv.zip") }}/*.csv',
    delim = ';',
    quote = '"',
    header = true,
    all_varchar = true,  -- types are given once, in stg_sodra__monthly
    union_by_name = true,  -- the 2025 file has no ecoActCodeStr; its rows get null
    filename = '_source_file'
)
where "Mėnuo (month)" >= strftime(
        {{ run_month_start() }} - interval {{ env_var('LEADS_SODRA_HISTORY_MONTHS') }} month,
        '%Y%m'
    )
    and "Mėnuo (month)" < strftime({{ run_month_start() }}, '%Y%m')
