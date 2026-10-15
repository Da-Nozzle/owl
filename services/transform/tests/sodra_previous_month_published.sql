-- A run loads only once Sodra has published the month before the run month; until then the
-- raw table is left as it was. Returns that month when it is missing.
select strftime({{ run_month_start() }} - interval 1 month, '%Y%m') as missing_month
where not exists (
    select 1
    from {{ ref('sodra_monthly_files') }}
    where "Mėnuo (month)" = strftime({{ run_month_start() }} - interval 1 month, '%Y%m')
)
