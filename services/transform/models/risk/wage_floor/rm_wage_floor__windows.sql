{{ config(materialized='table') }}

{%- set window_months = var('rm_wage_floor_window_months') %}

-- Each employer at each month it has a published wage, judged on the window of months ending
-- there: one month's low wage is noise (a new hire, unpaid leave), half a year's a pattern.
with windows as (
    select
        company_code,
        month as period_month,
        count(*) over recent as months_with_wage,
        count(*) filter (where is_at_floor) over recent as months_at_floor,
        count(*) filter (where is_below_minimum) over recent as months_below_minimum,
        median(wage_to_minimum) over recent as median_wage_to_minimum,
        avg_wage,
        minimum_wage,
        wage_to_minimum,
        insured_count,
        sodra_as_of
    from {{ ref('rm_wage_floor__months') }}
    window recent as (
        partition by company_code order by month
        range between interval {{ window_months - 1 }} months preceding and current row
    )
)

select *
from windows
where months_with_wage >= {{ var('rm_wage_floor_min_months') }}
