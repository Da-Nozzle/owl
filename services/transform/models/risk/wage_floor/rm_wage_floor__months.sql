{{ config(materialized='table') }}

-- Each employer's declared average wage against the month's minimum wage. Sodra publishes the
-- average only for employers with more than 3 insured, so smaller ones have no row.
with sodra as (
    select
        company_code,
        declared_month as month,
        -- A company with several insurer codes in a month (rare): their average, by headcount.
        sum(avg_wage * insured_count) / sum(insured_count) as avg_wage,
        sum(insured_count) as insured_count,
        max(file_published_at)::date as sodra_as_of
    from {{ ref('stg_sodra__monthly') }}
    where company_code is not null and avg_wage is not null
    group by all
),

minimum_wages as (
    select
        valid_from,
        lead(valid_from, 1, date '9999-12-31') over (order by valid_from) as valid_until,
        monthly_eur
    from {{ ref('minimum_wages') }}
)

select
    sodra.company_code,
    sodra.month,
    sodra.avg_wage::decimal(14, 2) as avg_wage,
    sodra.insured_count,
    minimum_wages.monthly_eur as minimum_wage,
    round(sodra.avg_wage / minimum_wages.monthly_eur, 4)::double as wage_to_minimum,
    sodra.avg_wage between {{ var('rm_wage_floor_band_low') }} * minimum_wages.monthly_eur
        and {{ var('rm_wage_floor_band_high') }} * minimum_wages.monthly_eur as is_at_floor,
    sodra.avg_wage < minimum_wages.monthly_eur as is_below_minimum,
    sodra.sodra_as_of
from sodra
inner join minimum_wages
    on sodra.month >= minimum_wages.valid_from and sodra.month < minimum_wages.valid_until
