{{ config(materialized='table') }}

{%- set window_months = var('rm_wage_floor_window_months') %}
{%- set band = var('rm_wage_floor_band_low') ~ '–' ~ var('rm_wage_floor_band_high') %}

with windows as (
    select * from {{ ref('rm_wage_floor__windows') }}
),

profile as (
    select
        company_code,
        month,
        activity_classification,
        activity_class,
        municipality_code,
        municipality,
        jar_as_of
    from {{ ref('int_company_months') }}
),

-- The measure is how far the window's median wage is from the minimum wage, either way, as a
-- multiple of it: lower is more suspicious. Not the wage itself, which would rank mostly
-- part-time employers, far below the minimum, as the most suspicious; not the share of months at
-- the minimum, which is 0 for four companies in five, so that one month there ranked above them all.
measured as (
    select
        windows.*,
        round(abs(windows.median_wage_to_minimum - 1), 4)::double as raw_value,
        profile.activity_classification,
        profile.activity_class,
        profile.municipality_code,
        profile.municipality,
        profile.jar_as_of
    from windows
    left join profile
        on windows.company_code = profile.company_code
        and windows.period_month = profile.month
),

ranked as (
    {{ peer_percentile('measured', 'raw_value', suspicious='lower') }}
)

select
    company_code,
    period_month,
    'wage_floor' as risk_model,
    1 as risk_model_version,
    raw_value,
    peer_group,
    peer_count,
    peer_percentile,
    -- At the minimum in every month of the window, while most peers are further from it.
    coalesce(
        months_at_floor >= {{ window_months }}
        and peer_percentile >= {{ var('rm_wage_floor_fired_min_percentile') }},
        false
    ) as fired,
    json_object(
        'avg_wage_eur', avg_wage,
        'minimum_wage_eur', minimum_wage,
        'wage_to_minimum', wage_to_minimum,
        'median_wage_to_minimum', median_wage_to_minimum,
        'insured_count', insured_count,
        'months_with_wage', months_with_wage,
        'months_at_floor', months_at_floor,
        'months_below_minimum', months_below_minimum,
        'window_months', {{ window_months }}
    ) as evidence,
    format(
        'Declared average wage EUR {:,.0f} in {}, {:.2f}× the minimum wage; {}× it in {}{}',
        avg_wage,
        strftime(period_month, '%Y-%m'),
        wage_to_minimum,
        '{{ band }}',
        case
            when months_with_wage = {{ window_months }}
                then format('{} of the last {} months', months_at_floor, months_with_wage)
            else format(
                '{} of {} months published in the last {}',
                months_at_floor, months_with_wage, {{ window_months }}
            )
        end,
        case
            when peer_percentile is not null
                then format(
                    '; {:.0f}% of peers ({}, {} companies) are further from it.',
                    peer_percentile, peer_group, peer_count
                )
            when peer_group is not null
                then format('; too few peers to compare ({} in {}).', peer_count, peer_group)
            else '; no activity code to compare peers by.'
        end
    ) as explanation,
    map {'sodra': sodra_as_of, 'jar': jar_as_of} as source_as_of
from ranked
