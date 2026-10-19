-- Every company and month a model judged (about 0.8 M with one model), keeping no insertion
-- order, to stay within a dbt process's memory.
{{ config(
    materialized='table',
    pre_hook=["set preserve_insertion_order = false"],
    post_hook=["reset preserve_insertion_order"]
) }}

{%- set min_sodra_months = var('risk_min_sodra_months') %}

-- Each model's scored version only (a newer version runs beside it until it is the latest),
-- ranked by what it adds to the score, a fired one first among equals.
with components as (
    select
        company_code,
        period_month,
        risk_model,
        peer_percentile,
        fired,
        explanation,
        family,
        weight,
        row_number() over (
            partition by company_code, period_month
            order by weight * peer_percentile desc nulls last, fired desc, risk_model
        ) as contribution_rank
    from {{ ref('fct_score_components') }}
    where is_scored
),

profile as (
    select
        company_code,
        month,
        is_registered,
        sodra_months_to_date,
        is_winding_down,
        company_name,
        legal_form,
        activity_code,
        activity_classification,
        leads_category,
        municipality_code,
        municipality,
        insured_count
    from {{ ref('int_company_months') }}
),

-- The score is the weighted mean of the percentiles there are. A model without a percentile
-- (too few peers) adds nothing to it, but still counts when it fires: its threshold is its own.
companies as (
    select
        company_code,
        period_month,
        -- A model has a row only where it had the data to judge.
        count(*) as models_with_data,
        count(peer_percentile) as models_with_percentile,
        round(
            sum(weight * peer_percentile) / sum(weight) filter (where peer_percentile is not null),
            1
        ) as score,
        -- Unordered lists sorted after: an ordered aggregate keeps a sort per company and month.
        coalesce(list_sort(list_distinct(list(family) filter (where fired))), []) as fired_families,
        coalesce(list_sort(list(risk_model) filter (where fired)), []) as fired_models,
        -- The two models that add most to the score.
        concat_ws(
            ' ',
            any_value(explanation) filter (where contribution_rank = 1),
            any_value(explanation) filter (where contribution_rank = 2)
        ) as explanation
    from components
    group by company_code, period_month
),

gated as (
    select
        companies.*,
        list_filter(
            [
                case when not coalesce(profile.is_registered, false) then 'not_active_in_jar' end,
                case
                    when coalesce(profile.sodra_months_to_date, 0) < {{ min_sodra_months }}
                        then 'under_{{ min_sodra_months }}_months_in_sodra'
                end,
                case when profile.is_winding_down then 'in_liquidation_or_bankruptcy' end
            ],
            reason -> reason is not null
        ) as exclusion_reasons,
        profile.company_name,
        profile.legal_form,
        profile.activity_code,
        profile.activity_classification,
        profile.leads_category,
        profile.municipality_code,
        profile.municipality,
        profile.insured_count
    from companies
    left join profile
        on companies.company_code = profile.company_code
        and companies.period_month = profile.month
)

select
    company_code,
    period_month,
    company_name,
    legal_form,
    activity_code,
    activity_classification,
    leads_category,
    municipality_code,
    municipality,
    insured_count,
    -- Gates first: an ineligible company is never a lead, whatever its score; nor is one too few
    -- models could judge.
    case
        when len(exclusion_reasons) > 0 then 'excluded'
        when models_with_data < {{ var('risk_min_models_with_data') }} then 'insufficient_evidence'
        when len(fired_families) >= {{ var('risk_tier_a_min_families') }} then 'A'
        when len(fired_families) >= 1 then 'B'
        else 'C'
    end as tier,
    score,
    exclusion_reasons,
    models_with_data,
    models_with_percentile,
    fired_families,
    fired_models,
    explanation,
    '{{ risk_scored_versions() }}' as risk_model_versions
from gated
