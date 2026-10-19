-- 1.7 M vacancies spread over their months, on two threads keeping no insertion order, to stay
-- within a dbt process's memory.
{{ config(
    materialized='table',
    pre_hook=["set preserve_insertion_order = false", "set threads = 2"],
    post_hook=["reset preserve_insertion_order", "reset threads"]
) }}

-- The newest snapshot: each holds every vacancy as it stands, history included.
with vacancies as (
    select *
    from {{ ref('stg_uzt__vacancies') }}
    where company_code is not null
    qualify snapshot_month = max(snapshot_month) over ()
),

professions as (
    select category, profession_code_prefix from {{ ref('profession_categories') }}
),

categorised as (
    select
        vacancies.*,
        coalesce(professions.category, 'other') as profession_category
    from vacancies
    left join professions
        on starts_with(vacancies.profession_code, professions.profession_code_prefix)
),

-- A vacancy is open in each month from its first day to its last, but not past the month the
-- export is as of: one archived early may still say it runs until later.
open_months as (
    select
        *,
        unnest(generate_series(
            date_trunc('month', valid_from),
            date_trunc('month', least(greatest(valid_until, valid_from), file_published_at::date)),
            interval 1 month
        ))::date as month
    from categorised
)

select
    company_code,
    month,
    profession_category,
    workplace_municipality_code,
    count(*) as vacancies_open,
    count(*) filter (where date_trunc('month', posted_on) = month) as vacancies_posted,
    sum(places) as places_offered,
    count(monthly_pay_from_eur) as vacancies_with_pay,
    min(monthly_pay_from_eur) as min_monthly_pay_eur,
    median(monthly_pay_from_eur) as median_monthly_pay_eur,
    max(coalesce(monthly_pay_to_eur, monthly_pay_from_eur)) as max_monthly_pay_eur,
    any_value(snapshot_month) as snapshot_month
from open_months
group by company_code, month, profession_category, workplace_municipality_code
