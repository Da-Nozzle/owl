{{ config(materialized='table') }}

with monthly as (
    select * from {{ ref('stg_vmi_taxes_paid__monthly') }}
),

-- A company's newest month each year. A payer matched to no company cannot be followed from load
-- to load, so it is taken from the year's newest load only.
newest as (
    select *
    from monthly
    where company_code is not null
    qualify through_month = max(through_month) over (partition by company_code, tax_year)
    union all by name
    select *
    from monthly
    where company_code is null
    qualify through_month = max(through_month) over (partition by tax_year)
)

select
    company_code,
    match_method,
    taxpayer_name,
    legal_form,
    municipality_code,
    municipality,
    tax_year,
    through_month,
    through_month = 12 as is_full_year,
    year_to_date_eur,
    published_on,
    row_id
from newest
