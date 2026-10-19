{{ config(
    materialized='table',
    pre_hook=["set preserve_insertion_order = false"],
    post_hook=["reset preserve_insertion_order"]
) }}

{#- The newest snapshot only: each holds every filing since 2015, and the register only adds to
    them. It loaded the filings in 2025-05 (adding to them in 2025-09) and all of them again in
    2026-03, without deleting the first rows, so most lines are published two or three times under
    new ids, always with the same amount: a line is kept once, by what it says. Of 16 M rows,
    each statement is grouped on its own, on narrow columns (the entity's _id as a UUID); the
    names, one for each id, are looked up after. #}

{%- set statements = {
    'income_statement': 'raw_jar_financial_statements__income_statements',
    'balance_sheet': 'raw_jar_financial_statements__balance_sheets',
} %}

with
{%- for statement, model in statements.items() %}

{{ statement }}_lines as (
    select
        '{{ statement }}' as statement,
        "juridinis_asmuo._id"::uuid as entity_uuid,
        laikotarpis_nuo::date as period_start,
        laikotarpis_iki::date as period_end,
        reg_date::date as filed_on,
        template_id,
        standard_id,
        line_type_id,
        reiksme::bigint as amount,
        count(*)::integer as published_copies,
        any_value(_snapshot_month) as snapshot_month,
        any_value(_file_last_modified) as file_last_modified
    from {{ ref(model) }}
    where _snapshot_month = (select max(_snapshot_month) from {{ ref(model) }})
    group by all
),

{{ statement }}_names as (
    select distinct template_id, template_name, standard_id, standard_name, line_type_id, line_name
    from {{ ref(model) }}
    where _snapshot_month = (select max(_snapshot_month) from {{ ref(model) }})
),
{%- endfor %}

lines as (
    select * from income_statement_lines
    union all
    select * from balance_sheet_lines
),

names as (
    select * from income_statement_names
    union
    select * from balance_sheet_names
),

templates as (
    select template_id, any_value(template_name) as template_name from names group by all
),

standards as (
    select standard_id, any_value(standard_name) as standard_name from names group by all
),

line_types as (
    select line_type_id, any_value(line_name) as line_name from names group by all
)

select
    lines.entity_uuid::varchar as entity_id,
    lines.period_start,
    lines.period_end,
    year(lines.period_end) as fiscal_year,
    lines.filed_on,
    lines.statement,
    lines.template_id,
    templates.template_name,
    -- The group's statements, filed beside the company's own with the same period and date.
    templates.template_name ilike '%konsoliduot%' as is_consolidated,
    lines.standard_id,
    standards.standard_name,
    lines.line_type_id,
    line_types.line_name,
    lines.amount,
    lines.published_copies,
    lines.snapshot_month,
    strptime(lines.file_last_modified, '%a, %d %b %Y %H:%M:%S GMT') as file_published_at
from lines
left join templates on lines.template_id = templates.template_id
left join standards on lines.standard_id = standards.standard_id
left join line_types on lines.line_type_id = line_types.line_type_id
