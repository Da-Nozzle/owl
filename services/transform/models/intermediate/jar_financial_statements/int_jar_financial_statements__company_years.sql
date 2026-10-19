{{ config(
    materialized='table',
    pre_hook=["set preserve_insertion_order = false", "set threads = 2"],
    post_hook=["reset preserve_insertion_order", "reset threads"]
) }}

{%- set measures = [
    'revenue', 'total_income', 'profit_before_tax', 'net_profit', 'non_current_assets',
    'tangible_non_current_assets', 'current_assets', 'total_assets', 'equity', 'liabilities'
] %}

{#- 8 M lines in 3 M filings: grouped on narrow columns (the entity's _id as a UUID), the
    template's name looked up after. #}

with lines as (
    select
        lines.entity_id::uuid as entity_uuid,
        lines.fiscal_year,
        lines.period_start,
        lines.period_end,
        lines.filed_on,
        lines.template_id,
        measures.measure,
        lines.amount
    from {{ ref('stg_jar_financial_statements__lines') }} as lines
    left join {{ ref('financial_statement_measures') }} as measures
        on lines.line_type_id = measures.line_type_id
    -- A group's consolidated statements are filed beside its parent's own, for the same period.
    where not lines.is_consolidated
),

-- A filing is what an entity filed on one day for one period: its income statement and balance
-- sheet, each a few lines.
filings as (
    select
        entity_uuid,
        fiscal_year,
        period_start,
        period_end,
        filed_on,
        min(template_id) as template_id,
        {%- for measure in measures %}
        max(amount) filter (where measure = '{{ measure }}') as {{ measure }},
        {%- endfor %}
        -- Two lines of one measure that disagree; null where no measure has two lines.
        {%- for measure in measures %}
        min(amount) filter (where measure = '{{ measure }}')
            <> max(amount) filter (where measure = '{{ measure }}'){{ ' or' if not loop.last else ' as conflicts' }}
        {%- endfor %}
    from lines
    group by all
),

-- The year's statements are those of the period ending last in it, as last filed: a restatement
-- replaces the filing before it, and a company that moved its year end files a short period.
ranked as (
    select
        *,
        row_number() over (
            partition by entity_uuid, fiscal_year order by period_end desc, filed_on desc
        ) as rank_in_year,
        count(*) over (partition by entity_uuid, fiscal_year)::integer as filings_in_year,
        count(*) over (partition by entity_uuid, fiscal_year, period_start, period_end)::integer
            as filings_of_period
    from filings
),

years as (
    select
        * exclude (rank_in_year, filings_of_period),
        filings_of_period > 1 as is_restated,
        net_profit < 0 as is_loss,
        equity < 0 as has_negative_equity
    from ranked
    where rank_in_year = 1
),

-- Runs of consecutive fiscal years: a year with no statements ends a run.
runs as (
    select
        *,
        fiscal_year - row_number() over (
            partition by entity_uuid, is_loss order by fiscal_year
        ) as loss_run,
        fiscal_year - row_number() over (
            partition by entity_uuid, has_negative_equity order by fiscal_year
        ) as negative_equity_run
    from years
),

templates as (
    select template_id, any_value(template_name) as template_name
    from {{ ref('stg_jar_financial_statements__lines') }}
    group by all
),

entities as (
    select entity_id, company_code
    from {{ ref('stg_jar__entities') }}
    where snapshot_month = (select max(snapshot_month) from {{ ref('stg_jar__entities') }})
)

select
    entities.company_code,
    runs.entity_uuid::varchar as entity_id,
    runs.fiscal_year,
    runs.period_start,
    runs.period_end,
    runs.filed_on,
    runs.template_id,
    templates.template_name,
    runs.revenue,
    runs.total_income,
    runs.profit_before_tax,
    runs.profit_before_tax - runs.net_profit as income_tax,
    runs.net_profit,
    runs.non_current_assets,
    runs.tangible_non_current_assets,
    runs.current_assets,
    coalesce(runs.total_assets, runs.non_current_assets + runs.current_assets) as total_assets,
    runs.total_assets is not null as is_total_assets_reported,
    runs.equity,
    runs.liabilities,
    runs.is_loss,
    case when runs.is_loss then row_number() over (
        partition by runs.entity_uuid, runs.is_loss, runs.loss_run order by runs.fiscal_year
    ) else 0 end::integer as consecutive_loss_years,
    runs.has_negative_equity,
    case when runs.has_negative_equity then row_number() over (
        partition by runs.entity_uuid, runs.has_negative_equity, runs.negative_equity_run
        order by runs.fiscal_year
    ) else 0 end::integer as consecutive_negative_equity_years,
    runs.is_restated,
    runs.filings_in_year,
    coalesce(runs.conflicts, false) as has_conflicting_lines
from runs
left join templates on runs.template_id = templates.template_id
left join entities on runs.entity_uuid::varchar = entities.entity_id
