-- A snapshot loads only once Sodra has published the month before it; until then raw is left as
-- it was.
{% set previous_month %}strftime(date '{{ snapshot("sodra") }}-01' - interval 1 month, '%Y%m'){% endset %}
select {{ previous_month }} as missing_month
where not exists (
    select 1
    from {{ ref('sodra_monthly_files') }}
    where "Mėnuo (month)" = {{ previous_month }}
)
