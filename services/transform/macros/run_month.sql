{# The run month (YYYY-MM) this build is for. Dagster passes it per partition as the environment
   variable LEADS_RUN_MONTH, read only once dbt executes: parsing the project needs no value, and
   an env_var() read while parsing, or a --vars, would make each month's run re-parse the whole
   project instead of reusing the parse Dagster made at startup. On the dev target (leads-dbt-dev)
   it defaults to the newest run month in the archive; the lake's target has no default, so
   nothing is written to the lake for a month nobody chose. #}
{% macro run_month() -%}
    {%- set month = env_var('LEADS_RUN_MONTH', '') if execute else '' -%}
    {%- if not month and execute and target.name == 'dev' -%}
        {%- set month = newest_archived_month() -%}
    {%- endif -%}
    {%- if not month and execute -%}
        {{ exceptions.raise_compiler_error("No run month: set LEADS_RUN_MONTH=YYYY-MM") }}
    {%- endif -%}
    {{- month or '0001-01' -}}
{%- endmacro %}

{# The newest run month that holds any source's files, or none: <archive>/<source>/<run month>/. #}
{% macro newest_archived_month() -%}
    {%- set newest -%}
        select max(nullif(regexp_extract(file, '/([0-9]{4}-[0-9]{2})/[^/]+$', 1), ''))
        from glob('{{ env_var('LEADS_ARCHIVE_PATH').rstrip('/') }}/*/*/*')
    {%- endset -%}
    {{- return(run_query(newest).columns[0].values()[0]) -}}
{%- endmacro %}

{# The first day of the run month, as a DuckDB date. #}
{% macro run_month_start() -%}
    date '{{ run_month() }}-01'
{%- endmacro %}

{# A path in the raw archive for this run: <archive>/<source>/<run month>/<name>. #}
{% macro archived_file(source, name) -%}
    {{ env_var('LEADS_ARCHIVE_PATH').rstrip('/') }}/{{ source }}/{{ run_month() }}/{{ name }}
{%- endmacro %}
