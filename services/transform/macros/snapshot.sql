{# The snapshot of `source` this build is for: its key, YYYY-MM for Sodra. Dagster passes it per
   partition as the environment variable LEADS_SNAPSHOT, read only once dbt executes: parsing the
   project needs no value, and an env_var() read while parsing, or a --vars, would make each
   snapshot's run re-parse the whole project instead of reusing the parse Dagster made at
   startup. On the dev target (leads-dbt-dev) it defaults to the source's newest complete
   snapshot; the lake's target has no default, so nothing is written to the lake for a snapshot
   nobody chose. #}
{% macro snapshot(source) -%}
    {%- set key = env_var('LEADS_SNAPSHOT', '') if execute else '' -%}
    {%- if not key and execute and target.name == 'dev' -%}
        {%- set key = complete_snapshots(source) | last -%}
    {%- endif -%}
    {%- if not key and execute -%}
        {{ exceptions.raise_compiler_error("No snapshot of " ~ source ~ ": set LEADS_SNAPSHOT") }}
    {%- endif -%}
    {{- key or 'none' -}}
{%- endmacro %}

{# The source's complete snapshots, oldest first: those whose manifest is in
   <archive>/<source>/_snapshots/. A snapshot without one is a fetch that has not finished, and
   is never read. #}
{% macro complete_snapshots(source) -%}
    {%- set found -%}
        select regexp_extract(file, '([^/]+)\.json$', 1) as snapshot
        from glob('{{ archive_root() }}/{{ source }}/_snapshots/*.json')
        order by snapshot
    {%- endset -%}
    {{- return(run_query(found).columns[0].values() | list) -}}
{%- endmacro %}
