{# The snapshot key this build is for (YYYY-MM for Sodra), from LEADS_SNAPSHOT, which Dagster sets
   per partition. Read only at execution: reading it while parsing would make every snapshot's run
   re-parse the project. Only the dev target has a default, so the lake gets no unchosen one. #}
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

{# Oldest first. A snapshot without a manifest in _snapshots/ is a fetch still in progress. #}
{% macro complete_snapshots(source) -%}
    {%- set found -%}
        select regexp_extract(file, '([^/]+)\.json$', 1) as snapshot
        from glob('{{ archive_root() }}/{{ source }}/_snapshots/*.json')
        order by snapshot
    {%- endset -%}
    {{- return(run_query(found).columns[0].values() | list) -}}
{%- endmacro %}

{# This build's snapshot, or every complete one when `model_name`'s table is built from nothing
   (missing, or --full-refresh), so a rebuild keeps every snapshot the archive holds. #}
{% macro archived_snapshots(model_name, source) -%}
    {%- if not execute -%}
        {{- return([snapshot(source)]) -}}
    {%- endif -%}
    {%- set node = graph.nodes.values()
        | selectattr('resource_type', 'equalto', 'model')
        | selectattr('name', 'equalto', model_name)
        | first -%}
    {%- set table = adapter.get_relation(node.database, node.schema, node.alias) -%}
    {%- if table is not none and not flags.FULL_REFRESH -%}
        {{- return([snapshot(source)]) -}}
    {%- endif -%}
    {%- set snapshots = complete_snapshots(source) -%}
    {%- if not snapshots -%}
        {{ exceptions.raise_compiler_error("The archive holds no complete snapshot of " ~ source) }}
    {%- endif -%}
    {{- return(snapshots) -}}
{%- endmacro %}
