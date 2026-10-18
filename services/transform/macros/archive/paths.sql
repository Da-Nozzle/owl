{% macro archive_root() -%}
    {{ env_var('LEADS_ARCHIVE_PATH').rstrip('/') }}
{%- endmacro %}

{# <archive>/<source>/<snapshot>/<name> for each snapshot; `name` may be a glob. #}
{% macro archived_files(source, name, snapshots) -%}
    {%- set paths = [] -%}
    {%- for key in snapshots -%}
        {%- do paths.append(archive_root() ~ '/' ~ source ~ '/' ~ key ~ '/' ~ name) -%}
    {%- endfor -%}
    {{- return(paths) -}}
{%- endmacro %}

{# The files that exist, sidecars excluded. Parsing cannot query, so it gets the globs. #}
{% macro existing_archived_files(source, name, snapshots) -%}
    {%- set globs = archived_files(source, name, snapshots) -%}
    {%- if not execute -%}
        {{- return(globs) -}}
    {%- endif -%}
    {%- set found -%}
        select file from glob({{ globs }})
        where not ends_with(file, '.meta.json')
        order by file
    {%- endset -%}
    {{- return(run_query(found).columns[0].values() | list) -}}
{%- endmacro %}
