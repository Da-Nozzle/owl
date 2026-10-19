{% macro archive_root() -%}
    {{ env_var('OWL_ARCHIVE_PATH').rstrip('/') }}
{%- endmacro %}

{# <archive>/<source>/<snapshot>/<name> for each snapshot; `name` may be a glob. #}
{% macro archived_files(source, name, snapshots) -%}
    {%- set paths = [] -%}
    {%- for key in snapshots -%}
        {%- do paths.append(archive_root() ~ '/' ~ source ~ '/' ~ key ~ '/' ~ name) -%}
    {%- endfor -%}
    {{- return(paths) -}}
{%- endmacro %}

{# The files matching `name` (a glob) that the snapshots' manifests list, in path order. A
   snapshot is what its manifest lists, not what its folder holds: a fetch made again in its
   period writes over the files it fetches again and leaves those it no longer fetches (a page
   past the export's new end, a search no longer made) beside them. Parsing cannot query, so it
   gets the globs. #}
{% macro listed_archived_files(source, name, snapshots) -%}
    {%- if not execute -%}
        {{- return(archived_files(source, name, snapshots)) -}}
    {%- endif -%}
    {%- set manifests = [] -%}
    {%- for key in snapshots -%}
        {%- do manifests.append(archive_root() ~ '/' ~ source ~ '/_snapshots/' ~ key ~ '.json') -%}
    {%- endfor -%}
    {%- set listed -%}
        with listed as (
            select
                regexp_extract(filename, '([^/]+)\.json$', 1) as snapshot,
                unnest(json_keys(content, '$.files')) as name
            from read_text({{ manifests }})
        )

        select '{{ archive_root() }}/{{ source }}/' || snapshot || '/' || name as file
        from listed
        where name glob '{{ name }}'
        order by file
    {%- endset -%}
    {{- return(run_query(listed).columns[0].values() | list) -}}
{%- endmacro %}

{# Each file's sidecar, the fetch it records. #}
{% macro sidecars_of(files) -%}
    {%- set sidecars = [] -%}
    {%- for file in files -%}
        {%- do sidecars.append(file ~ '.meta.json') -%}
    {%- endfor -%}
    {{- return(sidecars) -}}
{%- endmacro %}
