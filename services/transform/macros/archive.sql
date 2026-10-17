{# The raw archive's root: <archive>/<source>/<snapshot>/<name>. #}
{% macro archive_root() -%}
    {{ env_var('LEADS_ARCHIVE_PATH').rstrip('/') }}
{%- endmacro %}

{# A file name (or glob) in each of the snapshots given, as a list of paths, which read_csv and
   read_json take as they are. #}
{% macro archived_files(source, name, snapshots) -%}
    {%- set paths = [] -%}
    {%- for key in snapshots -%}
        {%- do paths.append(archive_root() ~ '/' ~ source ~ '/' ~ key ~ '/' ~ name) -%}
    {%- endfor -%}
    {{- return(paths) -}}
{%- endmacro %}

{# The snapshots a build of `model_name` reads, for a model that keeps one slice per snapshot of
   `source`: this build's, or every complete snapshot when the model's table is built from
   nothing, because it does not exist yet or the build is a --full-refresh. A rebuild then loses
   no snapshot the archive holds, as the archive is what raw is built from. #}
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
