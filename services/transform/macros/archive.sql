{# A path in the raw archive: <archive>/<source>/<run month>/<name>, for this run's month unless
   another is given; a month of '*' is every archived run month, as a glob. #}
{% macro archived_file(source, name, month=none) -%}
    {{ env_var('LEADS_ARCHIVE_PATH').rstrip('/') }}/{{ source }}/{{ month or run_month() }}/{{ name }}
{%- endmacro %}

{# The archived run months a build of `model_name` reads, for a model that keeps one slice per
   run month: this run's month, or '*' (all of them) when the model's table is built from nothing,
   because it does not exist yet or the build is a --full-refresh. A rebuild then loses no run
   month the archive holds, as the archive is what raw is built from. #}
{% macro archived_run_months(model_name) -%}
    {%- if not execute -%}
        {{- return(run_month()) -}}
    {%- endif -%}
    {%- set node = graph.nodes.values()
        | selectattr('resource_type', 'equalto', 'model')
        | selectattr('name', 'equalto', model_name)
        | first -%}
    {%- set table = adapter.get_relation(node.database, node.schema, node.alias) -%}
    {{- return('*' if table is none or flags.FULL_REFRESH else run_month()) -}}
{%- endmacro %}
