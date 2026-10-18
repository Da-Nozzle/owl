{# owl-dbt-diff: each model built on the dev target against the lake's table (attached as `lake`).
   Rows are matched on the model's meta `key` when it has one, else compared whole. #}
{% macro diff(models, lake_database='lake', examples=5) %}
    {%- for model in models -%}
        {%- do diff_model(model, lake_database, examples) -%}
    {%- endfor -%}
{% endmacro %}

{# `model` is a name or a unique id (model.<project>.<name>[.v<version>]). #}
{% macro diff_model(model, lake_database, examples) %}
    {%- set nodes = [graph.nodes[model]] if model in graph.nodes else graph.nodes.values()
        | selectattr('resource_type', 'equalto', 'model')
        | selectattr('name', 'equalto', model)
        | list -%}
    {%- if not nodes -%}
        {%- do print(model ~ ': no such model') -%}
        {{- return(none) -}}
    {%- endif -%}
    {%- set node = nodes[0] -%}
    {%- set name = node.name ~ ('.v' ~ node.version if node.version is not none else '') -%}
    {%- if node.config.materialized == 'ephemeral' -%}
        {%- do print(name ~ ': ephemeral, so it has no table to compare') -%}
        {{- return(none) -}}
    {%- endif -%}
    {%- set dev = adapter.get_relation(target.database, node.schema, node.alias) -%}
    {%- set lake = adapter.get_relation(lake_database, node.schema, node.alias) -%}
    {%- if dev is none -%}
        {%- do print(name ~ ': not built on the dev target; build it with owl-dbt-dev') -%}
        {{- return(none) -}}
    {%- endif -%}
    {%- if lake is none -%}
        {%- set rows = run_query('select count(*) from ' ~ dev).columns[0].values()[0] -%}
        {%- do print(name ~ ': new, not in the lake yet; ' ~ diff_count(rows) ~ ' rows in dev') -%}
        {{- return(none) -}}
    {%- endif -%}

    {%- set dev_types = diff_column_types(dev) -%}
    {%- set lake_types = diff_column_types(lake) -%}
    {%- set added, dropped, retyped, common = [], [], [], [] -%}
    {%- for column, data_type in dev_types.items() -%}
        {%- if column not in lake_types -%}
            {%- do added.append(column ~ ' ' ~ data_type) -%}
        {%- else -%}
            {%- do common.append(column) -%}
            {%- if lake_types[column] != data_type -%}
                {%- do retyped.append(column ~ ' ' ~ lake_types[column] ~ ' -> ' ~ data_type) -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
    {%- for column, data_type in lake_types.items() if column not in dev_types -%}
        {%- do dropped.append(column ~ ' ' ~ data_type) -%}
    {%- endfor -%}

    {%- set meta = node.config.get('meta', {}) or node.get('meta', {}) -%}
    {%- set key = meta.get('key') or [] -%}
    {%- set missing_key = [] -%}
    {%- for column in key if column not in common -%}
        {%- do missing_key.append(column) -%}
    {%- endfor -%}

    {%- do print('') -%}
    {%- do print(name ~ ': ' ~ dev ~ ' (dev) against ' ~ lake ~ ' (lake)'
        ~ (', matched on ' ~ key | join(', ') if key and not missing_key else ', whole rows')) -%}
    {%- do print('  columns added:   ' ~ (added | join(', ') or 'none')) -%}
    {%- do print('  columns dropped: ' ~ (dropped | join(', ') or 'none')) -%}
    {%- do print('  columns retyped: ' ~ (retyped | join(', ') or 'none')) -%}
    {%- if missing_key -%}
        {%- do print('  key ' ~ missing_key | join(', ') ~ ' is not in both tables') -%}
    {%- endif -%}

    {%- if key and not missing_key -%}
        {%- set differences = diff_matched_rows(dev, lake, key, common, examples) -%}
    {%- else -%}
        {%- set differences = diff_whole_rows(dev, lake, common) -%}
    {%- endif -%}
    {%- set changed = dropped or retyped or differences -%}
    {%- do print('  => ' ~ ('CHANGED: look at the above' if changed
        else 'only columns added' if added else 'unchanged')) -%}
{% endmacro %}

{% macro diff_column_types(relation) %}
    {%- set types = {} -%}
    {%- for column in adapter.get_columns_in_relation(relation) -%}
        {%- do types.update({column.name: column.data_type}) -%}
    {%- endfor -%}
    {{- return(types) -}}
{% endmacro %}

{% macro diff_count(value) %}
    {{- return('{:,}'.format(value | int)) -}}
{% endmacro %}
