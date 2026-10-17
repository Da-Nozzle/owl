{# What a change did to a model's output, for leads-dbt-diff: each model as the dev target built it
   (leads-dbt-dev) against the lake's table, which the dev target attaches read-only as `lake`.
   It reports rows found on one side only and columns added, dropped or retyped. A model whose
   meta names its `key` is matched row by row on it, and each column's changed values are
   counted, with a few examples; without a key, whole rows are compared. A change should show
   only what it meant to change: an added column is the column and nothing else.

   leads-dbt-dev run-operation diff --args '{models: [stg_sodra__monthly]}' runs it by hand. #}
{% macro diff(models, lake_database='lake', examples=5) %}
    {%- for model in models -%}
        {%- do diff_model(model, lake_database, examples) -%}
    {%- endfor -%}
{% endmacro %}

{# A model given by name or unique id (model.<project>.<name>[.v<version>]). #}
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
        {%- do print(name ~ ': not built on the dev target; build it with leads-dbt-dev') -%}
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

{# Rows only on one side by key, then each common column's changed values in the matched rows.
   Returns whether anything differs. #}
{% macro diff_matched_rows(dev, lake, key, common, examples) %}
    {%- set on = [] -%}
    {%- for column in key -%}
        {%- do on.append('lake.' ~ adapter.quote(column) ~ ' is not distinct from dev.'
            ~ adapter.quote(column)) -%}
    {%- endfor -%}
    {%- set on = on | join(' and ') -%}
    {%- set key_columns = [] -%}
    {%- for column in key -%}
        {%- do key_columns.append(adapter.quote(column)) -%}
    {%- endfor -%}
    {%- set key_columns = key_columns | join(', ') -%}
    {%- set counts -%}
        select
            (select count(*) from {{ lake }}),
            (select count(*) from {{ dev }}),
            (select count(*) from {{ lake }} as lake anti join {{ dev }} as dev on {{ on }}),
            (select count(*) from {{ dev }} as dev anti join {{ lake }} as lake on {{ on }}),
            (select count(*) from (
                select 1 from {{ lake }} group by {{ key_columns }} having count(*) > 1)),
            (select count(*) from (
                select 1 from {{ dev }} group by {{ key_columns }} having count(*) > 1))
    {%- endset -%}
    {%- set row = run_query(counts).rows[0] -%}
    {%- do print('  rows: ' ~ diff_count(row[0]) ~ ' in the lake, ' ~ diff_count(row[1])
        ~ ' in dev; ' ~ diff_count(row[2]) ~ ' only in the lake, ' ~ diff_count(row[3])
        ~ ' only in dev') -%}
    {%- if row[4] or row[5] -%}
        {%- do print('  the key is not unique: ' ~ diff_count(row[4]) ~ ' repeated keys in the lake, '
            ~ diff_count(row[5]) ~ ' in dev, so values are not compared') -%}
        {{- return(true) -}}
    {%- endif -%}

    {%- set compared = [] -%}
    {%- for column in common if column not in key -%}
        {%- do compared.append(column) -%}
    {%- endfor -%}
    {%- set changed = [] -%}
    {%- if compared -%}
        {%- set changes -%}
            select
            {%- for column in compared %}
                count(*) filter (
                    where lake.{{ adapter.quote(column) }}::varchar
                        is distinct from dev.{{ adapter.quote(column) }}::varchar
                ){{ ',' if not loop.last }}
            {%- endfor %}
            from {{ lake }} as lake
            join {{ dev }} as dev on {{ on }}
        {%- endset -%}
        {%- set counted = run_query(changes).rows[0] -%}
        {%- for column in compared -%}
            {%- if counted[loop.index0] -%}
                {%- do changed.append((column, counted[loop.index0])) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- if not changed -%}
        {%- do print('  values changed in matched rows: none') -%}
    {%- else -%}
        {%- do print('  values changed in matched rows:') -%}
    {%- endif -%}
    {%- for column, rows in changed -%}
        {%- do print('    ' ~ column ~ ': ' ~ diff_count(rows) ~ ' rows, for example') -%}
        {%- set example -%}
            select
                {% for column in key %}lake.{{ adapter.quote(column) }}, {% endfor %}
                lake.{{ adapter.quote(column) }}::varchar as lake,
                dev.{{ adapter.quote(column) }}::varchar as dev
            from {{ lake }} as lake
            join {{ dev }} as dev on {{ on }}
            where lake.{{ adapter.quote(column) }}::varchar
                is distinct from dev.{{ adapter.quote(column) }}::varchar
            order by {% for column in key %}lake.{{ adapter.quote(column) }}{{ ', ' if not loop.last }}{% endfor %}
            limit {{ examples }}
        {%- endset -%}
        {%- do run_query(example).print_table(max_columns=10, max_column_width=40) -%}
    {%- endfor -%}
    {{- return(row[2] or row[3] or changed) -}}
{% endmacro %}

{# Without a key: the rows of the common columns on one side only, counting repeats. Returns
   whether any differ. #}
{% macro diff_whole_rows(dev, lake, common) %}
    {%- set columns = [] -%}
    {%- for column in common -%}
        {%- do columns.append(adapter.quote(column)) -%}
    {%- endfor -%}
    {%- set columns = columns | join(', ') -%}
    {%- set counts -%}
        select
            (select count(*) from {{ lake }}),
            (select count(*) from {{ dev }}),
            (select count(*) from (
                select {{ columns }} from {{ lake }} except all select {{ columns }} from {{ dev }})),
            (select count(*) from (
                select {{ columns }} from {{ dev }} except all select {{ columns }} from {{ lake }}))
    {%- endset -%}
    {%- set row = run_query(counts).rows[0] -%}
    {%- do print('  rows: ' ~ diff_count(row[0]) ~ ' in the lake, ' ~ diff_count(row[1])
        ~ ' in dev; ' ~ diff_count(row[2]) ~ ' only in the lake, ' ~ diff_count(row[3])
        ~ ' only in dev (comparing the columns in both)') -%}
    {{- return(row[2] or row[3]) -}}
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
