{# Returns whether any row or value differs. #}
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

{# Returns whether any row differs; repeated rows count (except all). #}
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
