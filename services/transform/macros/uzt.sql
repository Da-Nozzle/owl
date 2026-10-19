{#- A street part of an Employment Service address, "Savanorių pr. 192" or "Kovo 11-osios g.":
    the street's name as an address writes it, its kind abbreviated (g., pr., al., ...) and the
    number. A seat's number may carry its premises after a dash (Gabijos g. 52-102). #}
{% macro uzt_street_pattern() -%}
    '^(.*\S)\s+(\p{Ll}+\.)(?:\s+(\S+))?$'
{%- endmacro %}

{% macro uzt_street_name(part) -%}
    nullif(regexp_extract({{ part }}, {{ uzt_street_pattern() }}, 1), '')
{%- endmacro %}

{% macro uzt_street_type(part) -%}
    nullif(regexp_extract({{ part }}, {{ uzt_street_pattern() }}, 2), '')
{%- endmacro %}

{#- The building's number as the Address Register writes it (12, 12A): a premises after a dash
    or slash is dropped. #}
{% macro uzt_house_number(part) -%}
    nullif(
        upper(regexp_extract(
            regexp_extract({{ part }}, {{ uzt_street_pattern() }}, 3), '^\d+[[:alpha:]]?'
        )),
        ''
    )
{%- endmacro %}
