{# A legal name as matched exactly: every kind of double quote (Lithuanian „ “ and the like) as ",
   and whitespace, non-breaking spaces included, as one space, trimmed. Nothing else changes, so
   UAB "Kava" and Kava, UAB stay two names. #}
{% macro normalized_name(name) -%}
    trim(regexp_replace(translate({{ name }}, '„“”″«»' || chr(160), '""""""' || ' '), '\s+', ' ', 'g'))
{%- endmacro %}
