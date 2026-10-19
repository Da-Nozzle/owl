{# The arrears list's published fields, with staging name and type. A reference is
   <property>.<key> in the CSV but {"<key>": ...} under <property> in a change; `field` is the
   change's key, which changed_fields lists. #}
{% macro vmi_tax_arrears_columns() %}
    {{- return([
        {'published': 'id', 'field': 'id', 'name': 'record_number', 'type': 'varchar'},
        {'published': 'sukurta', 'field': 'sukurta', 'name': 'record_created_on', 'type': 'date'},
        {'published': 'atnaujinta', 'field': 'atnaujinta', 'name': 'record_updated_on', 'type': 'date'},
        {'published': 'ja_pavadinimas', 'field': 'ja_pavadinimas', 'name': 'company_name', 'type': 'varchar'},
        {'published': 'sav_kodas.sav_kodas', 'field': 'sav_kodas', 'name': 'municipality_code', 'type': 'integer'},
        {'published': 'nepriemoka', 'field': 'nepriemoka', 'name': 'arrears_amount', 'type': 'decimal(18, 2)'},
        {'published': 'pradelsta_nepriemoka', 'field': 'pradelsta_nepriemoka', 'name': 'overdue_amount', 'type': 'decimal(18, 2)'},
        {'published': 'atideta_nepriemoka', 'field': 'atideta_nepriemoka', 'name': 'deferred_amount', 'type': 'decimal(18, 2)'},
        {'published': 'atideta_pradelsta', 'field': 'atideta_pradelsta', 'name': 'deferred_overdue_amount', 'type': 'decimal(18, 2)'},
    ]) -}}
{% endmacro %}
