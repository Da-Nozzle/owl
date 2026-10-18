{# The register's published columns in export order, with staging name and type. A reference is
   <property>._id in the CSV but {"_id": ...} under <property> in a change; `flag` (1 or 0) is
   staged as a boolean. #}
{% macro vmi_register_columns() %}
    {{- return([
        {'published': 'id', 'name': 'record_number', 'type': 'varchar'},
        {'published': 'klnt_tipas', 'name': 'client_type', 'type': 'varchar'},
        {'published': 'ja_kodas', 'name': 'company_code', 'type': 'bigint'},
        {'published': 'pavadinimas', 'name': 'taxpayer_name', 'type': 'varchar'},
        {'published': 'ireg_data', 'name': 'registered_on', 'type': 'date'},
        {'published': 'isreg_data', 'name': 'deregistered_on', 'type': 'date'},
        {'published': 'anul_data', 'name': 'annulled_on', 'type': 'date'},
        {'published': 'valstybe', 'name': 'country_code', 'type': 'varchar'},
        {'published': 'tipo_kodas', 'name': 'taxpayer_type_code', 'type': 'integer'},
        {'published': 'tipo_aprasymas', 'name': 'taxpayer_type', 'type': 'varchar'},
        {'published': 'tipas_nuo', 'name': 'taxpayer_type_from', 'type': 'date'},
        {'published': 'tipas_iki', 'name': 'taxpayer_type_to', 'type': 'date'},
        {'published': 'anul_tipas', 'name': 'taxpayer_type_annulled_on', 'type': 'date'},
        {'published': 'pvm_kodas_pref', 'name': 'vat_code_prefix', 'type': 'varchar'},
        {'published': 'pvm_kodas', 'name': 'vat_code', 'type': 'varchar'},
        {'published': 'pvm_iregistruota', 'name': 'vat_registered_on', 'type': 'date'},
        {'published': 'pvm_isregistruota', 'name': 'vat_deregistered_on', 'type': 'date'},
        {'published': 'padalinio_nr', 'name': 'branch_number', 'type': 'integer'},
        {'published': 'padalinio_pvd', 'name': 'branch_name', 'type': 'varchar'},
        {'published': 'padalinio_savivaldybe', 'name': 'branch_municipality_code', 'type': 'integer'},
        {'published': 'padalinio_kodas', 'name': 'branch_country_code', 'type': 'integer'},
        {'published': 'suformuota', 'name': 'branch_formed_on', 'type': 'date'},
        {'published': 'isformuota', 'name': 'branch_dissolved_on', 'type': 'date'},
        {'published': 'ekonomine_veikla._id', 'name': 'activity_id', 'type': 'varchar'},
        {'published': 'veiklos_pradzia', 'name': 'activity_from', 'type': 'date'},
        {'published': 'veiklos_pabaiga', 'name': 'activity_to', 'type': 'date'},
        {'published': 'veikla_anuliuota', 'name': 'activity_annulled_on', 'type': 'date'},
        {'published': 'pagrindine', 'name': 'is_main_activity', 'type': 'flag'},
        {'published': 'vv_savivaldybe', 'name': 'activity_municipality_code', 'type': 'integer'},
        {'published': 'vv_adresas_nuo', 'name': 'activity_address_from', 'type': 'date'},
        {'published': 'vv_adresas_iki', 'name': 'activity_address_to', 'type': 'date'},
        {'published': 'vv_adresas_anul', 'name': 'activity_address_annulled_on', 'type': 'date'},
        {'published': 'mm_grupe', 'name': 'taxpayer_group_code', 'type': 'integer'},
        {'published': 'grup_aprasymas', 'name': 'taxpayer_group', 'type': 'varchar'},
        {'published': 'grupe_nuo', 'name': 'taxpayer_group_from', 'type': 'date'},
        {'published': 'grupe_iki', 'name': 'taxpayer_group_to', 'type': 'date'},
        {'published': 'grupe_anul', 'name': 'taxpayer_group_annulled_on', 'type': 'date'},
    ]) -}}
{% endmacro %}

{% macro vmi_typed(value, column) -%}
    {%- if column.type == 'flag' -%}
        ({{ value }})::integer = 1
    {%- else -%}
        ({{ value }})::{{ column.type }}
    {%- endif -%}
{%- endmacro %}
