{#-- Check if relation exists in database --#}
{% macro tbmacro_check_relation(relation=this) %}
    {{ return(tbmacro.tbmacro_check_table(relation.identifier, relation.schema)) }}
{% endmacro %}


{#-- Check if schema.table exists in database --#}
{% macro tbmacro_check_table(model_name = model.name, schema_name = model.schema) %}
    {%- set result = false -%}

    {%- if execute -%}
        {% set query_check_schema -%}
        show schemas like '{{ schema_name }}'
        {%- endset %}
        {%- set check_schema = run_query(query_check_schema) -%}
        {%- set result_schema = true if check_schema else false -%}

        {%- if check_schema -%}
            {% set query_check_table -%}
            show tables from {{ schema_name }} like '{{ model_name }}'
            {%- endset %}
            {%- set check_table = run_query(query_check_table) -%}
            {%- set result = true if check_table else false -%}
        {%- endif -%}
    {%- endif -%}

    {{ return(result) }}
{% endmacro %}
