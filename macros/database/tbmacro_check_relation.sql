{% macro tbmacro_check_relation(relation=this) %}
    {#-- check existing relation in database --#}
    {{ return(tbmacro.tbmacro_check_table(relation.identifier, relation.schema)) }}
{% endmacro %}

{% macro tbmacro_check_table(model_name = model.name, schema_name = model.schema) %}
    {#-- check existing schema.model in database --#}
    {%- set result = false -%}

    {%- if execute -%}
        {#-- check schemas --#}
        {% set query_check_schema -%}
        show schemas like '{{ schema_name }}'
        {%- endset %}
        {%- set check_schema = run_query(query_check_schema) -%}
        {%- set result_schema = true if check_schema else false -%}

        {%- if check_schema -%}
            {#-- check tables --#}
            {% set query_check_table -%}
            show tables from {{ schema_name }} like '{{ model_name }}'
            {%- endset %}
            {%- set check_table = run_query(query_check_table) -%}
            {%- set result = true if check_table else false -%}
        {%- endif -%}
    {%- endif -%}

    {{ return(result) }}
{% endmacro %}
