{# Мета макросу: заміна кастомної схеми, згідно рекомендації:
https://docs.getdbt.com/docs/build/custom-schemas
#}

{#-- Custom schema from this: https://docs.getdbt.com/docs/build/custom-schemas --#}
{#-- Call it in overloaded macro 'generate_schema_name' like example below --#}
{#
  {% macro generate_schema_name(custom_schema_name, node) -%}
    {{ tbmacro.tbmacro_generate_schema_name(custom_schema_name, node) }}
  {%- endmacro -%}
#}

{% macro tbmacro_generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
