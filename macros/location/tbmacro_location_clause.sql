{#-- Custom location_clause using custom config 'bucket_root' --#}
{#-- Call it in overloaded macro 'spark__location_clause' like example below --#}
{#-- It is mandatory to correct working materialization 'tbm_incremental' with config tbm_contract=true  --#}
{#
  {% macro spark__location_clause() %}
    {{ tbmacro.tbmacro_location_clause() }}
  {%- endmacro -%}
#}
{% macro tbmacro_location_clause() %}
  {{ "location "~"'"~tbmacro.tbmacro_location()~"'" if tbmacro.tbmacro_location() else '' }}
{%- endmacro -%}

{#-- Custom location path using custom config 'bucket_root' --#}
{% macro tbmacro_location_path() %}
  {%- set raw_file_format = config.get('file_format', default='delta') -%}
  {{ raw_file_format~'.`'~tbmacro.tbmacro_location()~'`' if tbmacro.tbmacro_location() else '' }}
{%- endmacro -%}

{#-- Make location path using default config 'location_root' and custom config 'bucket_root' --#}
{#-- Reruns error if bucket_root and location_root not defined --#}
{% macro tbmacro_location() %}
  {%- set location_root = config.get('location_root', validator=validation.any[basestring]) -%}
  {%- set identifier = model['alias'] -%}
  {%- set location = '' -%}
  {%- if location_root is not none %}
    {% set location -%}
    {{ location_root }}/{{ identifier }}
    {% endset %}
  {# begin overload #}
  {%- else %}
    {%- set bucket_root = config.get('bucket_root') -%}
    {%- if bucket_root is not none and 'default' in bucket_root and model.fqn[1] is not none %}
      {%- set stage_root = config.get('bucket_root')[model.fqn[2]] -%}
      {% set location -%}
      {{ stage_root |default(bucket_root.default) }}/{{ model.fqn[1] }}/{{ identifier }}
      {% endset %}
    {%- else %}
      {% do exceptions.raise_compiler_error('Invalid bucket_root or location_root. Please set bucket_root config in dbt_project.yml') %}
    {%- endif %}
  {# end overload #}
  {%- endif %}
  {{ return(location|trim()) }}
{%- endmacro -%}
