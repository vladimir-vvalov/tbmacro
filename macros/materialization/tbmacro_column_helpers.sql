{#-- Build WHERE condition based on tbm_filter_* configuration --#}
{% macro tbmacro_filter_condition(relation, tbm_config, alias) -%}
{%- set key = tbmacro.tbmacro_surrogate_key(tbm_config.key, alias) -%}
{%- set sql = "" -%}
{%- set values_list = [] -%}
{%- if tbm_config.mode is none or not (key is not none and key) -%}
    {%- set sql = "" -%}
  {%- else -%}
{%- if tbm_config.mode == 'range' -%}
      {%- set from = "and "~key~" >= "~tbm_config.from if tbm_config.from and tbm_config.from is not none else "" -%}
      {%- set till = "and "~key~" <= "~tbm_config.till if tbm_config.till and tbm_config.till is not none else "" -%}
      {%- set sql = from~" "~till -%}
    {%- else -%}
      {%- set values_list = tbmacro.tbmacro_values_filter_key(relation, tbm_config.key) -%}
      {%- if values_list is not none and values_list -%}
        {%- if tbm_config.mode == 'values' -%}
          {%- set sql -%}
          and {{ key }} in (
            {%- for item in values_list -%}
            {{ tbmacro.tbmacro_quote(item, tbm_config.quote_values) }}{{ "," if not loop.last }}
            {%- endfor -%}
          )
          {%- endset -%}
        {%- else -%}
          {%- set from = "and "~key~" >= "~(tbm_config.from if tbm_config.from and tbm_config.from is not none else tbmacro.tbmacro_quote(values_list|first(), tbm_config.quote_values)) -%}
          {%- set till = "and "~key~" <= "~(tbm_config.till if tbm_config.till and tbm_config.till is not none else tbmacro.tbmacro_quote(values_list|last(), tbm_config.quote_values)) -%}
          {%- set sql = from~" "~till -%}
        {%- endif -%}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}

{%- set return_dict = {} -%}
{{ return_dict.update({"alias": sql}) }}
{{ return_dict.update({"default": sql | replace(alias, '')}) }}

{{ return(return_dict) }}
{%- endmacro %}


{#-- Build WHERE condition for partition_by columns based on tbm_filter_* configuration --#}
{% macro tbmacro_filter_partition_by(relation, tbm_config) -%}
{%- set key = tbmacro.tbmacro_surrogate_key(tbm_config.partition_by) -%}
{%- set sql = '' -%}
{%- set is_empty = false -%}
{%- if key is not none and key -%}
{%- set values_list = tbmacro.tbmacro_values_filter_key(relation, tbm_config.partition_by) -%}
{%- if values_list is none or not values_list -%}
      {%- set is_empty = true -%}
    {%- else -%}
    {%- set sql -%}
    and {{ key }} in (
      {%- for item in values_list -%}
      {{ tbmacro.tbmacro_quote(item, tbm_config.quote_values) }}{{ "," if not loop.last }}
      {%- endfor -%}
    )
    {%- endset -%}
    {%- endif -%}
  {%- endif -%}
{%- set return_dict = {"sql": sql, "is_empty": is_empty} -%}
{{ return(return_dict) }}
{%- endmacro %}


{#-- Extract distinct values from relation for given key columns --#}
{% macro tbmacro_values_filter_key(relation, key=[]) -%}
{%- set values_list = [] -%}
{%- if not (not relation or not key) -%}
    {%- if execute -%}
      {% set sql -%}
      select distinct
        {{ tbmacro.tbmacro_surrogate_key(key) }} as {{ tbmacro.tbmacro_surrogate_key_alias(key) }}
      from {{ relation }}
      {%- endset %}
      {%- set result = run_query(sql) -%}
      {%- for item in result.columns[0] -%}
        {%- do values_list.append(item) -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}
{%- set values_list_checked = tbmacro.tbmacro_validate_value_list(values_list) -%}
{%- if values_list_checked | count() > 0 -%}
    {{ return(values_list_checked|sort()) }}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}
{%- endmacro %}


{#-- Generate surrogate key expression from key columns (single or concatenated) --#}
{% macro tbmacro_surrogate_key(key=[], alias='') -%}
{%- set quoted_list = [] -%}
{%- set alias = '' if alias is none else alias -%}
{%- set return_value = '' -%}
{%- if key -%}
{%- for item in key -%}
{%- do quoted_list.append(alias~'`'~item~'`') -%}
{%- endfor -%}
{%- set return_value=quoted_list[0] if quoted_list | count() == 1 else quoted_list | join(" || '|' || ") -%}
{%- endif -%}
{{ return(return_value) }}
{%- endmacro %}


{#-- Generate alias name for surrogate key column --#}
{% macro tbmacro_surrogate_key_alias(key=[]) -%}
{% set return_value = '' %}
{%- if key and key is not none -%}
{% set return_value = '_dbt__tbmacro_key_'~(key[0] if key | count() == 1 else key | join("_")) %}
{%- endif -%}
{{ return('`'~return_value~'`') }}
{%- endmacro %}


{#-- Quote value as string literal if quote_values is true --#}
{% macro tbmacro_quote(value, quote_values = true) -%}
{%- set return_value = "'"~value~"'" if quote_values == true else value -%}
{{ return(return_value) }}
{%- endmacro %}


{#-- Get columns to update in MERGE statement (respects include/exclude configuration) --#}
{% macro tbmacro_get_merge_update_columns(tbm_config, dest_columns) -%}
{%- set default_cols = dest_columns | map(attribute="quoted") | list -%}
{%- set include_check_columns = tbm_config.include_check_columns -%}
{%- set exclude_check_columns = tbm_config.exclude_check_columns -%}
{%- set unique_key = tbm_config.unique_key -%}

{%- set update_columns = [] -%}
{%- if include_check_columns -%}
{%- for column in dest_columns -%}
{% if (column.column | lower in include_check_columns) or (column.column | lower in unique_key) %}
        {%- do update_columns.append(column.quoted) -%}
      {% endif %}
    {%- endfor -%}
  {%- elif exclude_check_columns -%}
{%- for column in dest_columns -%}
{% if (column.column | lower not in exclude_check_columns) %}
        {%- do update_columns.append(column.quoted) -%}
      {% endif %}
    {%- endfor -%}
  {%- else -%}
{%- set update_columns = default_cols -%}
{%- endif -%}

{{ return(update_columns) }}

{%- endmacro %}
