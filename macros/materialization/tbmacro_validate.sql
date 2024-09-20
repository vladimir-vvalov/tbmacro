{% macro tbmacro_validate_config() -%}
  {#-- Get model configurations, check and set defaults --#}

  {#-- origin --#}
  {%- set raw_file_format = config.get('file_format', default='delta') -%}
  {%- set raw_strategy = config.get('incremental_strategy') or 'append' -%}
  {%- set raw_partition_by = config.get('partition_by', none) or none -%}
  {%- set raw_unique_key = config.get('unique_key', none) or none -%}
  {%- set raw_merge_update_columns = config.get('merge_update_columns') -%}
  {%- set raw_merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {%- set raw_incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) or none -%}

  {#-- tbm common --#}
  {%- set raw_tbm_contract = config.get('tbm_contract', default=false) or false -%}
  {%- set raw_tbm_cast_columns = config.get('tbm_cast_columns', false) or false -%}
  {%- set contract_description = config.get('tbm_contract_description', 'warn') or 'warn' -%}
  {%- set limit = config.get('limit', none) or none -%}
  {%- set trap_for_bug = config.get('tbm_trap_for_bug', true) or true -%}

  {#-- tmb filter --#}
  {%- set mode = config.get('tbm_filter_mode', none) or none -%}
  {%- set raw_key = config.get('tbm_filter_key', none) or none -%}
  {%- set from = config.get('tbm_filter_from', none) or none -%}
  {%- set till = config.get('tbm_filter_till', none) or none -%}
  {%- set quote_values = config.get('tbm_filter_quote_values') or true -%}

  {#-- tbm merge --#}
  {%- set update_changes_only = config.get('tbm_update_changes_only') or false -%}
  {%- set operator = config.get('tbm_merge_operator', none) or none -%}
  {%- set raw_include_check_columns = config.get('tbm_include_check_columns', none) or none -%}
  {%- set raw_exclude_check_columns = config.get('tbm_exclude_check_columns', none) or none -%}

  {#-- allowed lists --#}
  {%- set allow_modes = ['values', 'range', 'ranged_values'] -%}
  {%- set allow_contract_description = ['ignore', 'warn', 'error'] -%}
  {%- set allow_operator = ['delete'] -%}
  {#-- error messages --#}
  {% set invalid_contract_description_msg -%}
    tbm_incremental error
    Invalid tbm_contract_description: '{{ contract_description }}'
    Set one of this: none or one of {{ allow_contract_description }}
  {%- endset %}
  {% set invalid_filter_mode_msg -%}
    tbm_incremental error
    Invalid tbm_filter_mode: '{{ mode }}'
    Set one of this: none or one of {{ allow_modes }}
  {%- endset %}
  {% set invalid_delete_insert_msg -%}
    tbm_incremental error
    Strategy 'delete+insert' dont allow if 'tbm_filter_mode' is none
  {%- endset %}
  {% set invalid_key_msg -%}
    tbm_incremental error
    'tbm_filter_key' not found for 'tbm_filter_mode' = '{{ mode }}'
  {%- endset %}
  {% set invalid_merge_operator_msg -%}
    tbm_incremental error
    Invalid merge operator: {{ operator }}
    Set one of this: none or one of {{ allow_operator }}
  {%- endset %}
  {% set invalid_merge_check_columns_msg -%}
    tbm_incremental error
    Model cannot specify 'include_check_columns' and 'exclude_check_columns'. Please update model to use only one config
  {%- endset %}
  {% set invalid_merge_condition_msg -%}
    tbm_incremental error
    Not defined merge conditions
    Set one or more configs of: 'unique_key', 'incremental_predicates', 'tbm_filter_key'
  {%- endset %}

  {#-- processing some raw parametes --#}
  {%- set key = [] -%}
  {%- if not(raw_key is not none and mode is none) -%}
    {%- if raw_key is string -%}
      {% do key.append(raw_key) %}
    {%- else -%}
      {% set key = raw_key %}
    {%- endif -%}
  {%- endif -%}

  {%- set unique_key = [] -%}
  {%- if raw_unique_key is string -%}
    {% do unique_key.append(raw_unique_key) %}
  {%- else -%}
    {% set unique_key = raw_unique_key %}
  {%- endif -%}

  {%- set merge_update_columns = [] -%}
  {%- if raw_merge_update_columns is string -%}
    {% do merge_update_columns.append(raw_merge_update_columns) %}
  {%- else -%}
    {% set merge_update_columns = raw_merge_update_columns %}
  {%- endif -%}

  {%- set merge_exclude_columns = [] -%}
  {%- if raw_merge_exclude_columns is string -%}
    {% do merge_exclude_columns.append(raw_merge_exclude_columns) %}
  {%- else -%}
    {% set merge_exclude_columns = raw_merge_exclude_columns %}
  {%- endif -%}

  {%- set include_check_columns = [] -%}
  {%- if not(raw_include_check_columns is not none and update_changes_only is none) -%}
    {%- if raw_include_check_columns is string -%}
      {% do include_check_columns.append(raw_include_check_columns) %}
    {%- else -%}
      {% set include_check_columns = raw_include_check_columns %}
    {%- endif -%}
  {%- endif -%}

  {%- set exclude_check_columns = [] -%}
  {%- if not(exclude_check_columns is not none and update_changes_only is none) -%}
    {%- if raw_exclude_check_columns is string -%}
      {% do exclude_check_columns.append(raw_exclude_check_columns) %}
    {%- else -%}
      {% set exclude_check_columns = raw_exclude_check_columns %}
    {%- endif -%}
  {%- endif -%}

  {%- set partition_by = [] -%}
  {%- if raw_partition_by is string -%}
    {% do partition_by.append(raw_partition_by) %}
  {%- else -%}
    {% set partition_by = raw_partition_by %}
  {%- endif -%}

  {%- set incremental_predicates = [] -%}
  {%- if raw_incremental_predicates is string -%}
    {% do incremental_predicates.append(raw_incremental_predicates) %}
  {%- else -%}
    {% set incremental_predicates = raw_incremental_predicates %}
  {%- endif -%}

  {#-- validate --#}
  {%- set file_format = tbmacro.tbmacro_validate_file_format(raw_file_format) -%}
  {%- set strategy = tbmacro.tbmacro_validate_strategy(raw_strategy, file_format) -%}

  {%- if contract_description not in allow_contract_description and contract_description is not none -%}
    {% do exceptions.raise_compiler_error(invalid_contract_description_msg) %}
  {%- endif -%}

  {%- set tbm_contract = tbmacro.tbmacro_validate_contract(raw_tbm_contract, contract_description, model) -%}
  {%- set cast_columns = raw_tbm_cast_columns if tbm_contract==true else false -%}

  {%- if mode not in allow_modes and mode is not none -%}
    {% do exceptions.raise_compiler_error(invalid_filter_mode_msg) %}
  {%- endif -%}

  {%- if mode is not none and not key -%}
    {% do exceptions.raise_compiler_error(invalid_key_msg) %}
  {%- endif -%}

  {%- if strategy == 'delete+insert' and mode is none -%}
    {% do exceptions.raise_compiler_error(invalid_delete_insert_msg) %}
  {%- endif -%}

  {%- if operator is not none and operator not in allow_operator -%}
    {% do exceptions.raise_compiler_error(invalid_merge_operator_msg) %}
  {%- endif -%}

  {%- if include_check_columns and exclude_check_columns -%}
    {{ exceptions.raise_compiler_error(invalid_merge_check_columns_msg)}}
  {%- endif -%}

  {%- if strategy == 'merge' and (unique_key is none and incremental_predicates is none and key is none) -%}
    {{ exceptions.raise_compiler_error(invalid_merge_condition_msg)}}
  {%- endif -%}

  {#-- make result dictionary --#}
  {%- set return_dict = {
    'file_format': file_format,
    'strategy': strategy,
    'partition_by': partition_by,
    'unique_key': unique_key,
    'merge_update_columns': merge_update_columns,
    'merge_exclude_columns': merge_exclude_columns,
    'incremental_predicates': incremental_predicates,

    'tbm_contract': tbm_contract,
    'cast_columns': cast_columns,
    'contract_description': contract_description,
    'limit': limit,
    'trap_for_bug': trap_for_bug,

    'mode': mode,
    'key': key,
    'from': from,
    'till': till,
    'quote_values': quote_values,

    'update_changes_only': update_changes_only,
    'operator': operator,
    'include_check_columns': include_check_columns,
    'exclude_check_columns': exclude_check_columns,
   } -%}

  {{ return(return_dict) }}

{%- endmacro %}


{% macro tbmacro_validate_file_format(raw_file_format) -%}
  {#-- Validate the file format --#}

  {%- set accepted_formats = ['delta'] -%}

  {% set invalid_file_format_msg -%}
    tbm_incremental error
    Invalid file format provided: {{ raw_file_format }}
    Expected one of: {{ accepted_formats | join(', ') }}
  {%- endset %}

  {% if raw_file_format not in accepted_formats %}
    {% do exceptions.raise_compiler_error(invalid_file_format_msg) %}
  {% endif %}

  {% do return(raw_file_format) %}
{%- endmacro %}


{% macro tbmacro_validate_strategy(raw_strategy, file_format) -%}
  {#-- Validate the incremental strategy --#}

  {%- set accepted_strategies = ['append', 'merge', 'insert_overwrite', 'delete+insert'] -%}

  {% set invalid_strategy_msg -%}
    tbm_incremental error
    Invalid incremental strategy provided: {{ raw_strategy }}
    Expected one of: {{ accepted_strategies }}
  {%- endset %}

  {% if raw_strategy not in accepted_strategies %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(raw_strategy) %}
{%- endmacro %}


{% macro tbmacro_validate_contract(raw_tbm_contract, contract_description, model = {}) -%}
  {#-- Validate tbm_contract --#}
  {%- if not raw_tbm_contract -%}
    {% do return(raw_tbm_contract) %}
  {%- endif -%}

  {% set no_yml_msg -%}
    tbm_contract error
    '{{ model.name }}' YML not found
    Create YML and describe name, description and data_type for each column
    Describe constraints if you need
  {%- endset %}

  {% set no_model_description_msg -%}
    tbm_contract error
    Description for model '{{ model.name }}' YML not found
    Describe description for model
  {%- endset %}

  {% set invalid_description_msg -%}
    tbm_contract error
    When tbm_contract = true, all columns must have description
    description not found for{{' '}} 
  {%- endset %}

  {% set invalid_data_type_msg -%}
    tbm_contract error
    When tbm_contract = true, all columns must have datatype
    data_type not found for column: 
  {%- endset %}

  {% if not model.columns %}
    {% do exceptions.raise_compiler_error(no_yml_msg) %}
  {% endif %}

  {% if (model.description is none or not model.description) and contract_description in ['warn', 'error'] %}
    {% if contract_description == 'error' %}
      {% do exceptions.raise_compiler_error(no_model_description_msg) %}
    {% else %}
      {% do exceptions.warn("Warning: "~no_model_description_msg) %}
    {% endif %}
  {% endif %}

  {%- set warn_count = [] -%}
  {%- for key, value in model.columns.items() %}
    {% if (value.description is none or not value.description) and contract_description in ['warn', 'error'] %}
      {% if contract_description == 'error' %}
        {% do exceptions.raise_compiler_error(invalid_description_msg~key) %}
      {% else %}
        {% do warn_count.append(key) %}
      {% endif %}
    {% endif %}
    {% if value.data_type is none or not value.data_type  %}
      {% do exceptions.raise_compiler_error(invalid_data_type_msg~key) %}
    {% endif %}
  {%- endfor %}

  {%- if warn_count | count() > 0 -%}
    {% do exceptions.warn("Warning: "~invalid_description_msg~warn_count) %}
  {%- endif -%}

  {{ log("(tbm_incremental) Contract validation successfull") }}

  {% do return(raw_tbm_contract) %}
{%- endmacro %}


{% macro tbmacro_validate_contract_columns(target, on_schema_change, model = {}) -%}
  {#-- Validate tbm_contract columns --#}
  {%- if on_schema_change not in ['fail', 'ignore'] and on_schema_change -%}
    {{ return(false) }}
  {%- endif -%}

  {%- set raw_dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- set dest_columns = [] -%}
  {%- for item in raw_dest_columns -%}
    {%- do dest_columns.append(item.column) -%}
  {%- endfor -%}

  {#-- Validate table columns in the YML --#}
  {%- for item in dest_columns -%}
    {%- if item not in model.columns.keys() -%}
      {% set invalid_yml -%}
      tbm_contract error
      Column {{ item.column }} was not described in YML
      {%- endset -%}
      {% do exceptions.raise_compiler_error(invalid_yml) %}
    {%- endif -%}
  {%- endfor -%}

  {#-- Validate YML columns in the table --#}
  {%- for item in model.columns.keys() -%}
    {%- if item not in dest_columns -%}
      {% set invalid_yml -%}
      tbm_contract error
      Column {{ item.column }} does not exist in table
      {%- endset -%}
      {% do exceptions.raise_compiler_error(invalid_yml) %}
    {%- endif -%}
  {%- endfor -%}
  {%- for item in raw_dest_columns -%}
    {%- if item.data_type != model.columns[item.column].data_type -%}
      {% set invalid_yml -%}
      tbm_contract error
      The data type of the column {{ item.column }} in the table does not match to YML
      Data_type in the table: {{ item.data_type }}
      Data_type in the YML: {{ model.columns[item.column].data_type }}
      {%- endset -%}
      {% do exceptions.raise_compiler_error(invalid_yml) %}
    {%- endif -%}
  {%- endfor -%}

  {{ log("(tbm_incremental) Contract columns validation successfull") }}
  {{ return(true) }}

{%- endmacro %}


{% macro tbmacro_validate_value_list(values_list=[]) -%}
  {#-- Validate values list that has got from values in columns defined in 'tbm_filter_key' --#}

  {% set invalid_value -%}
    tbm_incremental error
    Null values not allowed in columns defined in 'tbm_filter_key'
  {%- endset %}

  {%- for item in values_list -%}
    {% if not item %}
      {{ print(item) }}
      {% do exceptions.raise_compiler_error(invalid_value) %}
    {% endif %}
  {%- endfor -%}

  {% do return(values_list) %}
{%- endmacro %}
