{% materialization tbm_incremental, adapter='spark', supported_languages=['sql'] -%}
  {#-- Custom strategy 'tbm_incremental' --#}

  {#-- Validate config --#}
  {%- set tbm_config = tbmacro.tbmacro_validate_config() -%}
  {%- set file_format = tbm_config.file_format -%}
  {%- set strategy = tbm_config.strategy -%}

  {#-- Set vars --#}
  {%- set partition_by = tbm_config.partition_by -%}
  {%- set language = model['language'] -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set target_relation = this -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set tmp_relation = make_temp_relation(this) -%}
  {%- set tmp_insert_overwrite_relation = tbmacro.tbmacro_make_temp_insert_overwrite_relation(this) -%}

  {#-- for SQL model we will create temp view that doesn't have database and schema --#}
  {%- if language == 'sql'-%}
    {%- set tmp_relation = tmp_relation.include(database=false, schema=false) -%}
    {%- set tmp_insert_overwrite_relation = tmp_insert_overwrite_relation.include(database=false, schema=false) -%}
  {%- endif -%}

  {#-- Run pre-hooks --#}
  {{ run_hooks(pre_hooks) }}

  {%- if tbm_config.trap_for_bug == true -%}
    {#-- Check trap_for_bug --#}
    {{ tbmacro.tbmacro_trap_for_bug() }}
  {%- endif -%}

  {#-- Incremental run logic --#}
  {%- set is_existing_relation = true if not (existing_relation is none or existing_relation.is_view or should_full_refresh()) else false -%}

  {#-- Set Overwrite Mode --#}
  {%- if strategy == 'insert_overwrite' and partition_by and is_existing_relation == true -%}
    {%- call statement() -%}
      set spark.sql.sources.partitionOverwriteMode = DYNAMIC
    {%- endcall -%}
  {%- endif -%}

  {%- if is_existing_relation == false -%}
    {{ log("(tbm_incremental) Incremental relation not found or should be changed") }}

    {%- if tbm_config.tbm_contract == true -%}
      {#-- If tbm_contract == true create or restore+create table --#}
      {%- if existing_relation.is_view -%}
        {#-- Drop view --#}
        {% do adapter.drop_relation(existing_relation) %}
      {%- endif -%}
      {%- if (existing_relation.is_view or existing_relation is none) and location_clause() and location_clause() is not none -%}
        {#-- Get location path --#}
        {% set location_path = ((location_clause() | trim) | replace("location ", "")).strip("\'") %}
        {%- if tbmacro.tbmacro_get_existing_location(file_format, location_path) == true -%}
          {#-- Restore table because location exists --#}
          {%- call statement('tbmacro_restore_relation', language=language) -%}
            {{ tbmacro.tbmacro_restore_relation(target_relation, location_path) }}
          {%- endcall -%}
          {{ log("(tbm_incremental) Incremental relation was restored") }}
        {%- endif -%}
      {%- endif -%}
      {#-- Create or replace table --#}
      {%- call statement('create_relation', language=language) -%}
        {{ tbmacro.tbmacro_get_create_table(target_relation, tbm_config, should_full_refresh()) }}
      {%- endcall -%}
      {{ log("(tbm_incremental) Incremental relation was created or replaced") }}

    {%- else -%}
      {#-- Original behavior --#}
      {%- call statement('main', language=language) -%}
        {{ create_table_as(False, target_relation, compiled_code, language) }}
      {%- endcall -%}
      {% do persist_constraints(target_relation, model) %}
    {%- endif -%}
  {%- endif -%}

  {%- if is_existing_relation == true or tbm_config.tbm_contract == true -%}
    {{ log("(tbm_incremental) Incremental relation was found") }}
    {%- if tbm_config.tbm_contract == true -%}
      {#-- Check contract columns --#}
      {{ tbmacro.tbmacro_validate_contract_columns(target_relation, on_schema_change, model) }}
    {%- endif -%}
    {#-- Create temporary view --#}
    {%- call statement('create_tmp_relation', language=language) -%}
      {{ create_table_as(True, tmp_relation, tbmacro.tbmacro_cast_compiled_code(target_relation, compiled_code, tbm_config), language) }}
    {%- endcall -%}
    {#-- Set filters --#}
    {%- set alias = "DBT_INTERNAL_DEST." -%}
    {%- set filter_dict = tbmacro.tbmacro_filter_condition(tmp_relation, tbm_config, alias) -%}
    {%- set filter = filter_dict.default | default('') -%}
    {%- set filter_merge = filter_dict.alias | default('') -%}
    {{ log("(tbm_incremental) Got filters") }}
    {%- do process_schema_changes(on_schema_change, tmp_relation, existing_relation) -%}
    {%- if strategy == 'delete+insert' and existing_relation and tbm_config.mode is not none and filter and is_existing_relation == true -%}
      {#-- Delete if strategy == 'delete+insert' --#}
      {%- call statement('delete_from_relation') -%}
        {{ tbmacro.tbmacro_get_delete_from_sql(target_relation, filter) }}
      {%- endcall -%}
      {{ log("(tbm_incremental) Rows was deleted from "~target_relation) }}
    {%- endif -%}

    {%- if strategy == 'insert_overwrite' and is_existing_relation == true and tbm_config.mode is not none and filter and tbm_config.limit is none -%}
      {#-- Create secondary temporary view for union selection with out of range data if strategy == 'insert_overwrite' --#}
      {%- set filter_partition_by = tbmacro.tbmacro_filter_partition_by(tmp_relation, tbm_config) | default('') -%}
      {%- call statement('create_tmp_insert_overwrite_relation', language=language) -%}
        {{ tbmacro.tbmacro_create_insert_overwrite_as(tmp_relation, target_relation, tmp_insert_overwrite_relation, filter, filter_partition_by) }}
      {%- endcall -%}
      {{ log("(tbm_incremental) Selection unioned with out of range data for insert overwrite") }}
      {#-- Run incremental statement using secondary temporary view --#}
      {%- call statement('main') -%}
        {{ tbmacro.tbmacro_get_incremental_sql(tmp_insert_overwrite_relation, target_relation, existing_relation, tbm_config, filter, filter_merge) }}
      {%- endcall -%}

    {%- else -%}
      {#-- Run incremental statement using primary temporary view --#}
      {%- call statement('main') -%}
        {{ tbmacro.tbmacro_get_incremental_sql(tmp_relation, target_relation, existing_relation, tbm_config, filter, filter_merge) }}
      {%- endcall -%}
    {%- endif -%}
  {%- endif -%}

  {{ log("(tbm_incremental) Data was written into "~target_relation) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
