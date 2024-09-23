{% macro tbmacro_get_create_table(relation, tbm_config, should_full_refresh) -%}
  {{ adapter.dispatch('tbmacro_get_create_table', 'tbmacro')(relation, tbm_config, should_full_refresh) }}
{%- endmacro %}

{% macro spark__tbmacro_get_create_table(relation, tbm_config, should_full_refresh) -%}
  {#-- Get sql code with create or repclace table using tbm_contract=true --#}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set sql -%}
  {{ sql_header if sql_header is not none }}
  create or replace table {{ relation }}
  (
    {% for key, value in model.columns.items() -%}
    `{{ key|trim() }}` {{ value.data_type|trim() }} {{ 'not null' if value.constraints and value.constraints[0]['type'] == 'not_null' else '' }}
      {{ tbmacro.tbmacro_column_comment_clause(tbm_config, value.description) }}{{ ", " if not loop.last }}
    {% endfor %}
  )
  {{ file_format_clause() }}
  {{ options_clause() }}
  {{ tblproperties_clause() }}
  {{ partition_cols(label="partitioned by") }}
  {{ clustered_cols(label="clustered by") }}
  {{ location_clause() }}
  {{ tbmacro.tbmacro_comment_clause() }}
  {%- endset -%}
  {{ return(sql) }}
{% endmacro %}


{% macro tbmacro_comment_clause() -%}
  {#-- Get comment for table description --#}
  {%- if model.description and model.description is not none -%}
  comment '{{ (model.description | trim()) | replace("'", "\\'") }}'
  {%- else -%}
    {{' '}}
  {%- endif -%}
{%- endmacro %}


{% macro tbmacro_column_comment_clause(tbm_config, description) -%}
  {#-- Get comment for column description --#}
  {% if description and description is not none -%}
    comment '{{ (description|trim()) | replace("'", "\\'") }}'
  {%- else -%}
    {{' '}}
  {%- endif %}
{%- endmacro %}


{% macro tbmacro_restore_relation(relation, location) -%}
  {#-- Get sql code with restore table then it not exists in hive but exists in minio --#}
  {%- if not relation or not location -%}
    {{ exceptions.warn("Warning: Not exists relation: "~relation~" or location: "~location) }}
  {%- else -%}
    create table {{ relation }}
    {{ file_format_clause() }}
    {{ location_clause() }}
  {%- endif -%}
{%- endmacro %}


{% macro tbmacro_get_existing_location(file_format, location) -%}
  {#-- Check delta table by location in minio --#}
  {%- if execute -%}
    {%- set query = 'explain desc formatted '~file_format~'.`'~location~'`' -%}
    {%- set result = run_query(query) -%}
    {%- if 'DELTA_MISSING_DELTA_TABLE' in result.columns[0][0] -%}
      {%- set return_value = false -%}
    {%- else -%}
      {%- set return_value = true -%}
    {%- endif -%}
  {%- endif -%}
  {{ return(return_value) }}
{%- endmacro -%}


{% macro tbmacro_cast_compiled_code(target_relation, compiled_code, tbm_config) -%}
  {#-- Enrichment compiled code with casting columns and limit --#}
  {%- set cast_columns = tbm_config.cast_columns -%}
  {%- set limit = tbm_config.limit -%}
  {%- if cast_columns == true -%}
    {% set return_compiled_code -%}
    with _dbt__tbmacro_tmp_cast_{{ target_relation.identifier }} as (
      {{ compiled_code }}
    )
    select
      {% for key, value in model.columns.items() -%}
      cast(`{{ key|trim() }}` as {{ value.data_type|trim() }}) as `{{ key|trim() }}`{{ ", " if not loop.last }}
      {% endfor %}
    from _dbt__tbmacro_tmp_cast_{{ target_relation.identifier }}
    {%- endset %}
  {%- else -%}
    {%- set return_compiled_code = compiled_code -%}
  {%- endif -%}
  {{ return(tbmacro.tbmacro_limit_compiled_code(target_relation, return_compiled_code, limit)) }}
{%- endmacro %}


{% macro tbmacro_limit_compiled_code(target_relation, compiled_code, limit) -%}
  {#-- Enrichment compiled code with limit --#}
  {%- if limit is not none -%}
    {%- set return_compiled_code -%}
    with _dbt__tmp_limit_{{ target_relation.identifier }} as (
      {{ compiled_code }}
    )
    select *
    from _dbt__tmp_limit_{{ target_relation.identifier }}
    limit {{ limit }}
    {%- endset -%}
  {%- else -%}
    {%- set return_compiled_code = compiled_code -%}
  {%- endif -%}
  {{ return(return_compiled_code) }}
{%- endmacro %}


{% macro tbmacro_create_insert_overwrite_as(tmp_relation, target_relation, tmp_insert_overwrite_relation, filter, filter_partition_by) -%}
  {#-- Get sql code with union model selection and other partitions part according to tbm_filter_* configurations --#}
  {%- set empty_columns = [] -%}
  {%- set empty_columns_csv = '' -%}
  {%- set dest_columns = adapter.get_columns_in_relation(tmp_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

  {%- if not filter -%}
    {#-- create empty query --#}
    {%- for item in dest_columns -%}
      {%- do empty_columns.append('null as '~item.quoted) -%}
    {%- endfor -%}
    {%- set empty_columns_csv = empty_columns | join(', ') -%}
  {%- endif -%}
  create or replace temporary view {{ tmp_insert_overwrite_relation }} as (
    {% if filter -%}
    select {{ dest_cols_csv }}
    from {{ tmp_relation }}
    union all
    select {{ dest_cols_csv }}
    from {{ target_relation }}
    where
      not (true {{ filter }})
      {{ filter_partition_by }}
    {% else -%}
    select {{ empty_columns_csv }}
    where false
    {% endif %}
  );
{%- endmacro %}


{% macro tbmacro_make_temp_insert_overwrite_relation(base_relation, suffix='__dbt_insert_overwrite_tmp') -%}
  {#-- Get default sql code for temporary table with custom suffix --#}
  {{ return(adapter.dispatch('make_temp_relation', 'dbt')(base_relation, suffix)) }}
{%- endmacro %}
