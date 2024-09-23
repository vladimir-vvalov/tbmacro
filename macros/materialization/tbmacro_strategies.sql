{% macro tbmacro_get_incremental_sql(source, target, existing, tbm_config, filter, filter_merge) %}
  {#-- tbm_incremental strategies management --#}
  {%- set strategy = tbm_config.strategy -%}
  {%- if strategy in ['append', 'delete+insert'] -%}
    {#-- insert new records into existing table, without updating or overwriting --#}
    {{ tbmacro.tbmacro_get_insert_into_sql(source, target) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {#-- insert or overwrite all partitions existing in selection --#}
    {%- if tbm_config.mode is not none and not filter -%}
      {#-- append empty selection because selection is empty and config 'tbm_filter_mode' exists --#}
      {{ tbmacro.tbmacro_get_insert_into_sql(source, target) }}
    {%- else -%}
      {{ get_insert_overwrite_sql(source, target, existing) }}
    {%- endif -%}
  {%- elif strategy in ['merge'] -%}
    {#-- check for update using config 'tbm_update_changes_only' --#}
    {%- set check_update_changes_only = tbmacro.tbmacro_check_update_changes_only(target, source, tbm_config) -%}
    {%- if check_update_changes_only == false -%}
      {#-- merge all columns for datasources which implement MERGE INTO (spark) --#}
      {{ tbmacro.tbmacro_get_merge_sql(target, source, tbm_config, filter_merge, dest_columns=none) }}
    {%- else -%}
      {{ log("(tbm_incremental) Merge model did not update because data had not changed") }}
      {{ "SELECT true;" }}
    {%- endif -%}
  {%- else -%}
    {% set no_sql_for_strategy_msg -%}
      No known SQL for the incremental strategy provided: {{ strategy }}
    {%- endset %}
    {%- do exceptions.raise_compiler_error(no_sql_for_strategy_msg) -%}
  {%- endif -%}
{% endmacro %}


{% macro tbmacro_get_insert_into_sql(source_relation, target_relation) %}
  {#-- Insert clause --#}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
  insert into table {{ target_relation }} ({{dest_cols_csv}})
  select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}


{% macro tbmacro_get_delete_from_sql(target_relation, condition) %}
  {#-- Delete clause --#}
  delete from {{ target_relation }}
  where true
  {{ condition }}
{% endmacro %}


{% macro tbmacro_get_merge_sql(target, source, tbm_config, filter, dest_columns) -%}
  {{ adapter.dispatch('tbmacro_get_merge_sql', 'tbmacro')(target, source, tbm_config, filter, dest_columns) }}
{%- endmacro %}

{% macro spark__tbmacro_get_merge_sql(target, source, tbm_config, filter, dest_columns) -%}
  {#-- Merge clause --#}
  {%- set unique_key = tbm_config.unique_key -%}
  {%- set merge_update_columns = tbm_config.merge_update_columns -%}
  {%- set merge_exclude_columns = tbm_config.merge_exclude_columns -%}
  {%- set predicates = tbm_config.incremental_predicates -%}
  {%- set operator = tbm_config.operator -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {#-- need dest_columns for merge_exclude_columns, default to use "*" --#}
  {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {#-- update_columns was updated --#}
  {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}

  {{ sql_header if sql_header is not none }}

  merge into {{ target }} as DBT_INTERNAL_DEST
      using {{ source }} as DBT_INTERNAL_SOURCE
      on true
        {% if unique_key is not none and unique_key -%}
        {% for item in unique_key|unique|list -%}
          and DBT_INTERNAL_SOURCE.`{{ item }}` = DBT_INTERNAL_DEST.`{{ item }}`
        {% endfor %}
        {%- endif %}
        {% if predicates is not none and predicates -%}
        {% for item in predicates -%}
          and {{ item }}
        {% endfor %}
        {%- endif %}
        {{ filter }}

      when matched
      then update set
        {% if update_columns -%}
          {%- for column_name in update_columns %}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
          {%- endfor %}
        {%- else -%}
          {%- for column_name in dest_columns %}
            {{ column_name.quoted }} = DBT_INTERNAL_SOURCE.{{ column_name.quoted }}
            {%- if not loop.last %}, {%- endif %}
          {%- endfor %}
        {% endif %}

      when not matched
        {% if predicates is not none and predicates -%}
        {% for item in predicates -%}
          {%- if 'DBT_INTERNAL_SOURCE' in item and 'DBT_INTERNAL_DEST' not in item -%}
          and {{ item }}
          {%- endif -%}
        {% endfor %}
        {%- endif %}
        then insert
          ({% for column_name in dest_columns -%}
            {{ column_name.quoted }}{%- if not loop.last %}, {%- endif %}
          {%- endfor %})
          values
          ({% for column_name in dest_columns -%}
            DBT_INTERNAL_SOURCE.{{ column_name.quoted }}{%- if not loop.last %}, {%- endif %}
          {%- endfor %})

      {% if operator is not none and operator -%}
      when not matched by source
        {% if predicates is not none and predicates -%}
        {% for item in predicates -%}
          {%- if 'DBT_INTERNAL_SOURCE' not in item and 'DBT_INTERNAL_DEST' in item -%}
          and {{ item }}
          {%- endif -%}
        {% endfor %}
        {%- endif %}
        {{ filter }}
      then
        {{ operator }}
      {% endif %}

{%- endmacro %}


{% macro tbmacro_check_update_changes_only(target, source, tbm_config) -%}
  {#-- check merge for complete match --#}
  {%- set update_changes_only = tbm_config.update_changes_only -%}
  {#-- return false if not need to check --#}
  {%- if update_changes_only == true and execute -%}
    {%- set unique_key = tbm_config.unique_key -%}
    {%- set include_check_columns = tbm_config.include_check_columns -%}
    {%- set exclude_columns = tbm_config.exclude_columns -%}

    {#-- need dest_columns for merge_exclude_columns, default to use "*" --#}
    {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
    {#-- update_checked_columns was checked if tbm_update_changes_only=true --#}
    {%- set update_checked_columns = tbmacro.tbmacro_get_merge_update_columns(tbm_config, dest_columns) -%}

    {#-- count  --#}
    {%- set sql -%}
    select count(*) as _dbt__tbmacro_check_count
    from {{ target }} as DBT_INTERNAL_DEST
    full join {{ source }} as DBT_INTERNAL_SOURCE
      on true
      {% if unique_key is not none and unique_key -%}
      {% for item in unique_key|unique|list -%}
        and DBT_INTERNAL_SOURCE.`{{ item }}` = DBT_INTERNAL_DEST.`{{ item }}`
      {% endfor %}
      {%- endif %}
    where true
      and (
      {%- for column_name in update_checked_columns|unique|list %}
        {{ "or" if not loop.first }} not coalesce(DBT_INTERNAL_DEST.{{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}, coalesce(DBT_INTERNAL_DEST.{{ column_name }}, DBT_INTERNAL_SOURCE.{{ column_name }}) is null, false)
      {%- endfor %}
      )
    {%- endset -%}

    {%- set result = run_query(sql) -%}
    {%- set cnt = result.columns[0].values() -%}
    {%- if cnt[0] | int == 0 -%}
      {{ return(true) }}
    {%- else -%}
      {{ return(false) }}
    {%- endif -%}

  {%- else -%}
    {{ return(false) }}
  {%- endif -%}
{%- endmacro %}
