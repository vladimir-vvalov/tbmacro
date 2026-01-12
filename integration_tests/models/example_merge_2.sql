-- Test custom tbm_incremental merge with delete operator and check changes
{{ config(
    materialized='tbm_incremental',
    incremental_strategy='merge',
    tbm_contract=true,
    unique_key=['id','created_date'],
    tbm_merge_operator='delete',
    tbm_update_changes_only=true,
    tbm_exclude_check_columns=['modified_at'],
) }}

select
    id,
    value,
    created_date,
    year(created_date) * 100 + month(created_date) as partition_month,
    current_timestamp() as modified_at
from {{ ref('example_data') }}
where
{% if tbmacro.tbmacro_is_incremental() %}
    created_date in ('2024-02-01', '2024-02-03')
{% else %}
    created_date <= '2024-02-02'
{% endif %}
