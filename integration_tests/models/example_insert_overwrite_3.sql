-- Test custom tbm_incremental insert_overwrite with 'values' mode
{{ config(
    materialized='tbm_incremental',
    file_format='delta',
    incremental_strategy='insert_overwrite',
    tbm_contract=true,
    partition_by='partition_month',
    tbm_filter_mode='ranged_values',
    tbm_filter_key='created_date',
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
