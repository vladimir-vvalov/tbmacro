-- Test custom tbm_incremental materialization
{{ config(
    materialized='tbm_incremental',
    unique_key='id'
) }}

select
    id,
    value,
    created_at
from {{ ref('test_data') }}

{% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
{% endif %}
