{#-- is_incremental for tbm_incremental --#}
{% macro tbmacro_is_incremental() %}
    {%- set return_value = none -%}
    {%- if tbmacro.tbmacro_check_relation(this)==true
            and not should_full_refresh()
            and model.config.materialized == 'tbm_incremental' -%}
        {%- set return_value = true -%}
    {%- endif -%}
    {{ return(return_value) }}
{% endmacro %}
