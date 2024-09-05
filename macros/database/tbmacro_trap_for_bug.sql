{% macro tbmacro_trap_for_bug(relation=this) %}
    {#-- Trap for replace table bug in incremental models, error in spark on "show table extended like '*'" --#}
    {%- if not execute or should_full_refresh() -%}
        {{ return('') }}
    {%- else -%}
        {%- set existing_relation = load_relation(relation) -%}
        {%- if existing_relation is none or not existing_relation -%}
            {{ log('(tbmacro_trap_for_bug) existing_relation not found: '~relation.schema~'.'~relation.identifier) }}
            {%- set check = tbmacro.tbmacro_check_relation(relation) -%}
            {%- if check == True -%}
                {% set invalid_spark -%}
                    (tbmacro_trap_for_bug) table was found in database but relations doesnt exist in manifest: {{ relation }}
                {%- endset %}
                {% do exceptions.raise_compiler_error(invalid_spark) %}
                {{ return("select raise_error('ERROR: trap for bug');") }}
            {%- else -%}
                {{ log('(tbmacro_trap_for_bug) check completed successfully') }}
                {{ return('') }}
            {%- endif -%}
        {%- else -%}
            {{ log('(tbmacro_trap_for_bug) no check, relation was found in manifest: '~relation.schema~'.'~relation.identifier) }}
            {{ return('') }}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}
