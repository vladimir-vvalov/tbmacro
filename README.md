# tbmacro
custom macros for advanced dbt-spark usage

### materialization
macros for custom materialization 'tbm_incremental'

### database
- tbmacro_check_relation - check realition in database by relation or model,schema
- tbmacro_trap_for_bug - fail then relation isn't exists in manifest but exists in database (incorrect dbt behaviour then execute "show table extended like '*'")
