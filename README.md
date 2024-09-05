# tbmacro
custom macros for advanced dbt-spark usage

### database
- tbmacro_check_relation - check realition in database by relation or model,schema
- tbmacro_trap_for_bug - fail then relation isn't exists in manifest but exists in database (incorrect dbt behaviour then execute "show table extended like '*'")

### utils
tbmacro_extreme_dates - two simple macros for getting min and max date for timestamp

### materialization
macros for custom materialization 'tbm_incremental'

### location
custom location configurated by 'bucket_root'

### schema
custom configuration for schema
