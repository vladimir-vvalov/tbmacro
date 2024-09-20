# CHANGELOG

All notable changes to this project will be documented in this file.

## 0.4.5 - (2024-09-20)
---

### Fix
* add validation on null values of columns defined in 'tbm_filter_key'
* fix error messages

## 0.4.4 - (2024-09-20)
---

### Fix
* fix insert_overwrite bug with tbm_config.mode is not none

## 0.4.3 - (2024-09-16)
---

### Fix
* rename parameter 'tbm_filter_quote_columns' to 'tbm_filter_quote_values'
* remove quoting for 'tbm_filter_from' and 'tbm_filter_till'

## 0.4.2 - (2024-09-13)
---

### Fix
* removed location macros
* fix getting location path
* add preconditions to check none value before using string filters

## 0.4.0 - (2024-09-12)
---

### Fix
* removed unusable macros

## 0.3.0 - (2024-08-16)
---

### Feature
* check columns and datatypes when tbm_contract == true and on_schema_change not in ['fail', 'ignore']
* add parameter tbm_contract_description for manage check description behaviour: 'ignore', 'warn' or 'error'
* allow create table without descriptions

## 0.2.0 - (2024-08-14)
---

### Feature
* add runing tbmacro_trap_for_bug into tbm_incremental
* change tbm_update_changes_only behaviour
* change raize error in tbmacro_trap_for_bug
* add codeowners

### Fix
* fix query for delete+insert and merge (change * to columns)

## 0.1.0 - (2024-08-06)
---

### New
* created dbt package
