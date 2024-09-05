# CHANGELOG

All notable changes to this project will be documented in this file.

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
