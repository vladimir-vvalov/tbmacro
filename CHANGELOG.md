# CHANGELOG

All notable changes to this project will be documented in this file.

## 0.5.3 - (2026-01-12)

### Feature
* add `tbmacro_is_incremental()` macro for checking incremental mode with custom materializations

### Changed
* change default value of `tbm_filter_merge_check` from `false` to `true` for more intuitive behavior with filtered change detection

### Documentation
* complete comprehensive README.md with detailed configuration, examples, and best practices

### Testing
* add integration test models for all incremental strategies (delete+insert, merge, insert_overwrite)
* add YAML schema files for integration test models
* add incremental run step to CI workflow for proper incremental logic testing

## 0.5.2 - (2026-01-08)

### Testing
* add GitHub Actions CI/CD workflow for automated testing
* add integration tests structure
* add pre-commit hooks
* update .gitignore

## 0.5.1 - (2026-01-07)

### Fix
* fix 'tbm_filter_merge_check' to only apply when 'tbm_filter_mode' is configured
* comments clarified

## 0.5.0 - (2026-01-06)

### Feature
* add parameter 'tbm_filter_merge_check' to control filter behavior in merge operations

### Fix
* fix 'insert_overwrite' strategy to handle empty result set correctly
* fix 'merge' strategy with 'tbm_update_changes_only' option to correctly evaluate changes when using 'tbm_filter_mode' and 'tbm_filter_key'
* fix 'tbm_contract' validation to display detailed column differences

## 0.4.6 - (2024-09-20)

### Fix
* fix 'merge' strategy behavior with 'tbm_update_changes_only' option

## 0.4.5 - (2024-09-20)

### Fix
* add validation for null values in filter key columns
* improve error messages clarity

## 0.4.4 - (2024-09-20)

### Fix
* fix 'insert_overwrite' strategy when filter mode is configured

## 0.4.3 - (2024-09-16)

### Fix
* rename parameter 'tbm_filter_quote_columns' to 'tbm_filter_quote_values' for clarity
* fix quote handling for filter range parameters

## 0.4.2 - (2024-09-13)

### Fix
* remove location management functionality
* fix location retrieval logic
* add validation for null values before applying string-based filters

## 0.4.0 - (2024-09-12)

### Fix
* remove deprecated macro functionality

## 0.3.0 - (2024-08-16)

### Feature
* add 'tbm_contract' option to validate columns and data types with schema change handling
* add 'tbm_contract_description' parameter to control how column description mismatches are handled
* allow table creation without column descriptions

## 0.2.0 - (2024-08-14)

### Feature
* add bug trap mechanism to 'tbm_incremental' materialization (macro 'tbmacro_trap_for_bug')
* add 'tbm_update_changes_only' option to 'merge' strategy
* add error handling improvements in bug trap
* add codeowners file

### Fix
* fix column selection in 'delete+insert' and 'merge' strategies to use explicit columns instead of wildcards

## 0.1.0 - (2024-08-06)

### New
* created dbt package
