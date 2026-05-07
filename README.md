# tbmacro

Advanced custom incremental materialization for dbt-spark with enhanced filtering, schema validation, and partitioning strategies.

[![dbt](https://img.shields.io/badge/dbt-≥1.8.0-orange.svg)](https://docs.getdbt.com)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-compatible-blue.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-only-green.svg)](https://delta.io/)

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
  - [Basic Configuration](#basic-configuration)
  - [Schema Contract Options](#schema-contract-options)
  - [Filter Options](#filter-options)
  - [Merge Strategy Options](#merge-strategy-options)
- [Strategies](#strategies)
- [Examples](#examples)
- [Utilities](#utilities)
- [Limitations](#limitations)
- [Requirements](#requirements)
- [Changelog](#changelog)
- [License](#license)

## 🎯 Overview

`tbmacro` is a dbt package that provides a custom `tbm_incremental` materialization for Apache Spark with **Delta Lake tables only**. It extends the standard dbt incremental materialization with advanced features including:

- **Advanced filtering for incremental strategies** - optimize delete+insert, merge, and insert_overwrite operations by controlling which data they process
- **Schema contract with upfront table creation** - create tables with data types and NOT NULL constraints from YAML without ALTER TABLE
- **Enhanced merge strategies** with change-only updates
- **Flexible partition usage** with advanced filtering capabilities
- **Bug prevention** for Spark manifest inconsistencies
- **Non-conflicting design** - doesn't override any default dbt macros
- **Custom incremental check** - use `tbmacro_is_incremental()` instead of `is_incremental()` for custom materializations

This package is designed for data engineers working with large-scale Delta Lake tables who need fine-grained control over incremental updates and data quality. It works particularly well with external tables and helps better manage Delta files. Combined with optimize write, it reduces the number of file operations - especially beneficial for slow storage systems. The package works alongside standard dbt materializations without any conflicts.

## ✨ Features

### 🛡️ Schema Contract with Upfront Table Creation

Create Delta tables with proper schema definition **before** data insertion:
- Define data types and `NOT NULL` constraints directly from dbt YAML schema
- Tables are created upfront with all constraints, avoiding `ALTER TABLE` operations on every run
- Automatic column validation: check column names, types, and descriptions on every run
- Critical for environments where `ALTER TABLE` is limited (e.g., `delta.minWriterVersion=2`) or on slow storage

**Benefits of NOT NULL constraints:**
- **Faster joins**: Query optimizer can leverage constraint information
- **Skip dbt tests**: No need for `not_null` tests - enforced at the table level
- **Data quality**: Prevent NULL values at write time, not just test time

Standard dbt-spark cannot create tables with `NOT NULL` constraints before data insertion; `tbm_incremental` solves this.

### 🔍 Advanced Filtering for Strategy Optimization

Control which data your incremental strategies operate on, improving performance and reducing unnecessary operations:

**Three filtering modes:**
- **Values mode**: Target specific values (e.g., specific regions or categories)
- **Range mode**: Work with date/timestamp ranges
- **Ranged values mode**: Combine both approaches

**How it helps strategies:**
- **Delete+Insert**: Define which rows to delete before inserting (according to filter parameters)
- **Merge**: Limit merge conditions to relevant data subsets, plus `delete` operator support for removing unmatched rows
- **Insert Overwrite**: Control which partitions to process without overwriting everything

Without filtering, all strategies work with default behavior (as in dbt-spark 1.9.*).

### 🎯 Smart Change Detection

With `tbm_update_changes_only`, skip merge operations when source data hasn't changed, saving compute resources and processing time.

### 🐛 Built-in Bug Prevention

Automatically detect and prevent Spark manifest inconsistencies that can cause data pipeline failures (see [Utilities](#utilities) for details).

### ✅ Safe & Non-Conflicting

The package uses a custom `tbm_incremental` materialization and doesn't override any default dbt macros or materializations. You can safely use it alongside standard dbt incremental models without any conflicts.

### ⚠️ Important: Using `is_incremental()` with Custom Materializations

**Warning:** The standard dbt `is_incremental()` function does not recognize custom incremental materializations as incremental.

**Solution:** Use `tbmacro.tbmacro_is_incremental()` instead of `is_incremental()` in your model SQL when working with `tbm_incremental` materialization.

```sql
-- ❌ Don't use this with tbm_incremental:
{% if is_incremental() %}

-- ✅ Use this instead:
{% if tbmacro.tbmacro_is_incremental() %}
```

## 📦 Installation

Add one of the following to your `packages.yml`:

**Option 1: Git repository (recommended)**
```yaml
packages:
  - git: "https://github.com/vladimir-vvalov/tbmacro.git"
    revision: v1.0.1
```

**Option 2: Tarball**
```yaml
packages:
  - tarball: https://github.com/vladimir-vvalov/tbmacro/archive/refs/tags/v1.0.0.tar.gz
    name: 'tbmacro'
```

Then run:

```bash
dbt deps
```

## ⚙️ Configuration

### Basic Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `materialized` | string | - | Set to `'tbm_incremental'` |
| `file_format` | string | - | **Must be set to `'delta'`** (required) |
| `incremental_strategy` | string | `'append'` | Strategy: `append`, `delete+insert`, `insert_overwrite`, `merge` |
| `limit` | string | `none` | If defined, adds `LIMIT <value>` at the end of SQL code |
| `tbm_trap_for_bug` | boolean | `true` | Enable Spark manifest bug detection |

**Note:** If the manifest bug doesn't occur in your system, you can set `tbm_trap_for_bug` to `false` globally in your `dbt_project.yml` for all models.

### Schema Contract Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tbm_contract` | boolean | `false` | Enable schema contract validation |
| `tbm_cast_columns` | boolean | `false` | Auto-cast columns to match target schema |
| `tbm_contract_description` | string | `'warn'` | How to handle description validation: `ignore`, `warn`, `error` |

#### How `tbm_contract=true` works:

1. **YAML schema is required**: All columns and their data types must be defined in `.yml`. It may be more convenient to develop with the contract disabled or using the default `incremental` materialization, then configure `.yml` and enable `tbm_contract` at the end. Constraints are optional.

2. **Two-stage table creation**: When the target table is not found in the metastore, it's not created via CTAS like in default materialization. The macro splits the process into 2 stages executed "under the hood":
   - Creating an empty table based on `.yml` definition
   - Writing the result set to the table

3. **Table restoration from location**: When the target table is not found in the metastore, but Delta files from a previous or different table are found in the target location, the macro restores the old table under the current model's name, then proceeds as described in point 2.

4. **Full refresh behavior**: With the `--full-refresh` parameter, the macro acts as in point 2 without searching in the metastore.

5. **Validation on full refresh**: If the table is found in the metastore and `--full-refresh` is specified, the macro validates the column list and data types between `.yml` and the table. If there's a mismatch, it raises an error with a list of differences.
   - *Note: Constraint validation is not performed because this information is not available in `get_columns_in_relation()`.*

**When to use `tbm_contract`:**
- When you need strict control over column list, data types, and constraints
- When your system doesn't support metadata evolution through ALTER TABLE
- When working with external tables

#### Additional contract parameters:

**`tbm_cast_columns`** (requires `tbm_contract=true`):
- When set to `true`, automatically casts all columns to the data types defined in `.yml`
- Useful when source data types don't exactly match the target schema

**`tbm_contract_description`** (requires `tbm_contract=true`):
- Controls behavior for missing column descriptions in `.yml`:
  - `warn` - prints a warning message (default)
  - `error` - raises an error and stops execution
  - `ignore` - skips description validation

### Filter Options

Control which data your incremental strategies operate on (not for filtering result data).

**Important:** These parameters do not filter the data that will be inserted or updated in the target table. They are applied in the operation of incremental strategies. Their application is described in detail in the [Strategies](#strategies) section.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tbm_filter_mode` | string | `none` | Filter mode: `values`, `range`, `ranged_values` |
| `tbm_filter_key` | string/list | `none` | Column(s) to filter on |
| `tbm_filter_from` | string/list | `none` | Start value(s) for range filter |
| `tbm_filter_till` | string/list | `none` | End value(s) for range filter |
| `tbm_filter_quote_values` | boolean | `true` | Wrap column names in backticks during execution |
| `tbm_filter_merge_check` | boolean | `true` | Set to `false` to ignore `tbm_filter_*` parameters during change detection with `tbm_update_changes_only` |

#### Understanding `tbm_filter_mode`:

The `tbm_filter_mode` parameter is key to controlling behavior of incremental strategies: `delete+insert`, `insert_overwrite`, and `merge`.

**Available modes:**

- **`none`** (default): Default behavior without filtering

- **`values`**: Filters based on a list of unique value combinations from columns specified in `tbm_filter_key`. The filter list is built from distinct combinations of key column values in the source data.

- **`range`**: Filters based on a range between lower boundary (`tbm_filter_from`) and upper boundary (`tbm_filter_till`).
  - For composite keys, boundaries are compared as text
  - You can specify only one boundary (either `tbm_filter_from` or `tbm_filter_till`)

- **`ranged_values`**: Hybrid approach:
  - If both `tbm_filter_from` and `tbm_filter_till` are defined, behaves like `range` mode
  - If any boundary is not defined, the boundary value is determined from the query result

#### Additional filter parameters:

**`tbm_filter_quote_values`**:
- Controls whether column names are wrapped in backticks (`) during model execution
- Default: `true`

**`tbm_filter_merge_check`**:
- When set to `true`, applies filter from `tbm_filter_*` parameters for change detection when `tbm_update_changes_only` is enabled
- Useful for limiting the scope of change comparison to filtered data only

#### Performance considerations:

Value sets for filtering are obtained in `values` and `ranged_values` modes from the result set, stored in a list, and used as scalar filters. This means:

- **Memory impact**: Very high granularity of values in `tbm_filter_key` columns within the current result set can load the dbt session memory
- **Performance benefit**: Despite memory considerations, scalar filters are significantly more efficient than subqueries, making this approach suitable for most incremental models

Choose your filter key columns carefully to balance memory usage with query performance.

### Merge Strategy Options

Fine-tune merge behavior:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `tbm_update_changes_only` | boolean | `false` | Skip merge if no changes detected |
| `tbm_include_check_columns` | list | `none` | Columns to check for changes |
| `tbm_exclude_check_columns` | list | `none` | Columns to exclude from change check |
| `tbm_merge_operator` | string | `none` | Merge operator: `delete` |
| `incremental_predicates` | list | `none` | Additional merge conditions |

#### Understanding `tbm_update_changes_only`:

The `tbm_update_changes_only` parameter compares the result set data with the target table to determine if a merge operation is necessary.

**How it works:**
- Compares data using columns specified in `tbm_include_check_columns`, or all columns except those in `tbm_exclude_check_columns`
- If no changes are detected, the merge operation is skipped entirely, saving compute resources

**Filter integration:**
- When `tbm_filter_mode` is defined together with `tbm_update_changes_only`, the filter is automatically applied during change detection (default behavior: `tbm_filter_merge_check=true`)
- This applies the `tbm_filter_*` parameters to the target table during change detection, ensuring filters are consistent
- Set `tbm_filter_merge_check=false` if you need to compare the entire target table with the filtered result set

**When to use:**
- Ideal when model refresh frequency significantly exceeds data change frequency
- Best suited for lightweight models where change detection overhead is minimal

**Performance consideration:**
- For heavy/complex models, the change detection query may be more expensive than performing the merge operation itself
- Evaluate the cost-benefit based on your model's complexity and data change patterns

#### Understanding `tbm_merge_operator`:

The `tbm_merge_operator` parameter adds additional operations to the merge statement.

**Available operators:**

- **`delete`**: Adds a `WHEN NOT MATCHED BY SOURCE` block to the merge query:
  ```sql
  WHEN NOT MATCHED BY SOURCE
    <tbm_filter conditions>
  THEN DELETE
  ```
  This removes rows from the target table that don't exist in the source (with optional filter conditions applied).

Currently, `delete` is the only supported operator. Other operations may be added in future versions.

#### Understanding `incremental_predicates`:

The `incremental_predicates` parameter modifies the behavior of the standard dbt configuration ([see dbt docs](https://docs.getdbt.com/docs/build/incremental-strategy)). It can contain one or multiple conditions (when specified as a list).

**Predicate syntax:**
- Conditions use aliases `DBT_INTERNAL_SOURCE` and `DBT_INTERNAL_DEST` to reference source and target tables
- Multiple predicates are combined with `AND` automatically - don't add `AND` manually in your predicate code

**Predicate placement logic:**

1. **Standard behavior**: Predicates are added to the join block of the merge statement:
   ```sql
   MERGE INTO ... USING ... ON ... <incremental_predicates>
   ```

2. **Source-only predicates**: If a predicate includes only `DBT_INTERNAL_SOURCE`, it's added to the `WHEN NOT MATCHED` block

3. **Destination-only predicates**: If a predicate includes only `DBT_INTERNAL_DEST`, it's added to the `WHEN NOT MATCHED BY SOURCE` block

4. **Matched block**: Predicates are NOT added to the `WHEN MATCHED` block

## 🎯 Strategies

### Check Relation

Before diving into strategies, it's important to understand how to check if your model should run in incremental mode.

**The Problem with `is_incremental()`:**

The standard dbt `is_incremental()` function does not work with custom materializations. It only recognizes the built-in `incremental` materialization as incremental.

**The Solution:**

Use `tbmacro.tbmacro_is_incremental()` instead:

```sql
{% if tbmacro.tbmacro_is_incremental() %}
```

### Insert Overwrite

**How it works:**

The macro combines the result set with data from partitions that exist in the result set AND do not fall under the conditions of `tbm_filter_*` parameters. This reduces the number of file operations.

Compared to the default behavior, additional queries are executed under the hood:
- Selecting unique values from `tbm_filter_key`
- Selecting unique values from `partition_by`
- Result set UNION with data from the target table that doesn't match the filter conditions

Combined with `spark.databricks.delta.optimizeWrite.enabled`, this can be more efficient because the number of files is minimized.

#### Example: Optimizing date-based updates with minimal file operations

**The Task:**

Suppose we need to update a model for one or several `created_date` values. It's important to minimize load on file storage. We use Spark parameters for write optimization (configured in `profiles.yml`):

```python
'spark.databricks.delta.optimizeWrite.enabled': 'true',
'spark.databricks.delta.optimizeWrite.binSize': '512',
```

Also assume that the data volume per date is approximately 4 MB.

**Options with default insert_overwrite:**

1. **`partition_by='created_date'`**: The table will have many small files - doesn't meet our requirements

2. **`partition_by='partition_month'`**: One file will weigh 4×30 = 120 MB. This size is acceptable, but we'd have to filter and calculate data for a longer period (full months)

3. **Manually complicating the model script**: Requires additional logic and maintenance

**Solution with tbm_incremental:**

```sql
-- Optimized update with values mode
{{ config(
    materialized='tbm_incremental',
    file_format='delta',
    incremental_strategy='insert_overwrite',
    partition_by='partition_month',
    tbm_filter_mode='values',
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
```

**Step-by-step execution:**

**Initial run (first execution):**

The table doesn't exist yet, a simple initial run is performed.

Result:
- 2 partitions are created: `202401` and `202402`
- One file in each partition
- Data for full January 2024 and February 1-2, 2024

**Second run (incremental):**

Suppose something changed in the sources and now the result set returns only `created_date in ('2024-02-01', '2024-02-03')`.

What the macro does:
1. Identifies unique values of `tbm_filter_key` (`created_date`) from the result set: `['2024-02-01', '2024-02-03']`
2. Identifies unique values of `partition_by` (`partition_month`) from the result set: `[202402]`
3. Selects data from the target table in partition `202402` that **DOES NOT** match the filter `created_date in ('2024-02-01', '2024-02-03')`
   - This is data for `'2024-02-02'` - it remains unchanged
4. Combines (UNION) the result set with the selected data from step 3
5. Overwrites partition `202402` with the combined dataset

Result after the second run:
- Data for `'2024-02-01'` - fully updated
- Data for `'2024-02-02'` - remained unchanged (preserved from previous run)
- Data for `'2024-02-03'` - added
- Partition `202401` - remained unchanged
- **Only 1 new file physically added** to partition `202402`

**Benefits of this approach:**

Everything happens as it would with default `insert_overwrite` with `partition_by='created_date'` (pinpoint updates by dates), but physically the operation is performed at the `partition_month` partition level. Thanks to `optimizeWrite`, the entire updated dataset for the partition is written to one optimized file instead of multiple small files.

**Important note about partition overwrite behavior:**

The parameter `spark.sql.sources.partitionOverwriteMode = DYNAMIC` is set automatically **only when the table is found in the metastore**. This is critical for external tables where old Delta files may exist at the location while the metastore has no record of the table:

- **Table in metastore**: DYNAMIC mode is enabled, only affected partitions are overwritten
- **Table not in metastore**: DYNAMIC mode is NOT set, preventing old data with different partition values from appearing in the newly created table

This behavior ensures data integrity when working with external tables and existing Delta files.

#### Example 2: Using range mode for boundary-based filtering

**Solution with range mode:**

```sql
-- Optimized update with range mode
{{ config(
    materialized='tbm_incremental',
    file_format='delta',
    incremental_strategy='insert_overwrite',
    partition_by='partition_month',
    tbm_filter_mode='range',
    tbm_filter_key='created_date',
    tbm_filter_from="'2024-02-01'",
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
```

**How it differs from values mode:**

With `tbm_filter_mode='range'`, you specify boundary conditions using `tbm_filter_from` and/or `tbm_filter_till`. In this example, we set the lower boundary `tbm_filter_from="'2024-02-01'"`, which creates a filter `created_date >= '2024-02-01'`.

**Notes:**
- The filter is **inclusive** for both boundaries - values equal to `tbm_filter_from` or `tbm_filter_till` are included in the filter
- You can use expressions, not just date constants. For example: `tbm_filter_till="current_date-1"` to filter up to yesterday

Result after incremental run:
- Data for `'2024-02-01'` - fully updated
- Data for `'2024-02-02'` - deleted (falls under the range filter but not in result set)
- Data for `'2024-02-03'` - added
- Partition `202401` - remained unchanged
- **Only 1 new file physically added** to partition `202402`

The key difference: range mode filters by boundaries, so data within the range that's not in the result set gets deleted, while values mode only operates on specific values present in the result set.

#### Example 3: Using ranged_values mode with automatic boundary detection

**Solution with ranged_values mode:**

```sql
-- Optimized update with ranged_values mode
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
```

**How it works:**

`tbm_filter_mode='ranged_values'` is a hybrid approach that combines range and values modes:

- You can specify `tbm_filter_from` and/or `tbm_filter_till` - they take priority when defined
- If `tbm_filter_from` is not specified - the minimum value from `tbm_filter_key` in the result set is used
- If `tbm_filter_till` is not specified - the maximum value from `tbm_filter_key` in the result set is used

In this example, the filter range is automatically determined from the result set: `'2024-02-01'` to `'2024-02-03'` (inclusive).

Result after incremental run:
- Data for `'2024-02-01'` - fully updated
- Data for `'2024-02-02'` - deleted (falls under the range filter but not in result set)
- Data for `'2024-02-03'` - added
- Partition `202401` - remained unchanged
- **Only 1 new file physically added** to partition `202402`

The result is the same as Example 2, but the boundaries are determined dynamically from the data rather than being hardcoded in the configuration.

**Schema contract in action:**

This example also demonstrates `tbm_contract=true` functionality:

**Initial run:**
1. An empty table is created first according to the `.yml` schema definition
2. Initial data is then inserted into the table with all constraints (including `NOT NULL`) enforced from the start

**Second run (incremental):**
1. The macro validates the column list and data types against the `.yml` schema
2. After validation passes, the incremental update proceeds according to the scenario described above

**Why this matters:**

In some systems, you cannot add a `NOT NULL` constraint to an existing table using `ALTER TABLE`. The default `CREATE OR REPLACE TABLE` also removes all existing constraints. With `tbm_contract=true`, the `NOT NULL` constraints are preserved from the moment of table creation, ensuring data integrity throughout the table's lifetime without the need for schema alterations.

### Delete + Insert

**How it works:**

This strategy is not available in default dbt-spark but exists in some other adapters. The implementation is very similar to other strategies, but instead of using the `unique_key` parameter, it uses the `tbm_filter_*` parameter logic described above in the insert_overwrite examples.

The operation happens in two transactions:
1. **First transaction**: Rows matching the filter conditions are deleted
2. **Second transaction**: The result set is inserted

**Example with values mode:**

```sql
-- Delete+insert with values mode
{{ config(
    materialized='tbm_incremental',
    file_format='delta',
    incremental_strategy='delete+insert',
    partition_by='partition_month',
    tbm_filter_mode='values',
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
```

This example uses `values` mode. During incremental updates, it deletes rows with `created_date in ('2024-02-01', '2024-02-03')` - values obtained from the result set - and then inserts the result set.

Result after incremental run:
- Data for `'2024-02-01'` - fully updated
- Data for `'2024-02-02'` - remained unchanged
- Data for `'2024-02-03'` - added
- Partition `202401` - remained unchanged
- May create 2 files in partition `202402` before OPTIMIZE: first after delete, second after insert

**Important notes:**

1. **Use OPTIMIZE post-hook**: Since delete+insert creates two separate transactions, you may want to add `post_hook=["OPTIMIZE {{ this }};"]` to consolidate files

2. **insert_overwrite is generally better**: One transaction on the target table is always better than two. Consider using `insert_overwrite` strategy instead unless you have specific reasons to use `delete+insert`

### Merge

**How it works:**

The `tbm_filter_*` parameters are used in the same way as in `insert_overwrite` or `delete+insert` strategies, and serve as an alternative to `incremental_predicates`. You can use `tbm_filter_*` and `incremental_predicates` together - the filter conditions will be combined.

#### Example: Merge with values mode filtering

```sql
-- Merge with values mode
{{ config(
    materialized='tbm_incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key='id',
    tbm_filter_mode='values',
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
```

**How it works:**

During the incremental run, `created_date` values are obtained from the result set and applied in the merge transaction:

```sql
MERGE INTO <schema>.example_merge_1 AS DBT_INTERNAL_DEST
    USING example_merge_1__dbt_tmp AS DBT_INTERNAL_SOURCE
    ON TRUE
    AND DBT_INTERNAL_SOURCE.`id` = DBT_INTERNAL_DEST.`id`
    AND DBT_INTERNAL_DEST.`created_date` IN ('2024-02-01', '2024-02-03')
...
```

The filter condition `created_date IN ('2024-02-01', '2024-02-03')` is automatically added to the merge join, limiting which rows from the target table participate in the merge operation. This can significantly improve performance by reducing the scope of the merge operation.

#### Example 2: Merge with delete operator and change detection

This example demonstrates:
1. Using a composite `unique_key`
2. Deleting rows from the target table that don't exist in the result set
3. Comparing target table with result set to skip unnecessary updates

```sql
-- Merge with delete operator and check changes
{{ config(
    materialized='tbm_incremental',
    file_format='delta',
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
```

**Configuration highlights:**

- **`unique_key=['id','created_date']`**: Composite key for matching rows
- **`tbm_merge_operator='delete'`**: Removes rows from target that don't exist in source
- **`tbm_update_changes_only=true`**: Skips merge if no changes detected
- **`tbm_exclude_check_columns=['modified_at']`**: Excludes `modified_at` from change comparison

**Execution flow:**

**First run (initial):**
- Creates table with initial data

**Second run (incremental):**
- Data for `created_date='2024-02-01'` - updated
- Data for `created_date='2024-02-02'` - deleted (not in result set, deleted by operator)
- Data for `created_date='2024-02-03'` - added

**Third run (incremental):**
- All data in result set matches target table (except `modified_at` column)
- Since `modified_at` is excluded from change check, no differences detected
- **No table or file updates occur** - merge operation is skipped entirely

This approach is particularly useful when your model runs frequently but data changes infrequently, saving compute resources and avoiding unnecessary file writes.

#### Example 3: Merge with filters and change detection

This example builds on Example 2 by adding partition and filter parameters:

```sql
-- Merge with ranged_values mode, delete and check changes
{{ config(
    materialized='tbm_incremental',
    file_format='delta',
    incremental_strategy='merge',
    tbm_contract=true,
    unique_key=['id','created_date'],
    tbm_merge_operator='delete',
    partition_by='partition_month',
    tbm_filter_mode='ranged_values',
    tbm_filter_key='created_date',
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
```

**Execution flow:**

Results are the same as Example 2:
- **First run**: Initial data load
- **Second run**: Update `'2024-02-01'`, delete `'2024-02-02'`, add `'2024-02-03'`
- **Third run**: No changes detected, skip merge

**Alternative: Setting `tbm_filter_merge_check=false`**

If you explicitly set `tbm_filter_merge_check=false`, the change detection will ignore `tbm_filter_*` parameters and compare the entire target table with the result set:
- **Second run**: In addition to the changes above, **all rows for January 2024 would also be deleted**
- This happens because the change detection compares the entire target table with the filtered result set, treating all January data as "deleted"

## 🛠️ Utilities

The package includes utility macros for relation management:

### `tbmacro_check_relation(relation)`

Checks if a relation (table/view) exists in the database. This is the underlying macro used by `tbmacro_is_incremental()`.

**Parameters:**
- `relation` (optional): The relation to check. Defaults to `this` (current model)
- Can specify a model name as a string - checks for that table in the current schema

```sql
-- Check current model
{% if tbmacro.tbmacro_check_relation() %}
    -- Current model exists
{% endif %}

-- Check another model in current schema
{% if tbmacro.tbmacro_check_relation('other_model') %}
    -- other_model exists in current schema
{% endif %}
```

### `tbmacro_check_table(model_name, schema_name)`

Similar to `tbmacro_check_relation`, but allows specifying both table name and schema.

**Parameters:**
- `model_name`: Name of the table to check
- `schema_name` (optional): Schema name. If not provided, uses current schema

```sql
-- Check table in current schema
{% set is_exists = tbmacro.tbmacro_check_table('my_table') %}

-- Check table in specific schema
{% set is_exists = tbmacro.tbmacro_check_table('my_table', 'my_schema') %}
```

### `tbmacro_trap_for_bug(relation)`

Detects Spark manifest inconsistencies that can cause pipeline failures. Protects against [dbt-spark issue #950](https://github.com/dbt-labs/dbt-spark/issues/950).

**How it works:**
- Automatically integrated into `tbm_incremental` materialization
- Only queries the database when `load_relation` returns no result
- Enabled by default (`tbm_trap_for_bug=true`)

```sql
-- Manual invocation in pre_hook (usually not needed with tbm_incremental)
{{ config(
    pre_hook="{{ tbmacro.tbmacro_trap_for_bug() }}"
) }}
```

**Note:** If the manifest bug doesn't occur in your system, you can disable it globally in `dbt_project.yml`:

```yaml
models:
  your_project:
    +tbm_trap_for_bug: false
```

## 📋 Requirements

**Version requirements:**
- **dbt:** >=1.8.0, <2.0.0
- **Adapter:** dbt-spark
- **File format:** Delta Lake only (required)

**Package compatibility:**

The package uses a custom `tbm_incremental` materialization and **does not override** any default dbt macros or materializations. It can safely coexist with:

- Standard `incremental` materialization
- All other dbt materializations (table, view, ephemeral, etc.)
- Other dbt packages
- Custom macros from your project

You can use `tbm_incremental` and standard materializations in the same project without any conflicts.

## ⚠️ Limitations

### Schema Evolution with `on_schema_change`

When `on_schema_change` parameter is activated, the schema validation between the target table and YAML definition is disabled. This allows dbt's standard mechanisms to attempt schema changes, but may cause discrepancies between the table and YAML definitions.

### Manual Schema Updates with `tbm_contract=true`

The `tbm_contract=true` parameter does not support automatic schema changes when there's a mismatch between the table and `.yml` file. To manually update the schema:

1. Update the `.yml` file with new column definitions
2. Update the `.sql` model file accordingly
3. Run with `--full-refresh` flag
   - If you need an empty table after refresh, add `where false` at the end of your query
4. Manually copy data from the previous table version to the current one (accounting for schema changes)

### Constraint Validation

Validation between `.sql` and `.yml` does not check for constraint changes. The `get_columns_in_relation()` function doesn't provide constraint information, and adding an additional database query would be inefficient.

### Filter Boundary Behavior

With `tbm_filter_mode='ranged_values'`, the macro may ignore boundary values if they don't appear in the result set values but were present in the source filters.

### Merge Operator Options

The `tbm_merge_operator` parameter currently supports only `none` or `'delete'`. Other operations are not yet implemented.

## 📝 Changelog

See [CHANGELOG.md](CHANGELOG.md) for a detailed list of changes.

## 📄 License

This project is licensed under the Apache License 2.0.

---

**Note**: This package is specifically designed for Apache Spark with Delta Lake tables only. It will not work with other file formats (Parquet, ORC, etc.) or other data warehouses. For production use, thoroughly test the configurations with your specific Spark environment.
