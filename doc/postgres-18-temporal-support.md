# Upgrading `sql_saga` for PostgreSQL 18 Native Temporal Support

PostgreSQL 18 introduces native support for application-time periods, a significant development that overlaps with core features of `sql_saga`. This document outlines the new capabilities and provides a strategic overview for adapting `sql_saga` to leverage them.

The key features are defined in the `CREATE TABLE` and `ALTER TABLE` DDL commands and are based on a single column of a `RANGE` or `MULTIRANGE` type:

- **Temporal Uniqueness:** `PRIMARY KEY (... , range_column WITHOUT OVERLAPS)` and `UNIQUE (... , range_column WITHOUT OVERLAPS)`.
- **Temporal Referential Integrity:** `FOREIGN KEY (... , range_column) REFERENCES ...`.

## 1. The Authoritative Range Column

The foundation of the new temporal features is a single column of a range type (e.g., `daterange`, `tsrange`, `int4range`). This column represents the entire validity period for a record.

## 2. Temporal Primary Keys and Unique Constraints (`WITHOUT OVERLAPS`)

PostgreSQL 18 allows defining primary keys and unique constraints that are enforced over time using a range column. This is a native replacement for the exclusion constraints (`EXCLUDE USING gist (... WITH &&)`) that `sql_saga` currently creates.

**Syntax:**
```sql
CREATE TABLE employees (
    employee_id int,
    department text,
    valid daterange,
    PRIMARY KEY (employee_id, valid WITHOUT OVERLAPS)
);
```

**Impact on `sql_saga`:**
- **Architectural Shift:** `sql_saga`'s core concept of an `era` must shift from being defined by two discrete `_from` and `_until` columns to being defined by a single, authoritative `range` column.
- **Simplification:** The `sql_saga.add_unique_key` function, which currently generates complex `EXCLUDE` constraints, could be refactored. On PostgreSQL 18+, it would instead generate a standard `ADD CONSTRAINT ... PRIMARY KEY ... WITHOUT OVERLAPS` on the range column.
- **Performance:** Native constraints are likely to be more performant and better integrated with the query planner than GIST-based exclusion constraints.

## 3. Temporal Foreign Keys

PostgreSQL 18 introduces temporal foreign keys that ensure referential integrity over time using range columns. A row in the referencing table must have its validity period covered by a valid period in the referenced table for a matching key.

**Syntax:**
```sql
CREATE TABLE projects (
    project_id int,
    lead_employee_id int,
    active_period daterange,
    PRIMARY KEY (project_id, active_period WITHOUT OVERLAPS),
    FOREIGN KEY (lead_employee_id, active_period) REFERENCES employees (employee_id, valid)
);
```

This ensures that for any period a project is active with a given lead employee, that employee must have a corresponding record in the `employees` table whose `valid` range covers the project's `active_period`.

**Impact on `sql_saga`:**
- **Major Simplification:** The `add_temporal_foreign_key` function has been refactored to use native `FOREIGN KEY (... PERIOD ...)` constraints. This eliminates the entire C-based and pl/pgsql trigger mechanism for temporal-to-temporal foreign keys, significantly reducing complexity and improving maintainability.
- **Robustness:** Native foreign keys are deeply integrated into the database, handling `UPDATE`, `DELETE`, and `MERGE` operations correctly and transactionally without the complexities and potential edge cases of triggers.
- **Backward Compatibility:** The `add_regular_foreign_key` function, which handles foreign keys from non-temporal tables, retains its trigger-based implementation, as this use case is not covered by the native PG18 features.

## 4. Strategic Path for `sql_saga` on PostgreSQL 18+

`sql_saga` can evolve to become a compatibility and enhancement layer on top of PostgreSQL's native temporal features. This requires a significant architectural shift.

1.  **Shift to Range-First Architecture:**
    -   The `sql_saga.add_era` function has been refactored. It now requires a single `range_column_name` which must be of a `RANGE` type.
    -   The `valid_from_column_name`, `valid_until_column_name`, and `valid_to_column_name` parameters are optional. If provided (or auto-detected via the `synchronize_columns` parameter), `sql_saga` manages them as synchronized, de-normalized representations of the main `range` column's bounds. The range column is now the single source of truth.
2.  **Version Detection:** All core DDL functions (`add_unique_key`, `add_foreign_key`) must first detect the PostgreSQL version.
3.  **Conditional Logic (PG18+):**
    -   `add_unique_key`: Now generates `ALTER TABLE ... ADD PRIMARY KEY/UNIQUE ... WITHOUT OVERLAPS` on the era's range column for primary and natural keys. Predicated unique keys still use exclusion constraints.
    -   `add_foreign_key` (Temporal): Now generates `ALTER TABLE ... ADD FOREIGN KEY ... PERIOD ...`.
4.  **Conditional Logic (Pre-PG18):**
    -   The existing logic for `add_unique_key` (exclusion constraints) and `add_regular_foreign_key` (triggers) is maintained. The trigger-based implementation for temporal-to-temporal foreign keys has been removed in favor of the native approach.
5.  **Higher-Level API:**
    -   Features like `temporal_merge`, updatable views (`for_portion_of`, `current`), and system versioning are not covered by the new native features. These would remain the core value proposition of `sql_saga`, providing a powerful, high-level API for temporal data management that builds upon the native DDL foundations.
6.  **Migration:**
    -   A migration path would be needed for existing `sql_saga` users upgrading to PostgreSQL 18. This could involve a function that adds a new `range` column, populates it from the existing `_from`/`_until` columns, and then translates `sql_saga`'s constraints into their native PostgreSQL 18 equivalents.

By embracing these native features, `sql_saga` can shed much of its most complex, low-level code and focus on providing a best-in-class user experience for advanced temporal data operations.
