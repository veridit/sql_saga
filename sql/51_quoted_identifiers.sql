\i sql/include/test_setup.sql

BEGIN;

--
-- Test for handling of quoted identifiers (table, column, constraint names)
-- including spaces and quotes within the names.
--
-- PostgreSQL Identifier Quoting Rules:
--
-- This test verifies that sql_saga API functions correctly handle complex
-- identifier quoting. It's important to distinguish between how identifiers
-- are written in DDL (Identifier Syntax, for `%I`) and how their names are
-- passed as string literals to functions (Literal Syntax, for `%L`),
-- especially for functions that take `regclass` arguments.
--
-- 1. **Identifier Syntax (`%I` format):** Used for database object names in DDL.
--    - To handle spaces or case-sensitivity, use double quotes: `"My Table"`.
--    - To include a double quote inside, escape it with another: `"""my "" table"""`.
--    - The *actual name* of the table created by `"""my "" table"""` is `"my " table"`.
--
-- 2. **Literal Syntax (`%L` format):** Used for string values passed to functions.
--    - The value is enclosed in single quotes: `'a string'`.
--
-- How to call sql_saga functions with complex names:
--
--  - **For `regclass` arguments (e.g., table_oid):**
--    You must pass a string literal that contains the full *Identifier Syntax*
--    used in the DDL. PostgreSQL's `regclass` type resolver uses this string
--    to look up the object.
--      - DDL: `CREATE TABLE """my "" table"""`
--      - Function Call: `SELECT ...('"""my "" table"""', ...)`
--      - The cast `'"""my "" table"""'::regclass` succeeds, but `'"my " table"'::regclass` fails.
--
--  - **For `text`, `name`, or `text[]` arguments (e.g., column/constraint names):**
--    You must pass the *actual name* of the object as a string literal.
--      - Column DDL: `"""col "" 1""" TEXT`
--      - Actual Name: `"col " 1"`
--      - Function Call: `..., ARRAY['"col " 1"'], ...`
--
SET ROLE TO sql_saga_unprivileged_user;

-- 1. Table with a name containing quotes and spaces
CREATE TABLE """quoted"" table" (
    """id""" BIGINT,
    """product "" code""" TEXT,
    """valid from""" INTEGER,
    """valid until""" INTEGER
);

-- 2. Add era with quoted columns
SELECT sql_saga.add_era('"""quoted"" table"', '"valid from"', '"valid until"');
TABLE sql_saga.era;

-- 3. Add unique key with quoted columns and constraint name
SELECT sql_saga.add_unique_key(
    table_oid => '"""quoted"" table"',
    column_names => ARRAY['"id"', '"product " code"'],
    era_name => 'valid',
    key_type => 'natural',
    unique_key_name => '"my " uk"'
);
TABLE sql_saga.unique_keys;

-- 4. Create referencing table with quoted names
CREATE TABLE """fk "" test""" (
    """ref "" id""" BIGINT,
    """product""" TEXT,
    """from""" INTEGER,
    """until""" INTEGER
);

SELECT sql_saga.add_era('"""fk "" test"""', '"from"', '"until"');

-- 5. Add foreign key referencing the complex unique key
SELECT sql_saga.add_foreign_key(
    fk_table_oid => '"""fk "" test"""',
    fk_column_names => ARRAY['"ref " id"', '"product"'],
    fk_era_name => 'valid',
    unique_key_name => '"my " uk"'
);
TABLE sql_saga.foreign_keys;

-- 6. Test data insertion
-- This should succeed
INSERT INTO """quoted"" table" VALUES (1, 'A', 10, 20);
INSERT INTO """fk "" test""" VALUES (1, 'A', 12, 18);

-- Show table contents before the expected failure
TABLE """quoted"" table";
TABLE """fk "" test""";

-- This should fail (FK violation). Use a savepoint to contain the error.
SAVEPOINT expect_fail;
INSERT INTO """fk "" test""" VALUES (2, 'B', 15, 25);
ROLLBACK TO SAVEPOINT expect_fail;

ROLLBACK;

\i sql/include/test_teardown.sql
