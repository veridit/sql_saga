\i sql/include/test_setup.sql

BEGIN;

-- Test validation for SCD-2 incompatible schemas
\echo '--- Temporal Primary Key: Validation for SCD-2 incompatible schemas ---'

-- Scenario 1: Table with a simple, non-temporal PRIMARY KEY
CREATE TABLE incompatible_pk (
    id serial PRIMARY KEY,
    value text,
    valid_range daterange,
    valid_from date,
    valid_until date
);
-- This should issue a WARNING because of the simple PRIMARY KEY.
SELECT sql_saga.add_era('incompatible_pk','valid_range');

-- This should fail because the primary key on (id) does not include temporal columns.
DO $$
BEGIN
    PERFORM sql_saga.add_unique_key(
        table_oid => 'incompatible_pk',
        column_names => ARRAY['id'],
        key_type => 'primary'
    );
    RAISE EXCEPTION 'add_unique_key should have failed for incompatible PRIMARY KEY';
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Caught expected error: %', SQLERRM;
END;
$$;

-- Scenario 2: Table with GENERATED ALWAYS AS IDENTITY
CREATE TABLE incompatible_identity (
    id int GENERATED ALWAYS AS IDENTITY,
    value text,
    valid_range daterange,
    valid_from date,
    valid_until date
);
-- This should issue a WARNING because of the GENERATED ALWAYS column.
SELECT sql_saga.add_era('incompatible_identity','valid_range');

-- This should fail because GENERATED ALWAYS is incompatible with SCD-2 history.
DO $$
BEGIN
    PERFORM sql_saga.add_unique_key(
        table_oid => 'incompatible_identity',
        column_names => ARRAY['id'],
        key_type => 'primary'
    );
    RAISE EXCEPTION 'add_unique_key should have failed for GENERATED ALWAYS AS IDENTITY';
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Caught expected error: %', SQLERRM;
END;
$$;

ROLLBACK;

\i sql/include/test_teardown.sql
