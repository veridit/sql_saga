\i sql/include/test_setup.sql

SELECT $$
------------------------------------------------------------------------------------------
Test: Validate Timely Strategy for Temporal Operations
Concept:
  This test is a low-level validation of the core architectural assumptions that underpin
  the `temporal_merge` procedure, without using any `sql_saga` functions. It aims to prove
  that a specific sequence of DML operations (`INSERT` before `UPDATE`) within a
  transaction with deferred constraints is a valid and robust strategy for performing
  SCD Type 2 updates on tables with temporal-like foreign key and uniqueness constraints.

Assumptions to be Verified:
  1. INSERT-then-UPDATE with Deferred Constraints: A transaction can successfully perform
     an `INSERT` followed by an `UPDATE` that creates a temporary timeline overlap, as
     long as constraints are deferred. The final state is consistent, and the deferred
     `EXCLUDE` constraint will pass when checked at the end.
  2. MVCC Snapshot Visibility for Triggers: An `AFTER ROW` trigger (simulating our
     temporal FK check) fired by a DML statement will see the complete, consistent
     state of the database *after* the statement finishes, including the effects of
     prior statements within the same transaction. Specifically, the trigger on the
     `UPDATE` statement will see the row created by the preceding `INSERT` statement.

The Scenario:
  - A `parent` table has a temporal-like `EXCLUDE` constraint.
  - A `child` table has a foreign key to the `parent`.
  - An `AFTER` trigger on the `parent` table simulates the temporal foreign key check,
    ensuring no child records are ever orphaned.
  - We perform an SCD Type 2 update on a parent record, which requires shortening the
    old record and inserting a new one.
------------------------------------------------------------------------------------------
$$ as doc;

-- Use a schema to isolate the test objects
CREATE SCHEMA s;

--------------------------------------------------------------------------------
-- 1. Setup: Mimic sql_saga's temporal constraints with raw SQL
--------------------------------------------------------------------------------

-- The "referenced" table, like `legal_unit`
CREATE TABLE s.parent (
    id int NOT NULL,
    value text,
    valid_from date NOT NULL,
    valid_until date NOT NULL,
    CHECK (valid_from < valid_until),
    -- This is the equivalent of a temporal unique key. It is deferrable.
    EXCLUDE USING gist (id WITH =, daterange(valid_from, valid_until) WITH &&) DEFERRABLE INITIALLY IMMEDIATE
);
CREATE INDEX ON s.parent(id);

-- The "referencing" table, like `location`
CREATE TABLE s.child (
    id int,
    parent_id int,
    valid_from date,
    valid_until date,
    CHECK (valid_from < valid_until)
);

-- This trigger function simulates the logic of `sql_saga`'s `uk_update_check_c`
-- and `uk_delete_check_c` triggers. It ensures that any modification to a parent's
-- timeline does not leave any child records "orphaned".
CREATE FUNCTION s.check_child_coverage() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_child_exists boolean;
BEGIN
    RAISE NOTICE 'Trigger % on % fired for parent ID %', TG_OP, TG_TABLE_NAME, COALESCE(NEW.id, OLD.id);

    -- After any change to a parent, check if any child is now left without a valid parent.
    -- This query joins child records against the *current* state of the parent table.
    -- The core of the test is to see if this query correctly sees the new row inserted
    -- before the UPDATE that fires this trigger.
    SELECT EXISTS (
        SELECT 1
        FROM s.child c
        LEFT JOIN s.parent p
            ON c.parent_id = p.id
            AND daterange(c.valid_from, c.valid_until) <@ daterange(p.valid_from, p.valid_until)
        WHERE p.id IS NULL -- Find children with no covering parent
    )
    INTO v_child_exists;

    IF v_child_exists THEN
        RAISE EXCEPTION 'Foreign key violation: child record is not covered by any parent record.';
    END IF;

    -- Return value depends on the trigger event
    IF (TG_OP = 'DELETE') THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$;

-- Attach the trigger. It's an AFTER trigger, as sql_saga's are.
CREATE TRIGGER parent_fk_check
AFTER INSERT OR UPDATE OR DELETE ON s.parent
FOR EACH ROW EXECUTE FUNCTION s.check_child_coverage();


--------------------------------------------------------------------------------
-- 2. Initial State
--------------------------------------------------------------------------------

INSERT INTO s.parent VALUES (1, 'initial', '2024-01-01', 'infinity');
-- This test requires two child records to correctly simulate coverage across
-- the parent's SCD Type 2 change. The simplified trigger logic checks for
-- full containment within a *single* parent record, so we provide one child
-- for each of the parent's final time slices.
INSERT INTO s.child VALUES
    (101, 1, '2024-01-01', '2024-05-01'),
    (102, 1, '2024-05-01', 'infinity');

\echo '--- Initial State ---'
TABLE s.parent;
TABLE s.child ORDER BY id;

--------------------------------------------------------------------------------
-- 3. The Test: Perform an SCD Type 2 update using the INSERT-then-UPDATE strategy
--------------------------------------------------------------------------------
\echo '\n--- Performing SCD Type 2 Update ---\n'
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

\echo '--- Step 1: INSERT new version of parent record ---'
-- This insert creates a temporary overlap with the existing record for id=1.
-- The deferred EXCLUDE constraint allows this.
INSERT INTO s.parent VALUES (1, 'updated', '2024-05-01', 'infinity');

\echo '\n--- State after INSERT (within transaction) ---'
TABLE s.parent;

\echo '\n--- Step 2: UPDATE old version of parent record to shorten its timeline ---'
-- This update resolves the overlap.
-- The AFTER UPDATE trigger will fire here. It MUST see the result of the previous
-- INSERT statement to correctly validate that the child is still covered.
UPDATE s.parent SET valid_until = '2024-05-01' WHERE valid_from = '2024-01-01' AND id = 1;

\echo '\n--- State after UPDATE (within transaction) ---'
TABLE s.parent;

\echo '\n--- Step 3: Check deferred constraints ---'
SET CONSTRAINTS ALL IMMEDIATE;
\echo '--- Deferred constraints passed ---'

COMMIT;
\echo '\n--- Transaction committed successfully ---\n'


--------------------------------------------------------------------------------
-- 4. Verification: Final state is correct and consistent.
--------------------------------------------------------------------------------
\echo '--- Final State ---'
\echo '--- parent ---'
TABLE s.parent ORDER BY valid_from;
\echo '--- child ---'
TABLE s.child ORDER BY id;

DROP SCHEMA s CASCADE;

SELECT $$
------------------------------------------------------------------------------------------
Test: Validate Timely Strategy using sql_saga
Concept:
  This test repeats the scenario from the previous test, but uses the high-level
  sql_saga functions (`add_era`, `add_unique_key`, `add_foreign_key`) to set up
  the constraints. Its purpose is to verify that the extension's own implementation
  of temporal uniqueness and foreign keys correctly adheres to the same architectural
  principles validated by the low-level test.

  This provides an end-to-end confirmation that the `INSERT -> UPDATE` strategy is
  compatible with the triggers and constraints created by `sql_saga`.
------------------------------------------------------------------------------------------
$$ as doc;

-- Use a new schema to isolate this test
CREATE SCHEMA ss;

--------------------------------------------------------------------------------
-- 1. Setup: Use sql_saga functions
--------------------------------------------------------------------------------

CREATE TABLE ss.parent (
    id int NOT NULL,
    value text,
    valid_from date NOT NULL,
    valid_until date NOT NULL
);
CREATE TABLE ss.child (
    id int,
    parent_id int,
    valid_from date,
    valid_until date
);

-- Register the tables and constraints with sql_saga
SELECT sql_saga.add_era('ss.parent');
SELECT sql_saga.add_unique_key('ss.parent', '{id}', unique_key_name => 'parent_uk');
SELECT sql_saga.add_era('ss.child');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'ss.child',
    fk_column_names => '{parent_id}',
    fk_era_name => 'valid',
    unique_key_name => 'parent_uk'
);

--------------------------------------------------------------------------------
-- 2. Initial State (same as previous test)
--------------------------------------------------------------------------------

INSERT INTO ss.parent VALUES (1, 'initial', '2024-01-01', 'infinity');
INSERT INTO ss.child VALUES
    (101, 1, '2024-01-01', '2024-05-01'),
    (102, 1, '2024-05-01', 'infinity');

\echo '--- Initial State (sql_saga) ---'
TABLE ss.parent;
TABLE ss.child ORDER BY id;

--------------------------------------------------------------------------------
-- 3. The Test: Perform SCD Type 2 update (same as previous test)
--------------------------------------------------------------------------------
\echo '\n--- Performing SCD Type 2 Update (sql_saga) ---\n'
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

\echo '--- Step 1: INSERT new version ---'
INSERT INTO ss.parent VALUES (1, 'updated', '2024-05-01', 'infinity');

\echo '\n--- State after INSERT (within transaction) ---'
TABLE ss.parent;

\echo '\n--- Step 2: UPDATE old version ---'
UPDATE ss.parent SET valid_until = '2024-05-01' WHERE valid_from = '2024-01-01' AND id = 1;

\echo '\n--- State after UPDATE (within transaction) ---'
TABLE ss.parent;

\echo '\n--- Step 3: Check deferred constraints ---'
SET CONSTRAINTS ALL IMMEDIATE;
\echo '--- Deferred constraints passed ---'

COMMIT;
\echo '\n--- Transaction committed successfully ---\n'


--------------------------------------------------------------------------------
-- 4. Verification: Final state is correct and consistent.
--------------------------------------------------------------------------------
\echo '--- Final State (sql_saga) ---'
\echo '--- parent ---'
TABLE ss.parent ORDER BY valid_from;
\echo '--- child ---'
TABLE ss.child ORDER BY id;

DROP SCHEMA ss CASCADE;

\i sql/include/test_teardown.sql
