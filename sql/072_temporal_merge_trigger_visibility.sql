\i sql/include/test_setup.sql

SELECT $$
--------------------------------------------------------------------------------
Test: 58 - MVCC Trigger Visibility Semantics
Purpose:
  This test provides a crystal-clear demonstration of the Multi-Version
  Concurrency Control (MVCC) semantics for PostgreSQL triggers. Its goal is
  not to test any sql_saga functionality, but to establish a baseline of
  understanding about what data is visible to BEFORE and AFTER triggers for
  each DML operation (INSERT, UPDATE, DELETE).

Methodology:
  1. A simple table `mvcc_test.t` is created.
  2. A single, universal trigger function `mvcc_test.log_trigger()` is defined.
     This function logs:
     - The trigger's context (WHEN, OP).
     - The contents of the OLD and NEW tuples.
     - The *full contents of the table* as seen by a `SELECT *` query
       executed from within the trigger function. This is the crucial part
       that reveals the MVCC snapshot available to the trigger.
  3. This function is attached as both a BEFORE and AFTER ROW trigger to the
     table.
  4. A series of simple, isolated DML statements are executed, and the
     resulting NOTICE output is captured to demonstrate the visibility rules.

Expected Observations:
  - BEFORE Triggers: The `SELECT *` will show the state of the table
    *before* the current DML statement has made any changes. The OLD/NEW
    tuples reflect the change that is about to happen.
  - AFTER Triggers: The `SELECT *` will show the state of the table *after*
    the current DML statement has completed. The snapshot includes the
    change made by the statement that fired the trigger.
--------------------------------------------------------------------------------
$$ AS doc;

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE SCHEMA mvcc_test;
CREATE TABLE mvcc_test.t (
    row_id int GENERATED ALWAYS AS IDENTITY,
    id int not null,
    value text,
    valid_range int4range NOT NULL,
    PRIMARY KEY (row_id),
    UNIQUE (id, valid_range WITHOUT OVERLAPS)
);

CREATE FUNCTION mvcc_test.log_trigger()
RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_row record;
BEGIN
    RAISE NOTICE '---';
    RAISE NOTICE 'Trigger Fired: % % ON mvcc_test.t', TG_WHEN, TG_OP;

    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        RAISE NOTICE '  OLD: %', to_jsonb(OLD);
    END IF;
    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        RAISE NOTICE '  NEW: %', to_jsonb(NEW);
    END IF;

    RAISE NOTICE '  Visible Table State (SELECT * FROM mvcc_test.t):';
    FOR v_row IN SELECT * FROM mvcc_test.t ORDER BY row_id LOOP
        RAISE NOTICE '    %', v_row;
    END LOOP;

    IF TG_WHEN = 'BEFORE' THEN
        RETURN NEW;
    ELSE
        RETURN OLD;
    END IF;
END;
$$;

CREATE TRIGGER t_before
BEFORE INSERT OR UPDATE OR DELETE ON mvcc_test.t
FOR EACH ROW EXECUTE FUNCTION mvcc_test.log_trigger();

CREATE TRIGGER t_after
AFTER INSERT OR UPDATE OR DELETE ON mvcc_test.t
FOR EACH ROW EXECUTE FUNCTION mvcc_test.log_trigger();

\echo '\n--- Initial State ---'
-- id=1 has a continuous timeline. id=2 is a separate entity.
INSERT INTO mvcc_test.t (id, value, valid_range) 
VALUES (1, 'initial', '[10,20)'), (1, 'untouched', '[20,30)');
TABLE mvcc_test.t ORDER BY row_id;

\echo '\n\n--- Scenario 1: INSERT a new row ---'
BEGIN;
INSERT INTO mvcc_test.t (id, value, valid_range) 
VALUES (2, 'inserted', '[100,110)');
COMMIT;

\echo '\n\n--- Scenario 2: UPDATE an existing row ---'
BEGIN;
UPDATE mvcc_test.t SET value = 'updated' WHERE row_id = 1;
COMMIT;

\echo '\n\n--- Scenario 3: DELETE an existing row ---'
BEGIN;
DELETE FROM mvcc_test.t WHERE row_id = 1;
COMMIT;

\echo '\n\n--- Scenario 4: Multi-row UPDATE ---'
\echo 'Observation: Each row''s AFTER trigger sees the state *after its own update* is complete.'
\echo 'The snapshot for the second row''s triggers includes the update from the first row.'
BEGIN;
-- Add a row to update
INSERT INTO mvcc_test.t (id, value, valid_range) 
VALUES (2, 'another', '[110,120)');
UPDATE mvcc_test.t SET value = 'multi-updated' WHERE row_id IN (2, 4);
COMMIT;


\echo '\n\n--- Scenario 5: Two statements in one transaction ---'
\echo 'Observation: The triggers for the second UPDATE statement see the table state'
\echo 'that includes the completed update from the first statement.'
BEGIN;
\echo '-- First statement in transaction --'
UPDATE mvcc_test.t SET value = 'tx-update-1' WHERE row_id = 2;
\echo '-- Second statement in transaction --'
UPDATE mvcc_test.t SET value = 'tx-update-2' WHERE row_id = 3;
COMMIT;


\echo '\n\n--- Scenario 6: Adjusting Adjacent Timelines (SCD Type 2 simulation) ---'
\echo 'Observation: This scenario demonstrates why the order of operations is critical'
\echo 'for temporal data. A "shrink" operation must happen before a "grow" operation'
\echo 'to avoid creating a transient overlap that would violate NOT DEFERRABLE unique.'
\echo 'With DELETE→UPDATE→INSERT strategy, we minimize gaps instead of overlaps.'
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
-- Setup: two adjacent timeline segments for id=3
INSERT INTO mvcc_test.t (id, value, valid_range) 
VALUES (3, 'a', '[100,200)'), (3, 'b', '[200,300)');

\echo '\n-- 6a: Grow then Shrink (Incorrect Order with NOT DEFERRABLE unique) --'
\echo '-- We update row_id=6 to start at 150. This creates a temporary overlap from 150 to 200.'
\echo '-- The NOT DEFERRABLE unique constraint rejects this immediately.'
SAVEPOINT grow_shrink;
UPDATE mvcc_test.t SET valid_range = '[150,300)' WHERE row_id = 6;
SELECT 'UNEXPECTED: Overlap was allowed!' AS error;
ROLLBACK TO SAVEPOINT grow_shrink;

\echo '\n-- 6b: Shrink then Grow (Correct Order with NOT DEFERRABLE unique) --'
\echo '-- We update row_id=5 to end at 150. The AFTER trigger for this statement sees a'
\echo '-- gap from 150 to 200. A DEFERRABLE temporal FK check tolerates this.'
SAVEPOINT pre_6b_and_6c;
UPDATE mvcc_test.t SET valid_range = '[100,150)' WHERE row_id = 5;
\echo '\n-- We update row_id=6 to start at 150, closing the gap. The AFTER trigger'
\echo '-- for this statement sees a continuous, valid timeline.'
UPDATE mvcc_test.t SET valid_range = '[150,300)' WHERE row_id = 6;
ROLLBACK TO pre_6b_and_6c;

\echo '\n-- 6c: Ordered Multi-row UPDATE (Correct Order) --'
\echo '-- We perform both updates in a single statement, using a FROM clause'
\echo '-- with ORDER BY to force the "shrink" operation to happen first.'
UPDATE mvcc_test.t
SET valid_range = u.new_valid_range
FROM (
    SELECT
        row_id,
        CASE row_id WHEN 5 THEN '[100,150)'::int4range WHEN 6 THEN '[150,300)'::int4range END AS new_valid_range,
        -- Order shrinks (1) before growths (2) to avoid overlaps
        CASE row_id WHEN 5 THEN 1 ELSE 2 END AS op_order
    FROM mvcc_test.t
    WHERE row_id IN (5, 6)
    ORDER BY op_order
) AS u
WHERE mvcc_test.t.row_id = u.row_id;

COMMIT;


\echo '\n--- Final State ---'
TABLE mvcc_test.t ORDER BY row_id;

DROP SCHEMA mvcc_test CASCADE;

\i sql/include/test_teardown.sql
