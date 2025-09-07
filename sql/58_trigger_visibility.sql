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
    id int primary key,
    group_id int not null,
    value text,
    valid_from int,
    valid_until int,
    EXCLUDE USING gist (group_id WITH =, int4range(valid_from, valid_until) WITH &&) DEFERRABLE
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
    FOR v_row IN SELECT * FROM mvcc_test.t ORDER BY id LOOP
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
-- group_id=1 has a continuous timeline. group_id=2 is a separate entity.
INSERT INTO mvcc_test.t VALUES (1, 1, 'initial', 10, 20), (2, 1, 'untouched', 20, 30);
TABLE mvcc_test.t ORDER BY id;

\echo '\n\n--- Scenario 1: INSERT a new row ---'
BEGIN;
INSERT INTO mvcc_test.t VALUES (3, 2, 'inserted', 100, 110);
COMMIT;

\echo '\n\n--- Scenario 2: UPDATE an existing row ---'
BEGIN;
UPDATE mvcc_test.t SET value = 'updated' WHERE id = 1;
COMMIT;

\echo '\n\n--- Scenario 3: DELETE an existing row ---'
BEGIN;
DELETE FROM mvcc_test.t WHERE id = 1;
COMMIT;

\echo '\n\n--- Scenario 4: Multi-row UPDATE ---'
\echo 'Observation: Each row''s AFTER trigger sees the state *after its own update* is complete.'
\echo 'The snapshot for the second row''s triggers includes the update from the first row.'
BEGIN;
-- Add a row to update
INSERT INTO mvcc_test.t VALUES (4, 2, 'another', 110, 120);
UPDATE mvcc_test.t SET value = 'multi-updated' WHERE id IN (2, 4);
COMMIT;


\echo '\n\n--- Scenario 5: Two statements in one transaction ---'
\echo 'Observation: The triggers for the second UPDATE statement see the table state'
\echo 'that includes the completed update from the first statement.'
BEGIN;
\echo '-- First statement in transaction --'
UPDATE mvcc_test.t SET value = 'tx-update-1' WHERE id = 2;
\echo '-- Second statement in transaction --'
UPDATE mvcc_test.t SET value = 'tx-update-2' WHERE id = 3;
COMMIT;


\echo '\n\n--- Scenario 6: Adjusting Adjacent Timelines (SCD Type 2 simulation) ---'
\echo 'Observation: This scenario demonstrates why the order of operations is critical'
\echo 'for temporal data. A "grow" operation (extending a period) must happen before a'
\echo '"shrink" operation to avoid creating a transient gap that would cause a temporal'
\echo 'foreign key check to fail.'
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
-- Setup: two adjacent timeline segments for group_id=3
INSERT INTO mvcc_test.t VALUES (5, 3, 'a', 100, 200), (6, 3, 'b', 200, 300);

\echo '\n-- 6a: Shrink then Grow (Incorrect Order) --'
\echo '-- We update id=5 to end at 150. The AFTER trigger for this statement sees a'
\echo '-- gap from 150 to 200. A temporal FK check would fail here.'
SAVEPOINT shrink_grow;
UPDATE mvcc_test.t SET valid_until = 150 WHERE id = 5;
\echo '\n-- We update id=6 to start at 150, closing the gap.'
UPDATE mvcc_test.t SET valid_from = 150 WHERE id = 6;
ROLLBACK TO SAVEPOINT shrink_grow;

\echo '\n-- 6b: Grow then Shrink (Correct Order) --'
\echo '-- We update id=6 to start at 150. This creates a temporary overlap from 150 to 200.'
\echo '-- The deferred EXCLUDE constraint allows this. An FK check here would pass.'
SAVEPOINT pre_6b_and_6c;
UPDATE mvcc_test.t SET valid_from = 150 WHERE id = 6;
\echo '\n-- We update id=5 to end at 150, resolving the overlap. The AFTER trigger'
\echo '-- for this statement sees a continuous, valid timeline.'
UPDATE mvcc_test.t SET valid_until = 150 WHERE id = 5;
ROLLBACK TO pre_6b_and_6c;

\echo '\n-- 6c: Ordered Multi-row UPDATE (Correct Order) --'
\echo '-- We perform both updates in a single statement, using a FROM clause'
\echo '-- with ORDER BY to force the "grow" operation to happen first.'
UPDATE mvcc_test.t
SET
    valid_from = u.new_valid_from,
    valid_until = u.new_valid_until
FROM (
    SELECT
        id,
        CASE id WHEN 6 THEN 150 ELSE valid_from END AS new_valid_from,
        CASE id WHEN 5 THEN 150 ELSE valid_until END AS new_valid_until,
        -- Order growths (1) before shrinks (2)
        CASE id WHEN 6 THEN 1 ELSE 2 END AS op_order
    FROM mvcc_test.t
    WHERE id IN (5, 6)
    ORDER BY op_order
) AS u
WHERE mvcc_test.t.id = u.id;

COMMIT;


\echo '\n--- Final State ---'
TABLE mvcc_test.t ORDER BY id;

DROP SCHEMA mvcc_test CASCADE;

\i sql/include/test_teardown.sql
