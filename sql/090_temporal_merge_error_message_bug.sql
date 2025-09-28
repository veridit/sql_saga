\i sql/include/test_setup.sql

BEGIN;

SAVEPOINT scenario_1;

-- This test verifies that the planner correctly raises an error when a source table
-- is missing a valid_until/valid_to column.

CREATE TABLE target_units (
    id SERIAL NOT NULL,
    name TEXT,
    valid_from DATE NOT NULL,
    valid_until DATE,
    valid_to DATE
);

-- Note: we add a synchronized 'valid_to' column to make the expected error clear.
SELECT sql_saga.add_era('target_units', synchronize_valid_to_column => 'valid_to');
SELECT sql_saga.add_unique_key('target_units', ARRAY['id'], key_type => 'primary');


-- Source table is intentionally missing a temporal end column ('valid_until' or 'valid_to')
CREATE TABLE source_units (
    row_id INT,
    id INT,
    name TEXT,
    valid_from DATE
);

INSERT INTO target_units (id, name, valid_from, valid_to) VALUES (1, 'Existing Unit', '2023-01-01', 'infinity');
INSERT INTO source_units (row_id, id, name, valid_from) VALUES (1, 1, 'Updated Unit', '2024-01-01');

\echo '## Calling temporal_merge without valid_until in source should fail'
-- This call should fail because the source table lacks valid_until or valid_to.
CALL sql_saga.temporal_merge(
    target_table => 'target_units',
    source_table => 'source_units',
    row_id_column => 'row_id'
);

\echo '## State of target table after merge (should be unchanged)'
-- This will also error because the transaction is aborted.
TABLE target_units ORDER BY id, valid_from;

ROLLBACK TO SAVEPOINT scenario_1;

SAVEPOINT scenario_2;
-- Replicate the scenario from statbus: a target table with a SERIAL PK
CREATE TABLE target_units (
    id SERIAL NOT NULL,
    unit_ident TEXT NOT NULL,
    name TEXT,
    valid_from DATE NOT NULL,
    valid_until DATE
);

SELECT sql_saga.add_era('target_units');
-- Note: key_type=>'primary' adds a temporal PRIMARY KEY (id, valid_from) and an exclusion constraint.
SELECT sql_saga.add_unique_key('target_units', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_unique_key('target_units', ARRAY['unit_ident'], key_type => 'natural');

INSERT INTO target_units (id, unit_ident, name, valid_from, valid_until) VALUES
(1, 'U1', 'Existing Unit', '2023-01-01', 'infinity');

-- Source table with a new unit (id is NULL)
CREATE TABLE source_units (
    row_id INT,
    founding_row_id INT,
    id INT, -- This is the stable PK. Must match target.
    unit_ident TEXT,
    name TEXT,
    valid_from DATE
);

INSERT INTO source_units (row_id, founding_row_id, id, unit_ident, name, valid_from) VALUES
(1, 1, NULL, 'U2', 'New Unit', '2024-01-01');

\echo '## Initial state of target table'
TABLE target_units;

SET client_min_messages TO NOTICE;
SET sql_saga.temporal_merge.log_plan = true;
SET sql_saga.temporal_merge.enable_trace = true;

CALL sql_saga.temporal_merge(
    target_table => 'target_units',
    source_table => 'source_units',
    identity_columns => NULL, -- Discover based on sql_saga setup.
    natural_identity_columns => NULL, -- Discover based on sql_saga setup.
    row_id_column => 'row_id',
    founding_id_column => 'founding_row_id',
    update_source_with_identity => true,
    mode => 'MERGE_ENTITY_UPSERT'
);

\echo '## State of target table after merge'
TABLE target_units ORDER BY id, valid_from;

\echo '## State of source table after identity back-fill'
TABLE source_units ORDER BY row_id;

ROLLBACK TO SAVEPOINT scenario_2;

ROLLBACK;

\i sql/include/test_teardown.sql
