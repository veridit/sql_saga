\i sql/include/test_setup.sql

BEGIN;

-- Replicate the scenario from statbus: a target table with a SERIAL PK
CREATE TABLE target_units (
    id SERIAL NOT NULL,
    unit_ident TEXT NOT NULL,
    name TEXT,
    valid_range daterange NOT NULL,
    valid_from DATE,
    valid_until DATE
);

SELECT sql_saga.add_era('target_units', 'valid_range', 'valid',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
-- Note: key_type=>'primary' adds a temporal PRIMARY KEY (id, valid_range WITHOUT OVERLAPS).
ALTER TABLE target_units ADD PRIMARY KEY (id, valid_range WITHOUT OVERLAPS);
SELECT sql_saga.add_unique_key('target_units', ARRAY['id'], 'valid', key_type => 'primary');
SELECT sql_saga.add_unique_key('target_units', ARRAY['unit_ident'], 'valid', key_type => 'natural');

INSERT INTO target_units (id, unit_ident, name, valid_from, valid_until) VALUES
(1, 'U1', 'Existing Unit', '2023-01-01', 'infinity');

-- To ensure the test is robust, we manually advance the sequence after an explicit ID insert.
-- This simulates a correctly managed serial column.
SELECT setval(pg_get_serial_sequence('target_units', 'id'), (SELECT max(id) FROM target_units));

-- Source table with a new unit (id is NULL)
CREATE TABLE source_units (
    row_id INT,
    founding_row_id INT,
    id INT, -- This is the stable PK. Must match target.
    unit_ident TEXT,
    name TEXT,
    valid_from DATE,
    valid_until DATE
);

INSERT INTO source_units (row_id, founding_row_id, id, unit_ident, name, valid_from, valid_until) VALUES
(1, 1, NULL, 'U2', 'New Unit', '2024-01-01', 'infinity');

\echo '## Initial state of target table'
TABLE target_units;

CALL sql_saga.temporal_merge(
    target_table => 'target_units',
    source_table => 'source_units',
    primary_identity_columns => NULL, -- Discover based on sql_saga setup.
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

ROLLBACK;

\i sql/include/test_teardown.sql
