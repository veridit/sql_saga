\i sql/include/test_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

CREATE TABLE legal_unit (
  id INTEGER,
  valid_range daterange NOT NULL,
  valid_from date,
  valid_until date,
  name varchar NOT NULL
);

CREATE TABLE location (
  id INTEGER,
  valid_range daterange NOT NULL,
  valid_from date,
  valid_until date,
  legal_unit_id INTEGER NOT NULL,
  postal_place TEXT NOT NULL
);

-- Before using sql_saga
\d legal_unit
\d location

-- Verify that enable and disable each work correctly.
SELECT sql_saga.add_era('legal_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
ALTER TABLE legal_unit ADD PRIMARY KEY (id, valid_range WITHOUT OVERLAPS);
SELECT sql_saga.add_era('location', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
ALTER TABLE location ADD PRIMARY KEY (id, valid_range WITHOUT OVERLAPS);
TABLE sql_saga.era;

SELECT sql_saga.add_unique_key('legal_unit', ARRAY['id'], 'valid');
SELECT sql_saga.add_unique_key('location', ARRAY['id'], 'valid');
TABLE sql_saga.unique_keys;

SELECT sql_saga.add_temporal_foreign_key('location', ARRAY['legal_unit_id'], 'valid', 'legal_unit_id_valid');
TABLE sql_saga.foreign_keys;

-- While sql_saga is active
\d legal_unit
\d location

-- Initial Import
INSERT INTO legal_unit (id, valid_from, valid_until, name) VALUES
(101, '2015-01-01', 'infinity', 'NANSETKRYSSET AS');

INSERT INTO location (id, valid_from, valid_until, legal_unit_id, postal_place) VALUES
(201, '2015-01-01', 'infinity',101 , 'DRAMMEN');

TABLE legal_unit;
TABLE location;

-- Can't delete referenced legal_Init
DELETE FROM legal_unit WHERE id = 101;

-- Can't shorten referenced legal_unit more than the referencing location
UPDATE legal_unit SET valid_until = '2016-01-01' WHERE id = 101;

-- With deferred constraints, adjust the data
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

UPDATE legal_unit SET valid_until = '2016-01-01'
WHERE id = 101
  AND valid_from = '2015-01-01'
  AND valid_until = 'infinity';

INSERT INTO legal_unit (id, valid_from, valid_until, name) VALUES
(101, '2016-01-01', 'infinity', 'NANSETVEIEN AS');

TABLE legal_unit;
TABLE location;

SET CONSTRAINTS ALL IMMEDIATE;
COMMIT;

TABLE legal_unit;
TABLE location;

BEGIN;
SET CONSTRAINTS ALL DEFERRED;
-- The algorithm for *_era changes in stabus will delete the old row
-- and make a new one that replaces it, if they are adjacent.
DELETE FROM location
WHERE id = 201
  AND valid_from = '2015-01-01'
  AND valid_until = 'infinity';

INSERT INTO location (id, valid_from, valid_until, legal_unit_id, postal_place) VALUES
(201, '2015-01-01', 'infinity',101 , 'DRAMMEN');

SET CONSTRAINTS ALL IMMEDIATE;

COMMIT;

TABLE legal_unit;
TABLE location;

-- Teardown

SELECT sql_saga.drop_foreign_key('location', ARRAY['legal_unit_id'], 'valid');
TABLE sql_saga.foreign_keys;

SELECT sql_saga.drop_unique_key('legal_unit', ARRAY['id'], 'valid');
SELECT sql_saga.drop_unique_key('location',ARRAY['id'], 'valid');
TABLE sql_saga.unique_keys;

SELECT sql_saga.drop_era('legal_unit');
SELECT sql_saga.drop_era('location');
TABLE sql_saga.era;

-- After removing sql_saga, it should be as before.
\d legal_unit
\d location

DROP TABLE legal_unit;
DROP TABLE location;

\i sql/include/test_teardown.sql
