\i sql/include/test_setup.sql

-- This must be done as a superuser before setting the role.
\i sql/include/benchmark_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

-- Enable detailed temporal_merge index logging for this benchmark session.
--SET sql_saga.temporal_merge.log_index_checks = true;

CREATE TABLE legal_unit (id INTEGER, valid_from date, valid_until date, name varchar NOT NULL, PRIMARY KEY (id, valid_from));
CREATE TABLE establishment (id INTEGER, valid_from date, valid_until date, legal_unit_id INTEGER NOT NULL, postal_place TEXT NOT NULL, PRIMARY KEY (id, valid_from));
CREATE TABLE projects (id serial primary key, name text, legal_unit_id int);

\echo '--- Populating tables ---'
INSERT INTO legal_unit SELECT i, '2015-01-01', 'infinity', 'Company ' || i FROM generate_series(1, 20000) i;
-- Establishments only for the first 10,000 legal units
INSERT INTO establishment SELECT i, '2015-01-01', 'infinity', i, 'Shop ' || i FROM generate_series(1, 10000) i;
INSERT INTO projects (name, legal_unit_id) SELECT 'Project ' || i, i FROM generate_series(1, 10000) i;
ANALYZE legal_unit;
ANALYZE establishment;
ANALYZE projects;

--------------------------------------------------------------------------------
\echo '--- DML on Plain Tables (No sql_saga constraints) ---'
--------------------------------------------------------------------------------

-- Parent DML
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent INSERT (Plain)', 0, false);
CALL sql_saga.benchmark_reset();
INSERT INTO legal_unit SELECT i, '2015-01-01', 'infinity', 'New Company ' || i FROM generate_series(20001, 21000) i;
CALL sql_saga.benchmark_log_and_reset('Parent INSERT (Plain)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent INSERT (Plain) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent UPDATE Key (Plain)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE legal_unit SET valid_from = '2014-01-01' WHERE id BETWEEN 1 AND 1000;
CALL sql_saga.benchmark_log_and_reset('Parent UPDATE Key (Plain)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent UPDATE Key (Plain) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent UPDATE Non-Key (Plain)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE legal_unit SET name = 'Updated Company' WHERE id BETWEEN 1001 AND 2000;
CALL sql_saga.benchmark_log_and_reset('Parent UPDATE Non-Key (Plain)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent UPDATE Non-Key (Plain) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent DELETE (Plain)', 0, false);
CALL sql_saga.benchmark_reset();
DELETE FROM legal_unit WHERE id BETWEEN 15001 AND 16000;
CALL sql_saga.benchmark_log_and_reset('Parent DELETE (Plain)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent DELETE (Plain) end', 1000, true);

-- Child DML
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child INSERT (Plain)', 0, false);
CALL sql_saga.benchmark_reset();
INSERT INTO establishment SELECT i, '2015-01-01', 'infinity', i, 'New Shop ' || i FROM generate_series(10001, 11000) i;
CALL sql_saga.benchmark_log_and_reset('Child INSERT (Plain)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child INSERT (Plain) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child UPDATE Key (Plain)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE establishment SET valid_from = '2014-01-01' WHERE id BETWEEN 1 AND 1000;
CALL sql_saga.benchmark_log_and_reset('Child UPDATE Key (Plain)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child UPDATE Key (Plain) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child UPDATE Non-Key (Plain)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE establishment SET postal_place = 'New Place' WHERE id BETWEEN 1001 AND 2000;
CALL sql_saga.benchmark_log_and_reset('Child UPDATE Non-Key (Plain)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child UPDATE Non-Key (Plain) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child DELETE (Plain)', 0, false);
CALL sql_saga.benchmark_reset();
DELETE FROM establishment WHERE id BETWEEN 9001 AND 10000;
CALL sql_saga.benchmark_log_and_reset('Child DELETE (Plain)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child DELETE (Plain) end', 1000, true);

-- Regular Table DML
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular INSERT (Plain)', 0, false);
CALL sql_saga.benchmark_reset();
INSERT INTO projects (name, legal_unit_id) SELECT 'New Project ' || i, i FROM generate_series(10001, 11000) i;
CALL sql_saga.benchmark_log_and_reset('Regular INSERT (Plain)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular INSERT (Plain) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular UPDATE (Plain)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE projects SET name = 'Updated Project' WHERE id BETWEEN 1 AND 1000;
CALL sql_saga.benchmark_log_and_reset('Regular UPDATE (Plain)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular UPDATE (Plain) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular DELETE (Plain)', 0, false);
CALL sql_saga.benchmark_reset();
DELETE FROM projects WHERE id BETWEEN 9001 AND 10000;
CALL sql_saga.benchmark_log_and_reset('Regular DELETE (Plain)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular DELETE (Plain) end', 1000, true);


\echo '--- Resetting tables and enabling sql_saga constraints ---'
TRUNCATE legal_unit, establishment, projects;
INSERT INTO legal_unit SELECT i, '2015-01-01', 'infinity', 'Company ' || i FROM generate_series(1, 20000) i;
INSERT INTO establishment SELECT i, '2015-01-01', 'infinity', i, 'Shop ' || i FROM generate_series(1, 10000) i;
INSERT INTO projects (name, legal_unit_id) SELECT 'Project ' || i, i FROM generate_series(1, 10000) i;
ANALYZE legal_unit;
ANALYZE establishment;
ANALYZE projects;

-- Enable sql_saga constraints
SELECT sql_saga.add_era('legal_unit'::regclass);
SELECT sql_saga.add_era('establishment'::regclass);
SELECT sql_saga.add_unique_key('legal_unit'::regclass, column_names => ARRAY['id'], unique_key_name => 'legal_unit_id_valid');
SELECT sql_saga.add_unique_key('establishment'::regclass, column_names => ARRAY['id']);
SELECT sql_saga.add_foreign_key('establishment'::regclass, ARRAY['legal_unit_id'], 'legal_unit'::regclass, ARRAY['id'], fk_era_name => 'valid', create_index => false);
SELECT sql_saga.add_foreign_key('projects', ARRAY['legal_unit_id'], 'legal_unit', ARRAY['id']);

--------------------------------------------------------------------------------
\echo '--- DML on sql_saga Tables (No Performance Index) ---'
--------------------------------------------------------------------------------

-- Parent DML (fires uk_*_triggers)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent INSERT (No Index)', 0, false);
CALL sql_saga.benchmark_reset();
INSERT INTO legal_unit SELECT i, '2015-01-01', 'infinity', 'New Company ' || i FROM generate_series(20001, 21000) i;
CALL sql_saga.benchmark_log_and_reset('Parent INSERT (No Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent INSERT (No Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent UPDATE Key (No Index)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE legal_unit SET valid_from = '2014-01-01' WHERE id BETWEEN 1 AND 1000;
CALL sql_saga.benchmark_log_and_reset('Parent UPDATE Key (No Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent UPDATE Key (No Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent UPDATE Non-Key (No Index)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE legal_unit SET name = 'Updated Company' WHERE id BETWEEN 1001 AND 2000;
CALL sql_saga.benchmark_log_and_reset('Parent UPDATE Non-Key (No Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent UPDATE Non-Key (No Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent DELETE (No Index)', 0, false);
CALL sql_saga.benchmark_reset();
DELETE FROM legal_unit WHERE id BETWEEN 15001 AND 16000;
CALL sql_saga.benchmark_log_and_reset('Parent DELETE (No Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent DELETE (No Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent DELETE (Check Only, No Index)', 0, false);
-- Delete a batch of parent rows that have no children; exercises FK check path without violations.
CALL sql_saga.benchmark_reset();
DELETE FROM legal_unit WHERE id BETWEEN 20001 AND 21000;
CALL sql_saga.benchmark_log_and_reset('Parent DELETE (Check Only, No Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent DELETE (Check Only, No Index) end', 1000, true);

-- Child DML (fires fk_*_triggers)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child INSERT (No Index)', 0, false);
CALL sql_saga.benchmark_reset();
INSERT INTO establishment SELECT i, '2015-01-01', 'infinity', i, 'New Shop ' || i FROM generate_series(10001, 11000) i;
CALL sql_saga.benchmark_log_and_reset('Child INSERT (No Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child INSERT (No Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child UPDATE Key (No Index)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE establishment SET valid_from = '2014-01-01' WHERE id BETWEEN 1 AND 1000;
CALL sql_saga.benchmark_log_and_reset('Child UPDATE Key (No Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child UPDATE Key (No Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child UPDATE Non-Key (No Index)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE establishment SET postal_place = 'New Place' WHERE id BETWEEN 1001 AND 2000;
CALL sql_saga.benchmark_log_and_reset('Child UPDATE Non-Key (No Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child UPDATE Non-Key (No Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child DELETE (No Index)', 0, false);
CALL sql_saga.benchmark_reset();
DELETE FROM establishment WHERE id BETWEEN 9001 AND 10000;
CALL sql_saga.benchmark_log_and_reset('Child DELETE (No Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child DELETE (No Index) end', 1000, true);

-- Regular Table DML (fires CHECK constraint)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular INSERT (No Index)', 0, false);
CALL sql_saga.benchmark_reset();
INSERT INTO projects (name, legal_unit_id) SELECT 'New Project ' || i, i FROM generate_series(10001, 11000) i;
CALL sql_saga.benchmark_log_and_reset('Regular INSERT (No Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular INSERT (No Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular UPDATE (No Index)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE projects SET legal_unit_id = id + 1 WHERE id BETWEEN 1 AND 1000;
CALL sql_saga.benchmark_log_and_reset('Regular UPDATE (No Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular UPDATE (No Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular DELETE (No Index)', 0, false);
CALL sql_saga.benchmark_reset();
DELETE FROM projects WHERE id BETWEEN 9001 AND 10000;
CALL sql_saga.benchmark_log_and_reset('Regular DELETE (No Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular DELETE (No Index) end', 1000, true);


\echo '--- Adding performance index to child table ---'
CREATE INDEX ON establishment USING GIST (legal_unit_id, daterange(valid_from, valid_until));

--------------------------------------------------------------------------------
\echo '--- DML on sql_saga Tables (With Performance Index) ---'
--------------------------------------------------------------------------------

-- Parent DML (fires uk_*_triggers, now with index)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent INSERT (With Index)', 0, false);
CALL sql_saga.benchmark_reset();
INSERT INTO legal_unit SELECT i, '2015-01-01', 'infinity', 'New Company ' || i FROM generate_series(21001, 22000) i;
CALL sql_saga.benchmark_log_and_reset('Parent INSERT (With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent INSERT (With Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent UPDATE Key (With Index)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE legal_unit SET valid_from = '2013-01-01' WHERE id BETWEEN 2001 AND 3000;
CALL sql_saga.benchmark_log_and_reset('Parent UPDATE Key (With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent UPDATE Key (With Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent UPDATE Non-Key (With Index)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE legal_unit SET name = 'Updated Again Company' WHERE id BETWEEN 3001 AND 4000;
CALL sql_saga.benchmark_log_and_reset('Parent UPDATE Non-Key (With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent UPDATE Non-Key (With Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent DELETE (With Index)', 0, false);
CALL sql_saga.benchmark_reset();
DELETE FROM legal_unit WHERE id BETWEEN 16001 AND 17000;
CALL sql_saga.benchmark_log_and_reset('Parent DELETE (With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent DELETE (With Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent DELETE (Check Only, With Index)', 0, false);
-- Delete a batch of parent rows that have no children; exercises FK check path without violations.
CALL sql_saga.benchmark_reset();
DELETE FROM legal_unit WHERE id BETWEEN 21001 AND 22000;
CALL sql_saga.benchmark_log_and_reset('Parent DELETE (Check Only, With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Parent DELETE (Check Only, With Index) end', 1000, true);

-- Child DML (fires fk_*_triggers, should be unaffected by index on parent)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child INSERT (With Index)', 0, false);
CALL sql_saga.benchmark_reset();
INSERT INTO establishment SELECT i, '2015-01-01', 'infinity', i, 'New Shop ' || i FROM generate_series(11001, 12000) i;
CALL sql_saga.benchmark_log_and_reset('Child INSERT (With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child INSERT (With Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child UPDATE Key (With Index)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE establishment SET valid_from = '2013-01-01' WHERE id BETWEEN 2001 AND 3000;
CALL sql_saga.benchmark_log_and_reset('Child UPDATE Key (With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child UPDATE Key (With Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child UPDATE Non-Key (With Index)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE establishment SET postal_place = 'New Place Again' WHERE id BETWEEN 3001 AND 4000;
CALL sql_saga.benchmark_log_and_reset('Child UPDATE Non-Key (With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child UPDATE Non-Key (With Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child DELETE (With Index)', 0, false);
CALL sql_saga.benchmark_reset();
DELETE FROM establishment WHERE id BETWEEN 8001 AND 9000;
CALL sql_saga.benchmark_log_and_reset('Child DELETE (With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Child DELETE (With Index) end', 1000, true);

-- Regular Table DML (fires CHECK constraint)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular INSERT (With Index)', 0, false);
CALL sql_saga.benchmark_reset();
INSERT INTO projects (name, legal_unit_id) SELECT 'New Project ' || i, i FROM generate_series(11001, 12000) i;
CALL sql_saga.benchmark_log_and_reset('Regular INSERT (With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular INSERT (With Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular UPDATE (With Index)', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE projects SET legal_unit_id = id + 2 WHERE id BETWEEN 1001 AND 2000;
CALL sql_saga.benchmark_log_and_reset('Regular UPDATE (With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular UPDATE (With Index) end', 1000, true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular DELETE (With Index)', 0, false);
CALL sql_saga.benchmark_reset();
DELETE FROM projects WHERE id BETWEEN 8001 AND 9000;
CALL sql_saga.benchmark_log_and_reset('Regular DELETE (With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Regular DELETE (With Index) end', 1000, true);


--------------------------------------------------------------------------------
\echo '--- temporal_merge on sql_saga Tables (With Performance Index) ---'
--------------------------------------------------------------------------------
-- Prepare recommended indices by temporal_merge
CREATE INDEX ON legal_unit USING GIST (daterange(valid_from, valid_until, '[)'));
CREATE INDEX ON establishment USING GIST (daterange(valid_from, valid_until, '[)'));

-- Parent table merge
CREATE TEMP TABLE legal_unit_source (row_id int, id int, valid_from date, valid_until date, name varchar);
INSERT INTO legal_unit_source SELECT i, i, '2015-01-01', 'infinity', 'Updated via Merge ' || i FROM generate_series(4001, 5000) AS i;
CREATE INDEX ON legal_unit_source(id);
CREATE INDEX ON legal_unit_source USING GIST (daterange(valid_from, valid_until, '[)'));
ANALYZE legal_unit_source;

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('temporal_merge Parent (With Index)', 0, false);
CALL sql_saga.benchmark_reset();
CALL sql_saga.temporal_merge('legal_unit'::regclass, 'legal_unit_source'::regclass, ARRAY['id']);
CALL sql_saga.benchmark_log_and_reset('temporal_merge Parent (With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('temporal_merge Parent (With Index) end', 1000, true);

-- Child table merge
CREATE TEMP TABLE establishment_source (row_id int, id int, valid_from date, valid_until date, legal_unit_id int, postal_place text);
INSERT INTO establishment_source SELECT i, i, '2015-01-01', 'infinity', i, 'Updated via Merge ' || i FROM generate_series(4001, 5000) AS i;
CREATE INDEX ON establishment_source(id);
CREATE INDEX ON establishment_source USING GIST (daterange(valid_from, valid_until, '[)'));
ANALYZE establishment_source;

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('temporal_merge Child (With Index)', 0, false);
CALL sql_saga.benchmark_reset();
CALL sql_saga.temporal_merge('establishment'::regclass, 'establishment_source'::regclass, ARRAY['id']);
CALL sql_saga.benchmark_log_and_reset('temporal_merge Child (With Index)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('temporal_merge Child (With Index) end', 1000, true);


-- Teardown sql_saga constraints
SELECT sql_saga.drop_foreign_key('projects', ARRAY['legal_unit_id']);
SELECT sql_saga.drop_foreign_key('establishment'::regclass, ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment'::regclass, ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('establishment'::regclass, cleanup => true);
SELECT sql_saga.drop_unique_key('legal_unit'::regclass, ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('legal_unit'::regclass, cleanup => true);

DROP TABLE establishment;
DROP TABLE legal_unit;
DROP TABLE projects;

\echo '-- Monitor log from pg_stat_monitor --'
\set monitor_log_filename expected/performance/100_benchmark_basic_dml_benchmark_monitor.csv
\i sql/include/benchmark_monitor_csv.sql

-- Verify the benchmark events and row counts, but exclude volatile timing data
-- from the regression test output to ensure stability.
SELECT event, row_count FROM benchmark ORDER BY seq_id;

-- Capture performance metrics to a separate file for manual review.
\set benchmark_log_filename expected/performance/100_benchmark_basic_dml_benchmark_report.log
\i sql/include/benchmark_report_log.sql

\i sql/include/benchmark_teardown.sql
\i sql/include/test_teardown.sql
