\i sql/include/test_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

\i sql/include/benchmark_setup.sql

CREATE TABLE legal_unit_tm_bs_on (id INTEGER, valid_from date, valid_until date, name varchar NOT NULL);
CREATE TABLE establishment_tm_bs_on (id INTEGER, valid_from date, valid_until date, legal_unit_id INTEGER NOT NULL, postal_place TEXT NOT NULL);
SELECT sql_saga.add_era(table_oid => 'legal_unit_tm_bs_on', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era(table_oid => 'establishment_tm_bs_on', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit_tm_bs_on', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'establishment_tm_bs_on', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_temporal_foreign_key(
    fk_table_oid => 'establishment_tm_bs_on',
    fk_column_names => ARRAY['legal_unit_id'],
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_tm_bs_on_id_valid'
);

CREATE TABLE legal_unit_tm_bs_off (id INTEGER, valid_from date, valid_until date, name varchar NOT NULL);
CREATE TABLE establishment_tm_bs_off (id INTEGER, valid_from date, valid_until date, legal_unit_id INTEGER NOT NULL, postal_place TEXT NOT NULL);
SELECT sql_saga.add_era(table_oid => 'legal_unit_tm_bs_off', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era(table_oid => 'establishment_tm_bs_off', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit_tm_bs_off', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'establishment_tm_bs_off', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_temporal_foreign_key(
    fk_table_oid => 'establishment_tm_bs_off',
    fk_column_names => ARRAY['legal_unit_id'],
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_tm_bs_off_id_valid'
);


DO $$
DECLARE
    v_batch_sizes int[] := ARRAY[100, 1000, 10000];
    v_modes text[] := ARRAY['MERGE_ENTITY_PATCH', 'MERGE_ENTITY_REPLACE'];
    v_mode text;
    v_batch_size int;
    v_total_rows int := 2000;
    v_start_id int;
    v_end_id int;
BEGIN
    CREATE TEMPORARY TABLE legal_unit_source_bs (row_id int, id int, valid_from date, valid_until date, name varchar);
    CREATE TEMPORARY TABLE establishment_source_bs (row_id int, id int, valid_from date, valid_until date, legal_unit_id int, postal_place text);

    FOREACH v_batch_size IN ARRAY v_batch_sizes
    LOOP
        FOREACH v_mode IN ARRAY v_modes
        LOOP
            -- Triggers ON
            TRUNCATE legal_unit_tm_bs_on, establishment_tm_bs_on;
            INSERT INTO benchmark (event, row_count) VALUES (format('tm_loop, batch %s / %s (Triggers ON) start', v_batch_size, v_mode), 0);
        v_start_id := 1;
        WHILE v_start_id <= v_total_rows LOOP
            v_end_id := v_start_id + v_batch_size - 1;
            IF v_end_id > v_total_rows THEN v_end_id := v_total_rows; END IF;

            TRUNCATE legal_unit_source_bs, establishment_source_bs;
            INSERT INTO legal_unit_source_bs SELECT i, i, '2015-01-01', 'infinity', 'Company ' || i FROM generate_series(v_start_id, v_end_id) AS i;
            INSERT INTO establishment_source_bs SELECT i, i, '2015-01-01', 'infinity', i, 'Shop ' || i FROM generate_series(v_start_id, v_end_id) AS i;

            CALL sql_saga.temporal_merge('legal_unit_tm_bs_on'::regclass, 'legal_unit_source_bs'::regclass, ARRAY['id'], mode => v_mode::sql_saga.temporal_merge_mode, ephemeral_columns => ARRAY[]::text[]);
            CALL sql_saga.temporal_merge('establishment_tm_bs_on'::regclass, 'establishment_source_bs'::regclass, ARRAY['id'], mode => v_mode::sql_saga.temporal_merge_mode, ephemeral_columns => ARRAY[]::text[]);
            v_start_id := v_end_id + 1;
        END LOOP;
        INSERT INTO benchmark (event, row_count) VALUES (format('tm_loop, batch %s / %s (Triggers ON) end', v_batch_size, v_mode), v_total_rows * 2);

        -- Triggers OFF
        TRUNCATE legal_unit_tm_bs_off, establishment_tm_bs_off;
        INSERT INTO benchmark (event, row_count) VALUES (format('tm_loop, batch %s / %s (Triggers OFF) start', v_batch_size, v_mode), 0);
        v_start_id := 1;
        WHILE v_start_id <= v_total_rows LOOP
            v_end_id := v_start_id + v_batch_size - 1;
            IF v_end_id > v_total_rows THEN v_end_id := v_total_rows; END IF;

            TRUNCATE legal_unit_source_bs, establishment_source_bs;
            INSERT INTO legal_unit_source_bs SELECT i, i, '2015-01-01', 'infinity', 'Company ' || i FROM generate_series(v_start_id, v_end_id) AS i;
            INSERT INTO establishment_source_bs SELECT i, i, '2015-01-01', 'infinity', i, 'Shop ' || i FROM generate_series(v_start_id, v_end_id) AS i;

            CALL sql_saga.disable_temporal_triggers('legal_unit_tm_bs_off', 'establishment_tm_bs_off');
            CALL sql_saga.temporal_merge('legal_unit_tm_bs_off'::regclass, 'legal_unit_source_bs'::regclass, ARRAY['id'], mode => v_mode::sql_saga.temporal_merge_mode, ephemeral_columns => ARRAY[]::text[]);
            CALL sql_saga.temporal_merge('establishment_tm_bs_off'::regclass, 'establishment_source_bs'::regclass, ARRAY['id'], mode => v_mode::sql_saga.temporal_merge_mode, ephemeral_columns => ARRAY[]::text[]);
            CALL sql_saga.enable_temporal_triggers('legal_unit_tm_bs_off', 'establishment_tm_bs_off');
            v_start_id := v_end_id + 1;
        END LOOP;
        INSERT INTO benchmark (event, row_count) VALUES (format('tm_loop, batch %s / %s (Triggers OFF) end', v_batch_size, v_mode), v_total_rows * 2);
        END LOOP;
    END LOOP;
END;
$$;

SELECT 'legal_unit_tm_bs_on' AS type, COUNT(*) AS count FROM legal_unit_tm_bs_on
UNION ALL
SELECT 'establishment_tm_bs_on' AS type, COUNT(*) AS count FROM establishment_tm_bs_on
UNION ALL
SELECT 'legal_unit_tm_bs_off' AS type, COUNT(*) AS count FROM legal_unit_tm_bs_off
UNION ALL
SELECT 'establishment_tm_bs_off' AS type, COUNT(*) AS count FROM establishment_tm_bs_off;

-- Teardown for batch size benchmark tables
SELECT sql_saga.drop_foreign_key('establishment_tm_bs_on', ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment_tm_bs_on', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('establishment_tm_bs_on', cleanup => true);
SELECT sql_saga.drop_unique_key('legal_unit_tm_bs_on', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('legal_unit_tm_bs_on', cleanup => true);
SELECT sql_saga.drop_foreign_key('establishment_tm_bs_off', ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment_tm_bs_off', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('establishment_tm_bs_off', cleanup => true);
SELECT sql_saga.drop_unique_key('legal_unit_tm_bs_off', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('legal_unit_tm_bs_off', cleanup => true);

INSERT INTO benchmark (event, row_count) VALUES ('Constraints disabled', 0);

DROP TABLE establishment_tm_bs_on;
DROP TABLE legal_unit_tm_bs_on;
DROP TABLE establishment_tm_bs_off;
DROP TABLE legal_unit_tm_bs_off;

INSERT INTO benchmark (event, row_count) VALUES ('Tear down complete', 0);

-- Verify the benchmark events and row counts, but exclude volatile timing data
-- from the regression test output to ensure stability.
SELECT event, row_count FROM benchmark ORDER BY seq_id;

-- Capture performance metrics to a separate file for manual review.
\o expected/102_benchmark_temporal_merge_batch_sizes_performance.out

\i sql/include/benchmark_report.sql

-- Stop redirecting output
\o

\i sql/include/test_teardown.sql
