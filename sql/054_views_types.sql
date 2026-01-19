\i sql/include/test_setup.sql

-- This test verifies that the updatable views (`current` and `for_portion_of`)
-- work correctly across all supported range types.

CREATE SCHEMA test_types;
SET search_path TO test_types, sql_saga;

DO $test$
DECLARE
    p_rec record;
    v_table_name text;
    v_current_view_name text;
    v_portion_view_name text;
BEGIN
    RAISE NOTICE '--- Testing updatable views with various data types ---';

    FOR p_rec IN
        SELECT * FROM (
            VALUES
                ('date',      true,  '''2024-01-10''::date', '''2024-01-15''::date', '''2024-01-20''::date', '''2024-01-25''::date'),
                ('int',       false, '10',                  '15',                  '20',                  '25'),
                ('bigint',    false, '10',                  '15',                  '20',                  '25'),
                ('numeric',   false, '10.0',                '15.0',                '20.0',                '25.0'),
                ('timestamp', true,  '''2024-01-10 12:00:00''::timestamp', '''2024-01-15 12:00:00''::timestamp', '''2024-01-20 12:00:00''::timestamp', '''2024-01-25 12:00:00''::timestamp'),
                ('timestamptz', true,  '''2024-01-10 12:00:00Z''::timestamptz', '''2024-01-15 12:00:00Z''::timestamptz', '''2024-01-20 12:00:00Z''::timestamptz', '''2024-01-25 12:00:00Z''::timestamptz')
        ) AS t(base_type, test_current_view, time1, time2, time3, time4)
    LOOP
        v_table_name := format('test_table_%s', p_rec.base_type);
        v_current_view_name := v_table_name || '__current_valid';
        v_portion_view_name := v_table_name || '__for_portion_of_valid';

        RAISE NOTICE '';
        RAISE NOTICE '--- Testing type: % ---', p_rec.base_type;

        -- Create table
        EXECUTE format($SQL$
            CREATE TABLE %1$I (
                id int,
                value text,
                valid_range %2$s,
                valid_from %3$s,
                valid_until %3$s
            );
        $SQL$, v_table_name, p_rec.base_type || 'range', p_rec.base_type);

        -- Activate sql_saga
        PERFORM sql_saga.add_era(v_table_name::regclass, 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
        PERFORM sql_saga.add_unique_key(v_table_name::regclass, '{id}');
        PERFORM sql_saga.add_for_portion_of_view(v_table_name::regclass);

        -- == Test for_portion_of view ==
        RAISE NOTICE '1. for_portion_of: INSERT';
        EXECUTE format($SQL$
            INSERT INTO %1$I (id, value, valid_from, valid_until) VALUES (1, 'initial', %2$s, %4$s);
        $SQL$, v_portion_view_name, p_rec.time1, p_rec.time2, p_rec.time3, p_rec.time4);
        EXECUTE format('TABLE %I ORDER BY id, valid_from', v_table_name);

        RAISE NOTICE '2. for_portion_of: UPDATE (historical correction)';
        EXECUTE format($SQL$
            UPDATE %1$I SET value = 'updated' WHERE id = 1 AND valid_from = %2$s;
        $SQL$, v_portion_view_name, p_rec.time1);
        EXECUTE format('TABLE %I ORDER BY id, valid_from', v_table_name);

        RAISE NOTICE '3. for_portion_of: UPDATE (split)';
        EXECUTE format($SQL$
            UPDATE %1$I SET value = 'split', valid_from = %2$s, valid_until = %3$s WHERE id = 1;
        $SQL$, v_portion_view_name, p_rec.time2, p_rec.time3);
        EXECUTE format('TABLE %I ORDER BY id, valid_from', v_table_name);

        -- == Test current view ==
        IF p_rec.test_current_view THEN
            RAISE NOTICE '--- Testing current view for type: % ---', p_rec.base_type;
            -- We need a helper function to simulate the passage of time for the current view.

            -- Time 1: Initial INSERT
            EXECUTE format($SQL$
                CREATE FUNCTION get_current_time() RETURNS %1$s LANGUAGE sql IMMUTABLE AS $$SELECT %2$s$$;
            $SQL$, p_rec.base_type, p_rec.time1);
            PERFORM sql_saga.add_current_view(v_table_name::regclass, current_func_name => 'get_current_time()');
            RAISE NOTICE '4. current: INSERT at time 1';
            EXECUTE format($SQL$
                INSERT INTO %1$I (id, value) VALUES (2, 'current initial');
            $SQL$, v_current_view_name);
            PERFORM sql_saga.drop_current_view(v_table_name::regclass);
            EXECUTE format('TABLE %I ORDER BY id, valid_from', v_table_name);

            -- Time 2: SCD2 Update
            EXECUTE format($SQL$
                CREATE OR REPLACE FUNCTION get_current_time() RETURNS %1$s LANGUAGE sql IMMUTABLE AS $$SELECT %2$s$$;
            $SQL$, p_rec.base_type, p_rec.time2);
            PERFORM sql_saga.add_current_view(v_table_name::regclass, current_func_name => 'get_current_time()');
            RAISE NOTICE '5. current: UPDATE (SCD2) at time 2';
            EXECUTE format($SQL$
                UPDATE %1$I SET value = 'current updated' WHERE id = 2;
            $SQL$, v_current_view_name);
            PERFORM sql_saga.drop_current_view(v_table_name::regclass);
            EXECUTE format('TABLE %I ORDER BY id, valid_from', v_table_name);
            
            -- Time 3: Soft-delete
            EXECUTE format($SQL$
                CREATE OR REPLACE FUNCTION get_current_time() RETURNS %1$s LANGUAGE sql IMMUTABLE AS $$SELECT %2$s$$;
            $SQL$, p_rec.base_type, p_rec.time3);
            PERFORM sql_saga.add_current_view(v_table_name::regclass, current_func_name => 'get_current_time()');
            RAISE NOTICE '6. current: UPDATE (soft-delete) at time 3';
            EXECUTE format($SQL$
                UPDATE %1$I SET valid_from = 'infinity' WHERE id = 2;
            $SQL$, v_current_view_name);
            PERFORM sql_saga.drop_current_view(v_table_name::regclass);
            EXECUTE format('TABLE %I ORDER BY id, valid_from', v_table_name);

            EXECUTE 'DROP FUNCTION get_current_time()';
        ELSE
            RAISE NOTICE '--- Skipping current view test for type: % ---', p_rec.base_type;
        END IF;
    END LOOP;
END
$test$;

\i sql/include/test_teardown.sql
