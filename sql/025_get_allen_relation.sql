\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test: sql_saga.get_allen_relation'
\echo 'This test provides comprehensive validation for the get_allen_relation function,'
\echo 'covering all 13 of Allen''s Interval Algebra relations.'
\echo '----------------------------------------------------------------------------'

-- Helper to make test output clear
CREATE FUNCTION test_relation(
    x int4range,
    y int4range,
    expected_relation sql_saga.allen_interval_relation
) RETURNS TABLE(test_case text, x_range text, y_range text, relation_is_correct boolean)
LANGUAGE plpgsql AS $$
DECLARE
    v_actual_relation sql_saga.allen_interval_relation;
    v_actual_relation_inverse sql_saga.allen_interval_relation;
    v_expected_relation_inverse sql_saga.allen_interval_relation;
BEGIN
    v_actual_relation := sql_saga.get_allen_relation(lower(x), upper(x), lower(y), upper(y));
    v_actual_relation_inverse := sql_saga.get_allen_relation(lower(y), upper(y), lower(x), upper(x));

    v_expected_relation_inverse := CASE expected_relation
        WHEN 'precedes' THEN 'preceded_by'
        WHEN 'meets' THEN 'met_by'
        WHEN 'overlaps' THEN 'overlapped_by'
        WHEN 'starts' THEN 'started_by'
        WHEN 'during' THEN 'contains'
        WHEN 'finishes' THEN 'finished_by'
        WHEN 'equals' THEN 'equals'
        WHEN 'preceded_by' THEN 'precedes'
        WHEN 'met_by' THEN 'meets'
        WHEN 'overlapped_by' THEN 'overlaps'
        WHEN 'started_by' THEN 'starts'
        WHEN 'contains' THEN 'during'
        WHEN 'finished_by' THEN 'finishes'
    END;

    test_case := format('X %s Y', expected_relation);
    x_range := x::text;
    y_range := y::text;
    relation_is_correct := (v_actual_relation = expected_relation AND v_actual_relation_inverse = v_expected_relation_inverse);

    RETURN NEXT;
END;
$$;

SELECT * FROM test_relation('[10,20)', '[30,40)', 'precedes');
SELECT * FROM test_relation('[10,20)', '[20,30)', 'meets');
SELECT * FROM test_relation('[10,30)', '[20,40)', 'overlaps');
SELECT * FROM test_relation('[10,20)', '[10,30)', 'starts');
SELECT * FROM test_relation('[20,30)', '[10,40)', 'during');
SELECT * FROM test_relation('[20,30)', '[10,30)', 'finishes');
SELECT * FROM test_relation('[10,20)', '[10,20)', 'equals');
SELECT * FROM test_relation('[30,40)', '[10,20)', 'preceded_by');
SELECT * FROM test_relation('[20,30)', '[10,20)', 'met_by');
SELECT * FROM test_relation('[20,40)', '[10,30)', 'overlapped_by');
SELECT * FROM test_relation('[10,30)', '[10,20)', 'started_by');
SELECT * FROM test_relation('[10,40)', '[20,30)', 'contains');
SELECT * FROM test_relation('[10,30)', '[20,30)', 'finished_by');

ROLLBACK;
\i sql/include/test_teardown.sql
