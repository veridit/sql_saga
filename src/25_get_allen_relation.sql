CREATE OR REPLACE FUNCTION sql_saga.get_allen_relation(
    x_from anycompatible, x_until anycompatible,
    y_from anycompatible, y_until anycompatible
) RETURNS sql_saga.allen_interval_relation
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $get_allen_relation$
SELECT CASE
    -- Handle NULL inputs first
    WHEN x_from IS NULL OR x_until IS NULL OR y_from IS NULL OR y_until IS NULL THEN NULL
    -- Standard relations
    WHEN x_until < y_from THEN 'precedes'::sql_saga.allen_interval_relation
    WHEN x_until = y_from THEN 'meets'::sql_saga.allen_interval_relation
    WHEN x_from < y_from AND y_from < x_until AND x_until < y_until THEN 'overlaps'::sql_saga.allen_interval_relation
    WHEN x_from = y_from AND x_until < y_until THEN 'starts'::sql_saga.allen_interval_relation
    WHEN x_from > y_from AND x_until < y_until THEN 'during'::sql_saga.allen_interval_relation
    WHEN x_from > y_from AND x_until = y_until THEN 'finishes'::sql_saga.allen_interval_relation
    WHEN x_from = y_from AND x_until = y_until THEN 'equals'::sql_saga.allen_interval_relation
    -- Inverse relations
    WHEN y_until < x_from THEN 'preceded_by'::sql_saga.allen_interval_relation
    WHEN y_until = x_from THEN 'met_by'::sql_saga.allen_interval_relation
    WHEN y_from < x_from AND x_from < y_until AND y_until < x_until THEN 'overlapped_by'::sql_saga.allen_interval_relation
    WHEN x_from = y_from AND x_until > y_until THEN 'started_by'::sql_saga.allen_interval_relation
    WHEN x_from < y_from AND x_until > y_until THEN 'contains'::sql_saga.allen_interval_relation
    WHEN x_from < y_from AND x_until = y_until THEN 'finished_by'::sql_saga.allen_interval_relation
    ELSE NULL -- Should be unreachable for non-empty, valid intervals
END;
$get_allen_relation$;

COMMENT ON FUNCTION sql_saga.get_allen_relation(anycompatible, anycompatible, anycompatible, anycompatible) IS
'Calculates the Allen''s Interval Algebra relation between two intervals. This is a high-performance, inlinable SQL function.';
