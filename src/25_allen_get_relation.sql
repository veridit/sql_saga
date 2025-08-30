CREATE OR REPLACE FUNCTION sql_saga.allen_get_relation(
    x_from anycompatible, x_until anycompatible,
    y_from anycompatible, y_until anycompatible
) RETURNS sql_saga.allen_interval_relation
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
    IF x_from IS NULL OR x_until IS NULL OR y_from IS NULL OR y_until IS NULL THEN
        RETURN NULL;
    END IF;

    IF x_until < y_from THEN RETURN 'precedes'; END IF;
    IF x_until = y_from THEN RETURN 'meets'; END IF;
    IF x_from < y_from AND y_from < x_until AND x_until < y_until THEN RETURN 'overlaps'; END IF;
    IF x_from = y_from AND x_until < y_until THEN RETURN 'starts'; END IF;
    IF x_from > y_from AND x_until < y_until THEN RETURN 'during'; END IF;
    IF x_from > y_from AND x_until = y_until THEN RETURN 'finishes'; END IF;
    IF x_from = y_from AND x_until = y_until THEN RETURN 'equals'; END IF;

    -- Inverse relations
    IF y_until < x_from THEN RETURN 'preceded by'; END IF;
    IF y_until = x_from THEN RETURN 'met by'; END IF;
    IF y_from < x_from AND x_from < y_until AND y_until < x_until THEN RETURN 'overlapped by'; END IF;
    IF x_from = y_from AND x_until > y_until THEN RETURN 'started by'; END IF;
    IF x_from < y_from AND x_until > y_until THEN RETURN 'contains'; END IF;
    IF x_from < y_from AND x_until = y_until THEN RETURN 'finished by'; END IF;

    RETURN NULL; -- Should be unreachable
END;
$$;
