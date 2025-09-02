/*
 * This is a unified trigger function that synchronizes all three temporal column
 * types: bounds (`valid_from`/`valid_until`), inclusive-end (`valid_to`), and
 * native range (`validity`). It uses a fail-fast approach, raising an error
 * if the user provides inconsistent data.
 */
CREATE OR REPLACE FUNCTION sql_saga.synchronize_temporal_columns()
RETURNS TRIGGER LANGUAGE plpgsql AS $synchronize_temporal_columns$
DECLARE
    -- Column names passed from add_era
    from_col      name := TG_ARGV[0];
    until_col     name := TG_ARGV[1];
    to_col        name := NULLIF(TG_ARGV[2], ''); -- Convert empty string to NULL
    range_col     name := NULLIF(TG_ARGV[3], ''); -- Convert empty string to NULL
    range_subtype regtype := TG_ARGV[4]::regtype; -- The concrete type of the bounds

    -- For dynamic manipulation
    rec_new record;
    new_row jsonb := to_jsonb(NEW);

    -- Derived values
    derived_from    text;
    derived_until   text;
    derived_to      text;
    derived_range   text;

    -- User-provided values
    user_from    text := new_row ->> from_col;
    user_until   text := new_row ->> until_col;
    user_to      text := new_row ->> to_col;
    user_range   text := new_row ->> range_col;
BEGIN
    -- Step 1: Determine the source of truth and derive initial bounds.
    IF TG_OP = 'UPDATE' THEN
        DECLARE
            old_row jsonb := to_jsonb(OLD);
            range_changed boolean := range_col IS NOT NULL AND (new_row ->> range_col) IS DISTINCT FROM (old_row ->> range_col);
            to_changed    boolean := to_col IS NOT NULL AND (new_row ->> to_col) IS DISTINCT FROM (old_row ->> to_col);
        BEGIN
            IF range_changed THEN
                -- Source: Range
                DECLARE range_type regtype;
                BEGIN
                    SELECT a.atttypid INTO range_type FROM pg_attribute a WHERE a.attrelid = TG_RELID AND a.attname = range_col;
                    EXECUTE format('SELECT lower($1::%s)::text, upper($1::%s)::text', range_type, range_type) INTO derived_from, derived_until USING user_range;
                END;
            ELSIF to_changed THEN
                -- Source: valid_to
                derived_from := user_from;
                EXECUTE format('SELECT ($1::%s + 1)::text', range_subtype) INTO derived_until USING user_to;
            ELSE -- Source is bounds (or no change)
                derived_from  := user_from;
                derived_until := user_until;
            END IF;
        END;
    ELSE -- INSERT: use precedence
        IF range_col IS NOT NULL AND user_range IS NOT NULL AND user_range <> 'empty' THEN
            -- Source: Range
            DECLARE range_type regtype;
            BEGIN
                SELECT a.atttypid INTO range_type FROM pg_attribute a WHERE a.attrelid = TG_RELID AND a.attname = range_col;
                EXECUTE format('SELECT lower($1::%s)::text, upper($1::%s)::text', range_type, range_type) INTO derived_from, derived_until USING user_range;
            END;
        ELSIF to_col IS NOT NULL AND user_to IS NOT NULL THEN
            -- Source: valid_to
            derived_from := user_from;
            EXECUTE format('SELECT ($1::%s + 1)::text', range_subtype) INTO derived_until USING user_to;
        ELSE
            -- Source: Bounds (or nothing provided)
            derived_from  := user_from;
            derived_until := user_until;
        END IF;
    END IF;

    -- Step 2: From the derived bounds, derive all other representations.
    IF to_col IS NOT NULL AND derived_until IS NOT NULL THEN
        EXECUTE format('SELECT ($1::%s - 1)::text', range_subtype) INTO derived_to USING derived_until;
    END IF;

    IF range_col IS NOT NULL AND derived_from IS NOT NULL AND derived_until IS NOT NULL THEN
        DECLARE range_type regtype;
        BEGIN
            SELECT a.atttypid INTO range_type FROM pg_attribute a WHERE a.attrelid = TG_RELID AND a.attname = range_col;
            EXECUTE format('SELECT %s($1::%s, $2::%s, ''[)'')', range_type, range_subtype, range_subtype) INTO derived_range USING derived_from, derived_until;
        EXCEPTION WHEN data_exception THEN -- let CHECK constraint handle invalid bounds
        END;
    END IF;

    -- Step 3: Verify consistency for INSERTs and populate NEW record.
    -- For UPDATEs, we trust the derivation precedence and simply apply it.
    IF TG_OP = 'INSERT' THEN
        IF user_from IS NOT NULL AND user_from IS DISTINCT FROM derived_from THEN
            RAISE EXCEPTION 'Inconsistent values: "%" is %, but is derived as % from other inputs.', from_col, user_from, derived_from;
        END IF;
        IF user_until IS NOT NULL AND user_until IS DISTINCT FROM derived_until THEN
            RAISE EXCEPTION 'Inconsistent values: "%" is %, but is derived as % from other inputs.', until_col, user_until, derived_until;
        END IF;
        IF to_col IS NOT NULL AND user_to IS NOT NULL AND user_to IS DISTINCT FROM derived_to THEN
            RAISE EXCEPTION 'Inconsistent values: "%" is %, but is derived as % from other inputs.', to_col, user_to, derived_to;
        END IF;
        IF range_col IS NOT NULL AND user_range IS NOT NULL AND user_range <> 'empty' AND user_range IS DISTINCT FROM derived_range THEN
            RAISE EXCEPTION 'Inconsistent values: "%" is %, but is derived as % from other inputs.', range_col, user_range, derived_range;
        END IF;
    END IF;

    -- Populate all columns with the consistent, derived values.
    new_row := jsonb_set(new_row, ARRAY[from_col], to_jsonb(derived_from));
    new_row := jsonb_set(new_row, ARRAY[until_col], to_jsonb(derived_until));
    IF to_col IS NOT NULL THEN new_row := jsonb_set(new_row, ARRAY[to_col], to_jsonb(derived_to)); END IF;
    IF range_col IS NOT NULL THEN new_row := jsonb_set(new_row, ARRAY[range_col], to_jsonb(derived_range)); END IF;

    rec_new := jsonb_populate_record(NEW, new_row);
    RETURN rec_new;
END;
$synchronize_temporal_columns$;
