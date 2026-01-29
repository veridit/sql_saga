/*
 * This is a unified trigger function that synchronizes all temporal column
 * representations: the authoritative range column, the discrete bounds
 * (`valid_from`/`valid_until`), and the inclusive-end (`valid_to`).
 * It uses a "detect change, derive, check, then populate" approach to ensure
 * perfect consistency regardless of which representation the user provides.
 *
 * The logic is especially nuanced for UPDATE operations to correctly infer user
 * intent. The core principle is that only columns that are explicitly part of
 * the SET clause are considered sources of truth for the new period. Unchanged
 * temporal columns are ignored during consistency checks and their values are
 * preserved from the OLD record.
 *
 * The process is as follows:
 * 1. Detect Change (on UPDATE): It identifies which temporal representation was
 *    actually modified by comparing the NEW and OLD records. For example, if an
 *    `UPDATE` statement only `SET`s `valid_until`, only `valid_until` is
 *    treated as an input for the new period. The existing `valid_from` and
 *    `valid_range` are considered unchanged.
 * 2. Derive: It deconstructs all *changed* source representation(s) into a
 *    common `[from, until)` form.
 * 3. Check Consistency: It verifies that all derived forms are consistent. If a
 *    user provides multiple, conflicting temporal values in a single statement
 *    (e.g., a `valid_to` that disagrees with a `valid_range`), it raises an
 *    error. There is no priority; all provided inputs must agree.
 * 4. Preserve Unchanged Bounds (on UPDATE): If a bound (e.g., the period's start)
 *    was not specified in the `UPDATE`, its value is carried over from the OLD record.
 * 5. Apply Defaults: If the end of the period is still undetermined (e.g., it
 *    was explicitly set to NULL or was not provided on INSERT) and the era is
 *    configured to apply defaults, it is set to 'infinity'.
 * 6. Populate: It determines the final, authoritative period from the combination
 *    of derived, preserved, and defaulted bounds, and then overwrites all
 *    temporal columns in the NEW record to be consistent with that single period.
 */
CREATE OR REPLACE FUNCTION sql_saga.synchronize_temporal_columns()
RETURNS TRIGGER LANGUAGE plpgsql AS $synchronize_temporal_columns$
DECLARE
    -- Column names passed from add_era
    range_col     name := TG_ARGV[0];
    from_col      name := NULLIF(NULLIF(TG_ARGV[1], ''),'null');
    until_col     name := NULLIF(NULLIF(TG_ARGV[2], ''),'null');
    to_col        name := NULLIF(NULLIF(TG_ARGV[3], ''),'null');
    range_subtype regtype := TG_ARGV[4]::regtype;
    apply_defaults boolean := TG_ARGV[5]::boolean;

    -- For dynamic manipulation
    rec_new record;
    new_row jsonb := to_jsonb(NEW);
    old_row jsonb := CASE WHEN TG_OP = 'UPDATE' THEN to_jsonb(OLD) ELSE NULL END;

    -- Deconstructed bounds from each potential source in the NEW record
    from_via_bounds text;
    until_via_bounds text;
    until_via_to    text;
    from_via_range  text;
    until_via_range text;

    -- Final, authoritative values for the period
    final_from  text;
    final_until text;

    is_range_changed  boolean;
    is_from_changed   boolean;
    is_until_changed  boolean;
    is_to_changed     boolean;
    is_until_explicitly_null boolean;
BEGIN
    -- This trigger follows a strict logical sequence:
    -- 1. Gather all inputs provided by the user for this operation. For an UPDATE,
    --    an "input" is any temporal column whose value is distinct from the OLD record.
    -- 2. Apply default values. If the user did not provide any end-of-period and
    --    defaults are enabled, 'infinity' is used. This happens *before* consistency
    --    checks to simplify the logic.
    -- 3. Check for inconsistencies among all the gathered inputs. If multiple, conflicting
    --    values were provided for the start or end of the period, raise an error.
    -- 4. Derive the final, authoritative period. This is a single `[from, until)`
    --    that is either taken from a consistent user input, or preserved from the
    --    OLD record on an UPDATE if no change was requested.
    -- 5. Populate all temporal representations (`range`, `from/until`, `to`) from
    --    the single authoritative period to ensure the row is perfectly consistent.

    -- STEP 1: Gather all changed inputs.
    is_range_changed  := (TG_OP = 'INSERT' AND (new_row ->> range_col) IS NOT NULL AND (new_row ->> range_col) <> 'empty') OR (TG_OP = 'UPDATE' AND (new_row ->> range_col) IS DISTINCT FROM (old_row ->> range_col));
    is_from_changed   := from_col IS NOT NULL AND ((TG_OP = 'INSERT' AND (new_row ->> from_col) IS NOT NULL) OR (TG_OP = 'UPDATE' AND (new_row ->> from_col) IS DISTINCT FROM (old_row ->> from_col)));
    is_until_changed  := until_col IS NOT NULL AND ((TG_OP = 'INSERT' AND (new_row ->> until_col) IS NOT NULL) OR (TG_OP = 'UPDATE' AND (new_row ->> until_col) IS DISTINCT FROM (old_row ->> until_col)));
    is_to_changed     := to_col IS NOT NULL AND ((TG_OP = 'INSERT' AND (new_row ->> to_col) IS NOT NULL) OR (TG_OP = 'UPDATE' AND (new_row ->> to_col) IS DISTINCT FROM (old_row ->> to_col)));
    -- An explicit NULL is also a change. The condition for applying defaults must be more specific.
    is_until_explicitly_null := until_col IS NOT NULL AND (new_row ->> until_col) IS NULL AND ((TG_OP = 'INSERT') OR (TG_OP = 'UPDATE' AND (old_row ->> until_col) IS NOT NULL));

    -- Check for empty range explicitly
    IF (new_row ->> range_col) = 'empty' THEN
        RAISE EXCEPTION 'Cannot use an empty range for temporal column "%"', range_col;
    END IF;

    IF is_range_changed THEN
        DECLARE range_type regtype;
        BEGIN
            SELECT a.atttypid INTO range_type FROM pg_attribute a WHERE a.attrelid = TG_RELID AND a.attname = range_col;
            -- `upper()` correctly returns NULL for an unbounded upper range.
            EXECUTE format('SELECT lower($1::%s)::text, upper($1::%s)::text', range_type, range_type)
                INTO from_via_range, until_via_range
                USING (new_row ->> range_col);
        END;
    END IF;
    IF is_from_changed THEN from_via_bounds := (new_row ->> from_col); END IF;
    IF is_until_changed THEN until_via_bounds := (new_row ->> until_col); END IF;

    -- STEP 2: Check for inconsistencies and determine authoritative period.
    -- This logic is declarative: it gathers all provided sources for each part
    -- of the period (start and end), verifies they are consistent, and then
    -- determines the final, authoritative value.

    -- Part A: Determine the start of the period ('final_from')
    DECLARE
        from_sources jsonb := '{}'::jsonb;
    BEGIN
        IF is_range_changed THEN from_sources := from_sources || jsonb_build_object(range_col, from_via_range); END IF;
        IF is_from_changed THEN from_sources := from_sources || jsonb_build_object(from_col, from_via_bounds); END IF;

        DECLARE
            -- Ignore NULL values during consistency check; they mean "not provided".
            distinct_from_values text[] := (SELECT array_agg(DISTINCT value) FROM jsonb_each_text(from_sources) WHERE value IS NOT NULL);
        BEGIN
            IF array_length(distinct_from_values, 1) > 1 THEN
                 RAISE EXCEPTION 'Inconsistent start of period provided. Sources: %', jsonb_strip_nulls(from_sources);
            END IF;
            final_from := distinct_from_values[1];
        END;

        IF TG_OP = 'UPDATE' AND (SELECT count(*) FROM jsonb_object_keys(from_sources)) = 0 THEN
            final_from := old_row ->> from_col;
        END IF;
    END;

    -- Part B: Determine the end of the period ('final_until')
    DECLARE
        until_sources jsonb := '{}'::jsonb;
    BEGIN
        IF is_to_changed THEN
            IF final_from IS NULL THEN
                 RAISE EXCEPTION 'When setting "%", the start of the period must also be provided via "%" or "%".', to_col, from_col, range_col;
            END IF;
            EXECUTE format('SELECT ($1::%s + 1)::text', range_subtype) INTO until_via_to USING (new_row ->> to_col);
            until_sources := until_sources || jsonb_build_object(to_col, until_via_to);
        END IF;

        IF is_range_changed THEN until_sources := until_sources || jsonb_build_object(range_col, until_via_range); END IF;
        IF is_until_changed THEN until_sources := until_sources || jsonb_build_object(until_col, until_via_bounds); END IF;

        DECLARE
            -- Ignore NULL values during consistency check, they mean "not provided".
            distinct_until_values text[] := (SELECT array_agg(DISTINCT value) FROM jsonb_each_text(until_sources) WHERE value IS NOT NULL);
        BEGIN
            IF array_length(distinct_until_values, 1) > 1 THEN
                RAISE EXCEPTION 'Inconsistent end of period provided. Sources: %', jsonb_strip_nulls(until_sources);
            END IF;
            final_until := distinct_until_values[1];
        END;

        IF TG_OP = 'UPDATE' AND (SELECT count(*) FROM jsonb_object_keys(until_sources)) = 0 THEN
            final_until := old_row ->> until_col;
        END IF;

        IF apply_defaults AND final_until IS NULL THEN
            final_until := 'infinity';
        END IF;
    END;

    IF final_from IS NULL OR final_until IS NULL THEN
        IF to_col IS NOT NULL THEN
             RAISE EXCEPTION 'The temporal period could not be determined. At least one of "%", "%" (with "%"), or the pair "%"/"%" must be provided.', range_col, to_col, from_col, from_col, until_col;
        ELSE
             RAISE EXCEPTION 'The temporal period could not be determined. At least one of "%" or the pair "%"/"%" must be provided.', range_col, from_col, until_col;
        END IF;
    END IF;

    -- STEP 4: Populate all temporal columns in the NEW record from the final bounds.
    -- This ensures all representations are consistent.
    IF from_col IS NOT NULL THEN
        new_row := jsonb_set(new_row, ARRAY[from_col], to_jsonb(final_from));
        new_row := jsonb_set(new_row, ARRAY[until_col], to_jsonb(final_until));
    END IF;

    IF to_col IS NOT NULL THEN
        DECLARE final_to text;
        BEGIN
            EXECUTE format('SELECT CASE WHEN $1 = ''infinity'' THEN ''infinity'' ELSE ($1::%s - 1)::text END', range_subtype) INTO final_to USING final_until;
            new_row := jsonb_set(new_row, ARRAY[to_col], to_jsonb(final_to));
        END;
    END IF;

    DECLARE
        final_range text;
        range_type regtype;
    BEGIN
        SELECT a.atttypid INTO range_type FROM pg_attribute a WHERE a.attrelid = TG_RELID AND a.attname = range_col;
        EXECUTE format('SELECT %s($1::%s, $2::%s, ''[)'')', range_type, range_subtype, range_subtype) INTO final_range USING final_from, final_until;
        new_row := jsonb_set(new_row, ARRAY[range_col], to_jsonb(final_range));
    EXCEPTION WHEN data_exception THEN -- Let the table's CHECK constraint handle invalid bounds (e.g., from >= until)
    END;

    rec_new := jsonb_populate_record(NEW, new_row);
    RETURN rec_new;
END;
$synchronize_temporal_columns$;
