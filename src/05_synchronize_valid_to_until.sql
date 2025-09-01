/*
 * Generic trigger function to synchronize an inclusive end-date ('valid_to')
 * with the exclusive end-date ('valid_until') required by sql_saga.
 * Ensures valid_until = valid_to + '1 day'.
 * This is provided as a convenience for users who prefer to work with
 * inclusive end dates.
 */
CREATE FUNCTION sql_saga.synchronize_valid_to_until()
RETURNS TRIGGER LANGUAGE plpgsql AS $synchronize_valid_to_until$
DECLARE
    valid_until_col name := TG_ARGV[0];
    valid_to_col    name := TG_ARGV[1];
    rec_new         record;
    new_row         jsonb;
    old_row         jsonb;
    new_valid_until timestamptz;
    old_valid_until timestamptz;
    new_valid_to    timestamptz;
    old_valid_to    timestamptz;
BEGIN
    -- This trigger synchronizes an inclusive end column (e.g., 'valid_to')
    -- with an exclusive end column ('valid_until').
    -- The relationship is valid_until = valid_to + '1 day' (or equivalent).
    -- The names of the columns are passed as arguments to the trigger.

    new_row := to_jsonb(NEW);
    -- Cast to timestamptz for calculations, regardless of original type (date, ts, tstz)
    new_valid_until := (new_row ->> valid_until_col)::timestamptz;
    new_valid_to    := (new_row ->> valid_to_col)::timestamptz;

    -- For INSERT operations
    IF TG_OP = 'INSERT' THEN
        IF new_valid_until IS NOT NULL AND new_valid_to IS NULL THEN
            new_row := jsonb_set(new_row, ARRAY[valid_to_col], to_jsonb(new_valid_until - INTERVAL '1 day'), true);
        ELSIF new_valid_to IS NOT NULL AND new_valid_until IS NULL THEN
            new_row := jsonb_set(new_row, ARRAY[valid_until_col], to_jsonb(new_valid_to + INTERVAL '1 day'), true);
        ELSIF new_valid_until IS NOT NULL AND new_valid_to IS NOT NULL THEN
            IF new_valid_to != (new_valid_until - INTERVAL '1 day') THEN
                RAISE EXCEPTION 'On INSERT, % and % are inconsistent. Expected % = % - 1 day. Got %=%, %=%',
                    quote_ident(valid_to_col), quote_ident(valid_until_col),
                    quote_ident(valid_to_col), quote_ident(valid_until_col),
                    quote_ident(valid_to_col), new_valid_to,
                    quote_ident(valid_until_col), new_valid_until;
            END IF;
        -- If both are NULL, do nothing, let table constraints handle it.
        END IF;

    -- For UPDATE operations
    ELSIF TG_OP = 'UPDATE' THEN
        old_row := to_jsonb(OLD);
        old_valid_until := (old_row ->> valid_until_col)::timestamptz;
        old_valid_to    := (old_row ->> valid_to_col)::timestamptz;

        -- Case 1: Both columns are explicitly changed.
        IF new_valid_until IS DISTINCT FROM old_valid_until AND new_valid_to IS DISTINCT FROM old_valid_to THEN
            IF new_valid_until IS NULL OR new_valid_to IS NULL THEN
                RAISE EXCEPTION 'On UPDATE, when changing both % and %, neither can be set to NULL.',
                    quote_ident(valid_to_col), quote_ident(valid_until_col);
            END IF;
            IF new_valid_to != (new_valid_until - INTERVAL '1 day') THEN
                RAISE EXCEPTION 'On UPDATE, conflicting explicit values for % and %. With % = %, expected % = %. Got % = %',
                                 quote_ident(valid_to_col), quote_ident(valid_until_col),
                                 quote_ident(valid_until_col), new_valid_until,
                                 quote_ident(valid_to_col), new_valid_until - INTERVAL '1 day',
                                 quote_ident(valid_to_col), new_valid_to;
            END IF;
        -- Case 2: Only valid_until is explicitly changed.
        ELSIF new_valid_until IS DISTINCT FROM old_valid_until THEN
            IF new_valid_until IS NULL THEN
                RAISE EXCEPTION 'On UPDATE, % cannot be set to NULL.', quote_ident(valid_until_col);
            END IF;
            new_row := jsonb_set(new_row, ARRAY[valid_to_col], to_jsonb(new_valid_until - INTERVAL '1 day'), true);
        -- Case 3: Only valid_to is explicitly changed.
        ELSIF new_valid_to IS DISTINCT FROM old_valid_to THEN
            IF new_valid_to IS NULL THEN
                RAISE EXCEPTION 'On UPDATE, % cannot be set to NULL.', quote_ident(valid_to_col);
            END IF;
            new_row := jsonb_set(new_row, ARRAY[valid_until_col], to_jsonb(new_valid_to + INTERVAL '1 day'), true);
        -- Case 4: Neither is being distinctly changed. Check for consistency if they are not NULL.
        ELSE
            IF new_valid_until IS NOT NULL AND new_valid_to IS NOT NULL THEN
                 IF new_valid_to != (new_valid_until - INTERVAL '1 day') THEN
                     RAISE EXCEPTION 'On UPDATE, existing % and % are inconsistent. Got % = %, % = %',
                        quote_ident(valid_to_col), quote_ident(valid_until_col),
                        quote_ident(valid_to_col), new_valid_to,
                        quote_ident(valid_until_col), new_valid_until;
                 END IF;
            END IF;
        END IF;
    END IF;

    rec_new := jsonb_populate_record(NEW, new_row);
    RETURN rec_new;
END;
$synchronize_valid_to_until$;
