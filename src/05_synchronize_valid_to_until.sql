/*
 * Generic trigger function to synchronize an inclusive end-date ('valid_to')
 * with the exclusive end-date ('valid_until') required by sql_saga.
 * Ensures valid_until = valid_to + '1 day'.
 * This is provided as a convenience for users who prefer to work with
 * inclusive end dates.
 */
CREATE FUNCTION sql_saga.synchronize_valid_to_until()
RETURNS TRIGGER LANGUAGE plpgsql AS $synchronize_valid_to_until$
BEGIN
    -- This trigger synchronizes an inclusive end column (e.g., 'valid_to')
    -- with an exclusive end column ('valid_until') for date-based periods.
    -- The relationship is valid_until = valid_to + '1 day'.

    -- For INSERT operations
    IF TG_OP = 'INSERT' THEN
        IF NEW.valid_until IS NOT NULL AND NEW.valid_to IS NULL THEN
            NEW.valid_to := NEW.valid_until - INTERVAL '1 day';
        ELSIF NEW.valid_to IS NOT NULL AND NEW.valid_until IS NULL THEN
            NEW.valid_until := NEW.valid_to + INTERVAL '1 day';
        ELSIF NEW.valid_until IS NOT NULL AND NEW.valid_to IS NOT NULL THEN
            IF NEW.valid_to != (NEW.valid_until - INTERVAL '1 day') THEN
                RAISE EXCEPTION 'On INSERT, valid_to and valid_until are inconsistent. Expected valid_to = valid_until - 1 day. Got valid_to=%, valid_until=%', NEW.valid_to, NEW.valid_until;
            END IF;
        -- If both are NULL, do nothing, let table constraints handle it.
        END IF;

    -- For UPDATE operations
    ELSIF TG_OP = 'UPDATE' THEN
        -- Case 1: Both columns are explicitly changed.
        IF NEW.valid_until IS DISTINCT FROM OLD.valid_until AND NEW.valid_to IS DISTINCT FROM OLD.valid_to THEN
            IF NEW.valid_until IS NULL OR NEW.valid_to IS NULL THEN
                RAISE EXCEPTION 'On UPDATE, when changing both valid_to and valid_until, neither can be set to NULL.';
            END IF;
            IF NEW.valid_to != (NEW.valid_until - INTERVAL '1 day') THEN
                RAISE EXCEPTION 'On UPDATE, conflicting explicit values for valid_to and valid_until. With valid_until=%, expected valid_to=%. Got valid_to=%', 
                                 NEW.valid_until, NEW.valid_until - INTERVAL '1 day', NEW.valid_to;
            END IF;
        -- Case 2: Only valid_until is explicitly changed.
        ELSIF NEW.valid_until IS DISTINCT FROM OLD.valid_until THEN
            IF NEW.valid_until IS NULL THEN
                RAISE EXCEPTION 'On UPDATE, valid_until cannot be set to NULL.';
            END IF;
            NEW.valid_to := NEW.valid_until - INTERVAL '1 day';
        -- Case 3: Only valid_to is explicitly changed.
        ELSIF NEW.valid_to IS DISTINCT FROM OLD.valid_to THEN
            IF NEW.valid_to IS NULL THEN
                RAISE EXCEPTION 'On UPDATE, valid_to cannot be set to NULL.';
            END IF;
            NEW.valid_until := NEW.valid_to + INTERVAL '1 day';
        -- Case 4: Neither is being distinctly changed. Check for consistency if they are not NULL.
        ELSE
            IF NEW.valid_until IS NOT NULL AND NEW.valid_to IS NOT NULL THEN
                 IF NEW.valid_to != (NEW.valid_until - INTERVAL '1 day') THEN
                     RAISE EXCEPTION 'On UPDATE, existing valid_to and valid_until are inconsistent. Got valid_to=%, valid_until=%', NEW.valid_to, NEW.valid_until;
                 END IF;
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$synchronize_valid_to_until$;
