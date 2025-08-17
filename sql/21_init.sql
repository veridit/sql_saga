CREATE EXTENSION sql_saga CASCADE;

CREATE TABLE shifts (
  job_id INTEGER,
  worker_id INTEGER,
  valid_after timestamptz,
  valid_to timestamptz
);

CREATE TABLE houses (
  id INTEGER,
  assessment FLOAT,
  valid_after timestamptz,
  valid_to timestamptz
);

CREATE TABLE rooms (
  id INTEGER,
  house_id INTEGER,
  valid_after timestamptz,
  valid_to timestamptz
);

-- Before using sql_saga
\d rooms
\d houses
\d shifts

-- Verify that enable and disable each work correctly.
SELECT sql_saga.add_era('shifts', 'valid_after', 'valid_to');
SELECT sql_saga.add_era('houses', 'valid_after', 'valid_to', 'valid');
SELECT sql_saga.add_era('rooms', 'valid_after', 'valid_to');
TABLE sql_saga.era;

SELECT sql_saga.add_unique_key('shifts', ARRAY['job_id','worker_id'], 'valid');
SELECT sql_saga.add_unique_key('houses', ARRAY['id'], 'valid');
SELECT sql_saga.add_unique_key('rooms', ARRAY['id'], 'valid');
TABLE sql_saga.unique_keys;

SELECT sql_saga.add_foreign_key('rooms', ARRAY['house_id'], 'valid', 'houses_id_valid');
TABLE sql_saga.foreign_keys;

-- While sql_saga is active
\d rooms
\d houses
\d shifts

-- Remove sql_saga
SELECT sql_saga.drop_foreign_key('rooms', 'rooms_house_id_valid');
TABLE sql_saga.foreign_keys;

SELECT sql_saga.drop_unique_key('rooms', 'rooms_id_valid');
SELECT sql_saga.drop_unique_key('houses','houses_id_valid');
-- TODO: Simplify this API, to take the same parameters when created.
-- TODO: Detect and raise an error if there is no match in "sql_saga.unique_keys".
SELECT sql_saga.drop_unique_key('shifts', 'shifts_job_id_worker_id_valid');
TABLE sql_saga.unique_keys;

SELECT sql_saga.drop_era('rooms');
SELECT sql_saga.drop_era('houses');
SELECT sql_saga.drop_era('shifts');
TABLE sql_saga.era;

-- After removing sql_saga, it should be as before.
\d rooms
\d houses
\d shifts

-- Make convenience functions for later tests.
CREATE FUNCTION enable_sql_saga_for_shifts_houses_and_rooms() RETURNS void LANGUAGE plpgsql AS $EOF$
BEGIN
  PERFORM sql_saga.add_era('shifts', 'valid_after', 'valid_to');
  PERFORM sql_saga.add_unique_key('shifts', ARRAY['job_id','worker_id'], 'valid');

  PERFORM sql_saga.add_era('houses', 'valid_after', 'valid_to', 'valid');
  PERFORM sql_saga.add_unique_key('houses', ARRAY['id'], 'valid');

  PERFORM sql_saga.add_era('rooms', 'valid_after', 'valid_to');
  PERFORM sql_saga.add_unique_key('rooms', ARRAY['id'], 'valid');
  PERFORM sql_saga.add_foreign_key('rooms', ARRAY['house_id'], 'valid', 'houses_id_valid');
END;
$EOF$;

CREATE FUNCTION disable_sql_saga_for_shifts_houses_and_rooms() RETURNS void LANGUAGE plpgsql AS $EOF$
BEGIN
  -- Drop objects in reverse order of creation, checking for existence first
  -- to make the cleanup script robust against early test failures.
  IF EXISTS (SELECT 1 FROM sql_saga.foreign_keys WHERE foreign_key_name = 'rooms_house_id_valid') THEN
    PERFORM sql_saga.drop_foreign_key('rooms'::regclass, 'rooms_house_id_valid');
  END IF;

  IF EXISTS (SELECT 1 FROM sql_saga.unique_keys WHERE unique_key_name = 'rooms_id_valid') THEN
    PERFORM sql_saga.drop_unique_key('rooms'::regclass, 'rooms_id_valid');
  END IF;
  IF EXISTS (SELECT 1 FROM sql_saga.unique_keys WHERE unique_key_name = 'houses_id_valid') THEN
    PERFORM sql_saga.drop_unique_key('houses'::regclass,'houses_id_valid');
  END IF;
  IF EXISTS (SELECT 1 FROM sql_saga.unique_keys WHERE unique_key_name = 'shifts_job_id_worker_id_valid') THEN
    PERFORM sql_saga.drop_unique_key('shifts'::regclass, 'shifts_job_id_worker_id_valid');
  END IF;

  IF EXISTS (SELECT 1 FROM sql_saga.era WHERE table_oid = 'rooms'::regclass AND era_name = 'valid') THEN
    PERFORM sql_saga.drop_era('rooms'::regclass);
  END IF;
  IF EXISTS (SELECT 1 FROM sql_saga.era WHERE table_oid = 'houses'::regclass AND era_name = 'valid') THEN
    PERFORM sql_saga.drop_era('houses'::regclass);
  END IF;
  IF EXISTS (SELECT 1 FROM sql_saga.era WHERE table_oid = 'shifts'::regclass AND era_name = 'valid') THEN
    PERFORM sql_saga.drop_era('shifts'::regclass);
  END IF;
END;
$EOF$;

-- Test the convenience functions.
SELECT enable_sql_saga_for_shifts_houses_and_rooms();
SELECT disable_sql_saga_for_shifts_houses_and_rooms()
