#!/usr/bin/env bash
#
# Verify that ALTER FUNCTION attribution is isolated between concurrent sessions.
# Session 2 holds a ShareUpdateExclusiveLock on A's pg_proc object with COMMENT ON FUNCTION.
# Session 1 then fires ddl_command_start for an exact rename and blocks requesting the same object's
# AccessExclusiveLock. While that statement is paused before ddl_command_end, session 2 completes its
# whole exact ALTER on family B. pg_stat_activity and pg_locks assertions prove this ordering.
# Exit status: 0 all assertions pass, 1 assertion failure (RED), 2 harness/setup error.
# Compatible with macOS /bin/bash 3.2 and set -u.
#
set -u

PG_CONFIG=${PG_CONFIG:-pg_config}
PSQL="$($PG_CONFIG --bindir)/psql"
CREATEDB="$($PG_CONFIG --bindir)/createdb"
DROPDB="$($PG_CONFIG --bindir)/dropdb"
RUN_SUFFIX="${$}_${RANDOM}"
DB="saga_fa_${RUN_SUFFIX}"
APP_PREFIX="saga_fa_${RUN_SUFFIX}"
KEEP=${FUNCTION_ATTRIBUTION_KEEP:-0}

umask 022
WORK=$(mktemp -d "${TMPDIR:-/tmp}/saga_fa.XXXXXX") || exit 2
chmod 755 "$WORK"
SESSION_PIDS=""
CREATED=0

cleanup() {
    for pid in $SESSION_PIDS; do
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    done
    if [ "$KEEP" = 1 ]; then
        echo "logs: $WORK"
        echo "database kept: $DB"
    else
        if [ "$CREATED" = 1 ]; then
            "$DROPDB" --if-exists "$DB" >/dev/null 2>&1 || true
        fi
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT
trap 'exit 2' HUP INT TERM

die() {
    echo "HARNESS ERROR: $*" >&2
    echo "logs: $WORK" >&2
    exit 2
}

sql_file() {
    file=$1
    "$PSQL" -X --no-psqlrc --set=ON_ERROR_STOP=1 --dbname="$DB" --file="$file"
}

cat >"$WORK/setup.sql" <<'SQL'
SET client_min_messages = warning;
CREATE EXTENSION sql_saga CASCADE;
CREATE SCHEMA fa;
SET search_path = fa, public;

CREATE TABLE family_a (id integer PRIMARY KEY, payload text);
CREATE TABLE family_b (id integer PRIMARY KEY, payload text);
SELECT sql_saga.add_system_versioning('family_a');
SELECT sql_saga.add_system_versioning('family_b');

CREATE TABLE barrier (
    singleton boolean PRIMARY KEY DEFAULT true,
    start_one boolean NOT NULL DEFAULT false,
    run_two boolean NOT NULL DEFAULT false,
    release_two boolean NOT NULL DEFAULT false
);
INSERT INTO barrier(singleton) VALUES (true);
CREATE TABLE results (
    test_case text PRIMARY KEY,
    passed boolean NOT NULL,
    guc_empty boolean NOT NULL,
    detail text NOT NULL
);

CREATE FUNCTION attempt_exact(test_name text, ddl text, expected_message text)
RETURNS void LANGUAGE plpgsql AS $attempt_exact$
DECLARE
    caught_state text;
    caught_message text;
BEGIN
    BEGIN
        EXECUTE ddl;
        RAISE EXCEPTION USING ERRCODE = 'ZX001', MESSAGE = 'command succeeded';
    EXCEPTION
        WHEN SQLSTATE 'ZX001' THEN
            caught_state := 'NO_ERROR';
            caught_message := 'command succeeded';
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS caught_state = RETURNED_SQLSTATE, caught_message = MESSAGE_TEXT;
    END;

    INSERT INTO results(test_case, passed, guc_empty, detail)
    VALUES (
        test_name,
        caught_state = 'P0001' AND caught_message = expected_message,
        coalesce(current_setting('sql_saga.alter_function_resolved', true), '') = '',
        caught_state || ': ' || caught_message
    );
END;
$attempt_exact$;

CREATE FUNCTION attempt_allowed(test_name text, ddl text)
RETURNS void LANGUAGE plpgsql AS $attempt_allowed$
DECLARE
    caught_state text := 'NO_ERROR';
    caught_message text := 'command succeeded';
BEGIN
    BEGIN
        EXECUTE ddl;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS caught_state = RETURNED_SQLSTATE, caught_message = MESSAGE_TEXT;
    END;

    INSERT INTO results(test_case, passed, guc_empty, detail)
    VALUES (
        test_name,
        caught_state = 'NO_ERROR',
        coalesce(current_setting('sql_saga.alter_function_resolved', true), '') = '',
        caught_state || ': ' || caught_message
    );
END;
$attempt_allowed$;
SQL

cat >"$WORK/session_one.sql" <<SQL
\\set ON_ERROR_STOP on
SET application_name = '${APP_PREFIX}_one';
SELECT coalesce(current_setting('sql_saga.alter_function_resolved', true), '') = '' AS initial_guc_empty \gset
\if :initial_guc_empty
\else
    \quit 2
\endif
SET client_min_messages = warning;
SET search_path = fa, public;
SET application_name = '${APP_PREFIX}_one_ready';
DO \$barrier\$
BEGIN
    WHILE NOT (SELECT start_one FROM fa.barrier WHERE singleton) LOOP
        PERFORM pg_sleep(0.05);
    END LOOP;
END
\$barrier\$;
SET application_name = '${APP_PREFIX}_one_altering';
SELECT fa.attempt_exact(
    'session 1 exact A as_of',
    'ALTER FUNCTION fa.family_a__as_of(timestamptz) RENAME TO forbidden_a_as_of',
    'cannot drop or rename function "fa.family_a__as_of(timestamp with time zone)" because it is used in SYSTEM VERSIONING for table "fa.family_a"'
);
SELECT fa.attempt_allowed(
    'session 1 allowed B from_to',
    'ALTER FUNCTION fa.family_b__from_to(timestamptz,timestamptz) COST 101'
);
DO \$nested\$
DECLARE
    caught_state text := 'NO_ERROR';
    caught_message text := 'command succeeded';
BEGIN
    BEGIN
        EXECUTE 'ALTER ROUTINE fa.family_b__between_symmetric(timestamptz,timestamptz) COST 102';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS caught_state = RETURNED_SQLSTATE, caught_message = MESSAGE_TEXT;
    END;
    INSERT INTO fa.results(test_case, passed, guc_empty, detail)
    VALUES (
        'session 1 nested DO B between_symmetric',
        caught_state = 'NO_ERROR',
        coalesce(current_setting('sql_saga.alter_function_resolved', true), '') = '',
        caught_state || ': ' || caught_message
    );
END
\$nested\$;
INSERT INTO fa.results VALUES (
    'session 1 final GUC empty',
    coalesce(current_setting('sql_saga.alter_function_resolved', true), '') = '',
    coalesce(current_setting('sql_saga.alter_function_resolved', true), '') = '',
    'post-session assertion'
);
SQL

cat >"$WORK/session_two.sql" <<SQL
\\set ON_ERROR_STOP on
SET application_name = '${APP_PREFIX}_two';
SELECT coalesce(current_setting('sql_saga.alter_function_resolved', true), '') = '' AS initial_guc_empty \gset
\if :initial_guc_empty
\else
    \quit 2
\endif
SET client_min_messages = warning;
SET search_path = fa, public;
BEGIN;
COMMENT ON FUNCTION fa.family_a__as_of(timestamptz) IS 'concurrency lock holder';
INSERT INTO fa.results VALUES (
    'session 2 target lock holder',
    true,
    coalesce(current_setting('sql_saga.alter_function_resolved', true), '') = '',
    'COMMENT acquired the target function lock'
);
SET application_name = '${APP_PREFIX}_two_holding';
DO \$barrier\$
BEGIN
    WHILE NOT (SELECT start_one FROM fa.barrier WHERE singleton) LOOP
        PERFORM pg_sleep(0.05);
    END LOOP;
END
\$barrier\$;
SET application_name = '${APP_PREFIX}_two_waiting';
DO \$barrier\$
BEGIN
    WHILE NOT (SELECT run_two FROM fa.barrier WHERE singleton) LOOP
        PERFORM pg_sleep(0.05);
    END LOOP;
END
\$barrier\$;
SELECT fa.attempt_exact(
    'session 2 exact B between',
    'ALTER ROUTINE fa.family_b__between(timestamptz,timestamptz) RENAME TO forbidden_b_between',
    'cannot drop or rename function "fa.family_b__between(timestamp with time zone,timestamp with time zone)" because it is used in SYSTEM VERSIONING for table "fa.family_b"'
);
DO \$nested\$
DECLARE
    caught_state text := 'NO_ERROR';
    caught_message text := 'command succeeded';
BEGIN
    BEGIN
        EXECUTE 'ALTER FUNCTION fa.family_a__from_to(timestamptz,timestamptz) COST 104';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS caught_state = RETURNED_SQLSTATE, caught_message = MESSAGE_TEXT;
    END;
    INSERT INTO fa.results(test_case, passed, guc_empty, detail)
    VALUES (
        'session 2 nested DO A from_to',
        caught_state = 'NO_ERROR',
        coalesce(current_setting('sql_saga.alter_function_resolved', true), '') = '',
        caught_state || ': ' || caught_message
    );
END
\$nested\$;
INSERT INTO fa.results VALUES (
    'session 2 final GUC empty',
    coalesce(current_setting('sql_saga.alter_function_resolved', true), '') = '',
    coalesce(current_setting('sql_saga.alter_function_resolved', true), '') = '',
    'post-session assertion'
);
SET application_name = '${APP_PREFIX}_two_completed';
DO \$barrier\$
BEGIN
    WHILE NOT (SELECT release_two FROM fa.barrier WHERE singleton) LOOP
        PERFORM pg_sleep(0.05);
    END LOOP;
END
\$barrier\$;
COMMIT;
SQL

cat >"$WORK/report.sql" <<'SQL'
\set QUIET 1
\pset format unaligned
\pset tuples_only on
SELECT CASE WHEN passed AND guc_empty THEN 'ok  ' ELSE 'FAIL' END || '  ' || test_case ||
       CASE WHEN passed AND guc_empty THEN '' ELSE ' (' || detail || ', guc_empty=' || guc_empty || ')' END
FROM fa.results
ORDER BY test_case;
SELECT count(*) FILTER (WHERE passed AND guc_empty) || '|' || count(*)
FROM fa.results;
SQL

echo "== function attribution concurrency"
"$CREATEDB" "$DB" >"$WORK/createdb.log" 2>&1 || die "creating database $DB"
CREATED=1
sql_file "$WORK/setup.sql" >"$WORK/setup.log" 2>&1 || die "setting up fixtures"

sql_file "$WORK/session_one.sql" >"$WORK/session_one.log" 2>&1 &
pid_one=$!
SESSION_PIDS="$pid_one"
sql_file "$WORK/session_two.sql" >"$WORK/session_two.log" 2>&1 &
pid_two=$!
SESSION_PIDS="$SESSION_PIDS $pid_two"

wait_for_activity() {
    description=$1
    expected=$2
    predicate=$3
    observed=0
    attempt=0
    while [ "$attempt" -lt 200 ]; do
        observed=$("$PSQL" -X --no-psqlrc --tuples-only --no-align --dbname="$DB" --command="SELECT count(*) FROM pg_stat_activity AS a WHERE a.datname = current_database() AND ($predicate)" 2>"$WORK/poll.err") || die "polling $description"
        if [ "$observed" = "$expected" ]; then
            echo "    ok    $description"
            return 0
        fi
        sleep 0.05
        attempt=$((attempt + 1))
    done
    die "$description did not reach count $expected before timeout (observed $observed)"
}

wait_for_activity "session 2 holds the target function object lock" 1 \
    "a.application_name = '${APP_PREFIX}_two_holding' AND a.wait_event = 'PgSleep'"
"$PSQL" -X --no-psqlrc --set=ON_ERROR_STOP=1 --dbname="$DB" --command="UPDATE fa.barrier SET start_one = true WHERE singleton" >"$WORK/start_one.log" 2>&1 || die "starting session one's exact ALTER"

wait_for_activity "session 1 waits on the target pg_proc object lock after ddl_command_start" 1 \
    "a.application_name = '${APP_PREFIX}_one_altering' AND a.wait_event = 'object' AND EXISTS (SELECT 1 FROM pg_locks AS waiting WHERE waiting.pid = a.pid AND waiting.locktype = 'object' AND NOT waiting.granted AND waiting.classid = 'pg_proc'::regclass AND waiting.objid = 'fa.family_a__as_of(timestamptz)'::regprocedure::oid)"
wait_for_activity "session 2 is paused before its exact ALTER" 1 \
    "a.application_name = '${APP_PREFIX}_two_waiting' AND a.wait_event = 'PgSleep'"
"$PSQL" -X --no-psqlrc --set=ON_ERROR_STOP=1 --dbname="$DB" --command="UPDATE fa.barrier SET run_two = true WHERE singleton" >"$WORK/run_two.log" 2>&1 || die "starting session two's exact ALTER"

wait_for_activity "session 2 completed its ALTER while session 1 remained lock-blocked" 2 \
    "(a.application_name = '${APP_PREFIX}_one_altering' AND a.wait_event_type = 'Lock') OR (a.application_name = '${APP_PREFIX}_two_completed' AND a.wait_event = 'PgSleep')"
"$PSQL" -X --no-psqlrc --set=ON_ERROR_STOP=1 --dbname="$DB" --command="UPDATE fa.barrier SET release_two = true WHERE singleton" >"$WORK/release_two.log" 2>&1 || die "releasing session two's target function lock"

status_one=0
status_two=0
wait "$pid_one" || status_one=$?
wait "$pid_two" || status_two=$?
SESSION_PIDS=""
[ "$status_one" = 0 ] || die "session one failed (see $WORK/session_one.log)"
[ "$status_two" = 0 ] || die "session two failed (see $WORK/session_two.log)"

echo "    ok    both sessions completed"
report=$(sql_file "$WORK/report.sql") || die "reporting assertions"
printf '%s\n' "$report" | sed '$d' | sed 's/^/    /'
summary=$(printf '%s\n' "$report" | tail -n 1)
passed=${summary%%|*}
total=${summary#*|}
if [ "$total" != 8 ]; then
    die "expected 8 assertion rows, got $total"
fi
if [ "$passed" != "$total" ]; then
    echo
    echo "RED. $passed of $total concurrency assertions passed."
    echo "logs: $WORK"
    exit 1
fi

echo
echo "GREEN. All $total concurrency assertions passed."
exit 0
