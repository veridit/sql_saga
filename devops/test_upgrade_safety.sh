#!/usr/bin/env bash
# No databases, builds or extension installation are performed by this test.
set -u
REPO=$(cd "$(dirname "$0")/.." && pwd)
. "$REPO/devops/upgrade_safety.sh" || exit 2

ok() { return 0; }
fail() { return 1; }
die() { exit 2; }
passes=0
expect_scan() {
    local expected=$1 migration_sql=$2 actual
    if check_migration_no_cascade "$migration_sql"; then actual=0; else actual=$?; fi
    if [ "$actual" != "$expected" ]; then
        printf 'FAIL: expected %s, got %s for %s\n' "$expected" "$actual" "$migration_sql" >&2
        exit 1
    fi
    passes=$((passes + 1))
}

expect_scan 1 'DROP TABLE t CASCADE;'
expect_scan 1 'truncate table t cascade;'
expect_scan 1 'DrOp VIEW t CaScAdE;'
expect_scan 1 $'DROP TABLE t\nCASCADE;'
expect_scan 1 $'TRUNCATE\nTABLE t\nCASCADE;'
expect_scan 0 'DROP TABLE t RESTRICT;'
expect_scan 0 'DROP TABLE cascade_history;'
expect_scan 0 'DROP TABLE history_cascade;'
expect_scan 0 'DROP TABLE cascade2;'
expect_scan 0 'DROP TABLE t; SELECT cascade;'
expect_scan 0 'SELECT drop_cascade();'
expect_scan 0 'SELECT drop_cascade, cascade;'
expect_scan 0 'SELECT truncate_me(), cascade;'
expect_scan 0 'SELECT 1;'
expect_scan 0 ''

expect_migration() {
    local expected=$1 migration_sql=$2 sanitized
    sanitized=$(printf '%s\n' "$migration_sql" | executable_migration_sql) || exit 2
    expect_scan "$expected" "$sanitized"
}
expect_migration 0 $'DO $m$ BEGIN ALTER TABLE t DROP CONSTRAINT old_check, ADD CHECK (x <> \'CASCADE\'); END $m$;'
expect_migration 0 $'DO $m$ BEGIN RAISE NOTICE \'-- DROP TABLE t CASCADE;\'; END $m$;'
expect_migration 0 'DO $m$ BEGIN /* DROP TABLE t CASCADE; */ END $m$;'
expect_migration 0 'CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $fn$ BEGIN DROP TABLE t CASCADE; END $fn$;'
expect_migration 0 'CREATE PROCEDURE p() AS $fn$ BEGIN DROP TABLE t CASCADE; END $fn$ LANGUAGE plpgsql;'
expect_migration 0 'COMMENT ON TABLE t IS $comment$DROP TABLE t CASCADE;$comment$;'
expect_migration 0 $'-- DROP TABLE t CASCADE;\nSELECT 1;'
expect_migration 1 'DO $m$ BEGIN DROP TABLE t CASCADE; END $m$;'
expect_migration 1 'DO LANGUAGE plpgsql $m$ BEGIN TRUNCATE t CASCADE; END $m$;'
expect_migration 1 'DROP/* separator */TABLE t CASCADE;'

for grep_error in 2 127; do
    if (grep() { return "$grep_error"; }; check_migration_no_cascade 'SELECT 1;'); then actual=0; else actual=$?; fi
    if [ "$actual" != 2 ]; then
        printf 'FAIL: grep error %s became exit %s instead of harness error 2\n' "$grep_error" "$actual" >&2
        exit 1
    fi
    passes=$((passes + 1))
done
printf 'PASS: all %s upgrade safety scanner assertions.\n' "$passes"
