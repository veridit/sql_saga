#!/usr/bin/env bash
# Regular-to-temporal foreign key cost: ON DELETE CASCADE, NO ACTION and child inserts, against native foreign keys
# (the measurement behind doc/decision-log.md, "ON DELETE CASCADE for Regular-to-Temporal Foreign Keys").
#
# Usage: devops/bench_regular_fk_cascade.sh            (N=10000 keys, 2 timeline rows each; best of 3 per case)
# Environment: libpq (PGHOST, PGPORT, PGUSER, ...) of a server with sql_saga installed or staged; N; PG_CONFIG.
# Creates and drops only the database bench_regular_fk_cascade_$$. Every timed statement is rolled back.
set -eu
BIN=$("${PG_CONFIG:-pg_config}" --bindir)
DB=bench_regular_fk_cascade_$$
N=${N:-10000}
trap '"$BIN/dropdb" --if-exists "$DB" >/dev/null 2>&1' EXIT HUP INT TERM
"$BIN/createdb" "$DB"
"$BIN/psql" -X -q -v ON_ERROR_STOP=1 -d "$DB" >/dev/null <<'SQL'
SET client_min_messages = warning;
CREATE EXTENSION sql_saga CASCADE;
CREATE SCHEMA bc;
-- temporal parent with ON DELETE CASCADE children
CREATE TABLE bc.lu (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('bc.lu', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('bc.lu', ARRAY['id'], key_type => 'primary', unique_key_name => 'bc_pk');
CREATE TABLE bc.note (id int PRIMARY KEY, lu_id int);
SELECT sql_saga.add_regular_foreign_key('bc.note', ARRAY['lu_id'], 'bc_pk', delete_action => 'CASCADE');
-- temporal parent with NO ACTION children
CREATE TABLE bc.free (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('bc.free', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('bc.free', ARRAY['id'], key_type => 'primary', unique_key_name => 'bc_free_pk');
CREATE TABLE bc.free_note (id int PRIMARY KEY, lu_id int);
SELECT sql_saga.add_regular_foreign_key('bc.free_note', ARRAY['lu_id'], 'bc_free_pk');
-- native baseline: plain parent, native ON DELETE CASCADE
CREATE TABLE bc.plain (id int PRIMARY KEY);
CREATE TABLE bc.plain_note (id int PRIMARY KEY, plain_id int REFERENCES bc.plain ON DELETE CASCADE);
CREATE INDEX ON bc.plain_note (plain_id);
SQL

run() { # label, setup, timed statement
    local best=999999 r t
    for r in 1 2 3; do
        t=$("$BIN/psql" -X -At -d "$DB" -v ON_ERROR_STOP=1 <<SQL
SET client_min_messages = warning;
BEGIN;
$2
SELECT extract(epoch FROM clock_timestamp())::numeric(20,6) AS t0 \gset
$3
SELECT round((extract(epoch FROM clock_timestamp()) - :t0) * 1000, 1);
ROLLBACK;
SQL
)
        t=$(echo "$t" | grep -E '^[0-9]+(\.[0-9]+)?$' | tail -1)
        best=$(echo "$t $best" | awk '{print ($1<$2)?$1:$2}')
    done
    printf '%-62s %8s ms  (%s rows/s)\n' "$1" "$best" "$(awk -v n="$N" -v t="$best" 'BEGIN{printf "%d", n*1000/t}')"
}
LU="INSERT INTO bc.lu (id, valid_from, valid_until) SELECT g, d, e FROM generate_series(1,$N) g, (VALUES ('2020-01-01'::date,'2022-01-01'::date),('2022-01-01','infinity')) v(d,e);"
FR="INSERT INTO bc.free (id, valid_from, valid_until) SELECT g, d, e FROM generate_series(1,$N) g, (VALUES ('2020-01-01'::date,'2022-01-01'::date),('2022-01-01','infinity')) v(d,e);"
run "sql_saga CASCADE, delete all rows of $N keys (2 notes each)" \
    "$LU INSERT INTO bc.note SELECT g, 1 + (g-1) % $N FROM generate_series(1, 2*$N) g;" \
    "DELETE FROM bc.lu;"
run "sql_saga CASCADE, delete one of two rows of $N keys (kept)" \
    "$LU INSERT INTO bc.note SELECT g, 1 + (g-1) % $N FROM generate_series(1, 2*$N) g;" \
    "DELETE FROM bc.lu WHERE valid_from = '2020-01-01';"
run "sql_saga NO ACTION, delete all rows of $N unreferenced keys" \
    "$FR" \
    "DELETE FROM bc.free;"
run "sql_saga NO ACTION, delete one of two rows of $N keys (kept)" \
    "$FR INSERT INTO bc.free_note SELECT g, 1 + (g-1) % $N FROM generate_series(1, 2*$N) g;" \
    "DELETE FROM bc.free WHERE valid_from = '2020-01-01';"
run "native PostgreSQL CASCADE, delete $N keys (2 notes each)" \
    "INSERT INTO bc.plain SELECT generate_series(1,$N); INSERT INTO bc.plain_note SELECT g, 1 + (g-1) % $N FROM generate_series(1, 2*$N) g;" \
    "DELETE FROM bc.plain;"
run "sql_saga regular FK, insert $((2*N)) child rows (check each)" \
    "$FR" \
    "INSERT INTO bc.free_note SELECT g, 1 + (g-1) % $N FROM generate_series(1, 2*$N) g;"
run "native FK, insert $((2*N)) child rows (check each)" \
    "INSERT INTO bc.plain SELECT generate_series(1,$N);" \
    "INSERT INTO bc.plain_note SELECT g, 1 + (g-1) % $N FROM generate_series(1, 2*$N) g;"
