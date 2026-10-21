#!/usr/bin/env bash
#
# Concurrency contract of regular (non-temporal) to temporal foreign keys (design B, sql/129, 1.3).
# A regular reference requires that its key exists in the parent at some time. Whatever the isolation level and
# interleaving, committed data must never end with a child row whose non-NULL key no parent row has.
#
# Sessions are driven step by step. A "driven" session is a psql reading its commands from a FIFO; every step sends
# its SQL followed by \echo of a unique acknowledgement token, and the harness waits until exactly that token is in
# the session's output, so the step (snapshot establishment, statement, COMMIT) has completed before the other
# session acts. A statement expected to block on a lock carries a unique marker comment; the harness waits until
# pg_stat_activity shows a backend running that marked query with wait_event_type Lock (or its process has ended,
# which the following checks then report). Waiting is bounded polling; no outcome depends on a fixed timing
# assumption, and a wait that expires is a harness error (exit 2), never a pass.
# A refused side may fail with a foreign key violation (23503) or a serialization failure (40001).
#
#   rr_parent_delete         REPEATABLE READ parent DELETE of the only row of a key, snapshot taken before a
#                            concurrent child INSERT of that key committed: refused, no orphan
#   rc_parent_delete         the same in READ COMMITTED (control): refused
#   rr_parent_key_update     REPEATABLE READ UPDATE of the parent key, same interleaving: refused
#   rc_parent_key_update     the same in READ COMMITTED (control): refused
#   rr_parent_segment_delete REPEATABLE READ DELETE of one of two timeline rows of the key, same interleaving:
#                            accepted (another row keeps the key), no orphan
#   rr_child_insert          REPEATABLE READ child INSERT, snapshot taken before a concurrent DELETE of the only
#                            parent row committed: refused
#   child_lock_update        an uncommitted child INSERT blocks a parent key UPDATE until it commits; then refused
#   child_lock_delete        the same for a parent DELETE
#   segment_deletes_rc/rr    two sessions each delete one of the two timeline rows of a referenced key: the
#                            second blocks, then is refused; one row stays
#
# ON DELETE CASCADE (1.4), a second child table rfc.cchild. Same contract (never an orphan), and the child goes with
# the last parent row of its key, and only then:
#   cascade_rc_after_child_commit  READ COMMITTED parent DELETE after a concurrent child INSERT committed: the
#                                  child it could not see when it began is cascaded too
#   cascade_rr_after_child_commit  the same in REPEATABLE READ: refused with 40001 (as native CASCADE), both kept
#   cascade_child_lock             an open child INSERT blocks the parent DELETE; once committed it is cascaded
#   cascade_rr_child_insert        REPEATABLE READ child INSERT after the key's last row was deleted: refused
#   cascade_segment_deletes_rc/rr  two sessions each delete one of key 2's rows: the first keeps the child, the
#                                  second (the last row) cascades it
#
# Safety: runs only against a private cluster the caller names explicitly.
#   PGHOST, PGPORT                          required, no defaults
#   REGULAR_FK_CONCURRENCY_DATA_DIRECTORY   required; must equal the server's data_directory (identity guard)
# PGDATABASE, PGSERVICE, PGSERVICEFILE, PGAPPNAME, PGHOSTADDR, PGOPTIONS and PGTARGETSESSIONATTRS are ignored
# (unset). Every step reports its SQLSTATE (psql VERBOSITY sqlstate); outcomes are classified by exact SQLSTATE.
# A scratch database saga_rfc_<pid>_<n> is created and dropped (kept, with the logs, when a check fails, on a
# harness error or signal, or when REGULAR_FK_CONCURRENCY_KEEP=1).
# Work files go to $JCODE_SCRATCH_DIR, else <repo>/tmp.
# Exit status: 0 all checks pass, 1 a check failed (RED), 2 harness/setup error. macOS /bin/bash 3.2 compatible.
#
set -u
unset PGDATABASE PGSERVICE PGSERVICEFILE PGAPPNAME PGHOSTADDR PGOPTIONS PGTARGETSESSIONATTRS
# Every session: bounded connect, statement and lock waits. A timeout is a harness error (exit 2), never a refusal.
export PGCONNECT_TIMEOUT=10
# REGULAR_FK_CONCURRENCY_LOCK_TIMEOUT (default 40s) exists for the harness's own timeout control run.
export PGOPTIONS="-c statement_timeout=60s -c lock_timeout=${REGULAR_FK_CONCURRENCY_LOCK_TIMEOUT:-40s} -c idle_in_transaction_session_timeout=120s"

if [ -z "${PGHOST:-}" ] || [ -z "${PGPORT:-}" ]; then
    echo "HARNESS ERROR: set PGHOST and PGPORT explicitly to the private test cluster" >&2
    exit 2
fi
if [ -z "${REGULAR_FK_CONCURRENCY_DATA_DIRECTORY:-}" ]; then
    echo "HARNESS ERROR: set REGULAR_FK_CONCURRENCY_DATA_DIRECTORY to the data directory of the private test cluster" >&2
    exit 2
fi

PG_CONFIG=${PG_CONFIG:-pg_config}
PSQL="$($PG_CONFIG --bindir)/psql"
CREATEDB="$($PG_CONFIG --bindir)/createdb"
DROPDB="$($PG_CONFIG --bindir)/dropdb"
RUN_SUFFIX="${$}_${RANDOM}"
DB="saga_rfc_${RUN_SUFFIX}"
APP="saga_rfc_${RUN_SUFFIX}"
KEEP=${REGULAR_FK_CONCURRENCY_KEEP:-0}
REPO=$(cd "$(dirname "$0")/.." && pwd)
SCRATCH=${JCODE_SCRATCH_DIR:-$REPO/tmp}
mkdir -p "$SCRATCH" || exit 2
WORK=$(mktemp -d "$SCRATCH/saga_rfc.XXXXXX") || exit 2
CREATED=0
FAILURES=0
DRIVEN=""     # names of open driven sessions
DRIVEN_PIDS="" # pids of open driven sessions
BACKGROUND="" # pids of background sessions

# wait_pid <pid> <tenths of a second>: bounded wait for a child process; sets WAIT_RC to its exit status.
# Returns 1 (process still running) when the bound expires.
wait_pid() {
    i=0
    while kill -0 "$1" 2>/dev/null; do
        [ "$i" -ge "$2" ] && return 1
        i=$((i + 1))
        sleep 0.1
    done
    wait "$1" 2>/dev/null
    WAIT_RC=$?
    return 0
}

# stop_pid <pid>: TERM, bounded wait, then KILL and a bounded wait; never hangs.
stop_pid() {
    kill -0 "$1" 2>/dev/null || { wait "$1" 2>/dev/null; return 0; }
    kill "$1" 2>/dev/null
    wait_pid "$1" 50 && return 0
    kill -9 "$1" 2>/dev/null
    wait_pid "$1" 50 || echo "cleanup: process $1 did not exit" >&2
}

cleanup() {
    trap '' HUP INT TERM PIPE
    for name in $DRIVEN; do
        eval "fd=\${FD_$name:-}"
        [ -n "$fd" ] && eval "exec $fd>&-" 2>/dev/null
    done
    for pid in $DRIVEN_PIDS $BACKGROUND; do
        stop_pid "$pid"
    done
    if [ "$KEEP" = 1 ]; then
        echo "logs: $WORK"
        [ "$CREATED" = 1 ] && echo "database kept: $DB"
    else
        if [ "$CREATED" = 1 ]; then
            "$DROPDB" --maintenance-db=postgres --if-exists "$DB" >/dev/null 2>&1 || true
        fi
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT
trap 'echo "HARNESS ERROR: interrupted by a signal" >&2; KEEP=1; exit 2' HUP INT TERM PIPE

# die may run inside $(...) (outcome, q, run_session): the flag file makes the main shell exit 2 at the next check.
die() {
    echo "HARNESS ERROR: $*" >&2
    : >"$WORK/harness_error"
    KEEP=1
    exit 2
}
harness_ok() { [ ! -e "$WORK/harness_error" ] || { KEEP=1; exit 2; }; }

PSQL_OPTS="-X --no-psqlrc --set=VERBOSITY=sqlstate"
pq() { "$PSQL" $PSQL_OPTS -q -At --set=ON_ERROR_STOP=1 --dbname=postgres -c "$1"; }
# q: helper query in the scratch database; any failure is a harness error.
q() {
    q_out=$("$PSQL" $PSQL_OPTS -q -At --set=ON_ERROR_STOP=1 --dbname="$DB" -c "$1" 2>&1) || die "query failed: $1: $q_out"
    echo "$q_out"
}

actual_dir=$(pq "SHOW data_directory") || die "cannot connect to PGHOST=$PGHOST PGPORT=$PGPORT"
want_dir=$(cd "$REGULAR_FK_CONCURRENCY_DATA_DIRECTORY" 2>/dev/null && pwd -P) || die "no such directory: $REGULAR_FK_CONCURRENCY_DATA_DIRECTORY"
actual_dir=$(cd "$actual_dir" 2>/dev/null && pwd -P) || die "server data_directory $actual_dir is not readable here"
[ "$actual_dir" = "$want_dir" ] || die "server data_directory $actual_dir is not $want_dir; refusing to run"

# open_session <name>: a psql reading commands from a FIFO. File descriptors 5..9 (bash 3.2 allows single
# digits) are taken from a pool and returned by close_session.
FREE_FDS="5 6 7 8 9"
ACK=0
open_session() {
    fd=$(echo $FREE_FDS | cut -d' ' -f1)
    [ -n "$fd" ] || die "no free file descriptor for session $1"
    FREE_FDS=$(echo $FREE_FDS | cut -s -d' ' -f2-)
    mkfifo "$WORK/$1.fifo" || die "mkfifo failed"
    # Opened read-write first: never blocks, even if psql fails to start (the ACK wait then expires).
    eval "exec $fd<>\"\$WORK/\$1.fifo\"" || die "cannot open fifo for $1"
    eval "FD_$1=$fd"
    PGAPPNAME="$APP-$1" "$PSQL" $PSQL_OPTS --echo-all --dbname="$DB" <"$WORK/$1.fifo" >"$WORK/$1.log" 2>&1 &
    eval "PID_$1=$!"
    eval "STATES_$1="
    DRIVEN="$DRIVEN $1"
    DRIVEN_PIDS="$DRIVEN_PIDS $!"
    step "$1" "SELECT 'connected';"
}

# wait_for <description> <command...>: poll (bounded, about 30 s) until the command succeeds.
wait_for() {
    what=$1
    shift
    i=0
    while [ "$i" -lt 1500 ]; do
        "$@" && return 0
        i=$((i + 1))
        sleep 0.02
    done
    die "timed out waiting for $what; logs: $WORK"
}

has_line() { grep -qx "$2" "$WORK/$1.log"; }

# has_ack <name> <token>: the session printed "<token> <SQLSTATE>"; sets STEP_STATE.
has_ack() {
    line=$(grep -E "^$2 [0-9A-Z]{5}\$" "$WORK/$1.log") || return 1
    STEP_STATE=${line#"$2 "}
}

# try_step <name> <sql>: send the SQL, then \echo a unique token with the SQLSTATE of the step's last error
# (00000 when none), and wait (bounded) for that line. The state is recorded for outcome. A statement or lock
# timeout (57014, 55P03) or a lost connection is a harness error.
try_step() {
    ACK=$((ACK + 1))
    token="ACK_${RUN_SUFFIX}_$ACK"
    eval "fd=\$FD_$1"
    printf '\\set LAST_ERROR_SQLSTATE 00000\n%s\n\\echo %s :LAST_ERROR_SQLSTATE\n' "$2" "$token" >&"$fd" \
        || die "cannot write to session $1"
    wait_for "session $1 to complete step $ACK" has_ack "$1" "$token"
    case $STEP_STATE in
        57014 | 55P03 | 08*) die "session $1 step $ACK ended with SQLSTATE $STEP_STATE (timeout or connection); logs: $WORK" ;;
    esac
    eval "STATES_$1=\"\$STATES_$1 \$STEP_STATE\""
}

# step <name> <sql>: a step that must succeed; any error is a harness error.
step() {
    try_step "$1" "$2"
    [ "$STEP_STATE" = 00000 ] || die "session $1 step $ACK failed with SQLSTATE $STEP_STATE; logs: $WORK"
}

close_session() {
    eval "fd=\$FD_$1; pid=\$PID_$1"
    printf '\\q\n' >&"$fd" || die "cannot write to session $1"
    eval "exec $fd>&-"
    wait_pid "$pid" 300 || die "session $1 did not exit; logs: $WORK"
    [ "$WAIT_RC" = 0 ] || die "session $1 exited with status $WAIT_RC; logs: $WORK"
    DRIVEN_PIDS=$(echo " $DRIVEN_PIDS " | sed "s/ $pid / /")
    FREE_FDS="$FREE_FDS $fd"
    eval "FD_$1="
    DRIVEN=$(echo "$DRIVEN" | sed "s/ $1\$//; s/ $1 / /")
}

# run_session <name> <sql>: a separate session run to completion (autocommit unless the SQL says otherwise).
# Prints its exit status (0 success, 1/3 SQL error); 2 (connection lost) or a timeout SQLSTATE is a harness error.
run_session() {
    PGAPPNAME="$APP-$1" "$PSQL" $PSQL_OPTS --set=ON_ERROR_STOP=1 --dbname="$DB" -c "$2" >"$WORK/$1.log" 2>&1
    rc=$?
    case $rc in
        0 | 1 | 3) ;;
        *) die "session $1 exited with status $rc; logs: $WORK" ;;
    esac
    case $(sqlstate_of "$1") in
        57014 | 55P03 | 08*) die "session $1 hit a timeout or connection SQLSTATE; logs: $WORK" ;;
    esac
    echo $rc
}

# sqlstate_of <name>: the SQLSTATE of the first ERROR line of a one-shot session's log, or nothing.
sqlstate_of() {
    sed -n 's/^\(psql:[^ ]* \)\{0,1\}ERROR:  \([0-9A-Z]\{5\}\)$/\2/p' "$WORK/$1.log" | head -n 1
}

# start_background <name> <sql>: run a session in the background. Its SQL starts with the marker comment
# /* <marker of name> */ so that await_blocked can find its backend.
marker() { echo "rfc_marker_${RUN_SUFFIX}_$1"; }
start_background() {
    PGAPPNAME="$APP-$1" "$PSQL" $PSQL_OPTS --set=ON_ERROR_STOP=1 --dbname="$DB" \
        -c "/* $(marker "$1") */ $2" >"$WORK/$1.log" 2>&1 &
    eval "PID_$1=$!"
    BACKGROUND="$BACKGROUND $!"
}

# blocked_or_done <name>: the marked statement waits on a lock, or the session process has ended.
blocked_or_done() {
    eval "pid=\$PID_$1"
    kill -0 "$pid" 2>/dev/null || return 0
    [ "$(q "SELECT count(*) FROM pg_stat_activity AS a
            WHERE a.query LIKE '%$(marker "$1")%' AND a.wait_event_type = 'Lock' AND a.pid <> pg_backend_pid()")" = 1 ]
}

await_blocked() {
    wait_for "session $1 to block on a lock or finish" blocked_or_done "$1"
}

# lock_waiting <name>: 'waiting' when the marked statement waits on a lock now, else 'not waiting'.
lock_waiting() {
    if [ "$(q "SELECT count(*) FROM pg_stat_activity AS a
               WHERE a.query LIKE '%$(marker "$1")%' AND a.wait_event_type = 'Lock' AND a.pid <> pg_backend_pid()")" = 1 ]; then
        echo waiting
    else
        echo "not waiting"
    fi
}

# finish_background <name>: bounded wait; records the exit status in RC_<name>.
finish_background() {
    eval "pid=\$PID_$1"
    wait_pid "$pid" 300 || die "background session $1 did not finish; logs: $WORK"
    BACKGROUND=$(echo " $BACKGROUND " | sed "s/ $pid / /")
    eval "RC_$1=$WAIT_RC"
}

check() {
    harness_ok
    if [ "$2" = "$3" ]; then
        echo "    ok    $1"
    else
        echo "    FAIL  $1 (expected '$2', got '$3')"
        FAILURES=$((FAILURES + 1))
    fi
}

# classify <sqlstates...>: accepted (all 00000), refused (the only error is 23503 or 40001) or other error.
# A timeout or connection SQLSTATE is a harness error.
classify() {
    result=accepted
    for s in "$@"; do
        case $s in
            00000) ;;
            23503 | 40001) [ "$result" = accepted ] && result=refused ;;
            57014 | 55P03 | 08*) die "SQLSTATE $s (timeout or connection); logs: $WORK" ;;
            *) result="other error: SQLSTATE $s" ;;
        esac
    done
    echo "$result"
}

# outcome <name>: the classified result of a driven session (its recorded per-step SQLSTATEs) or of a finished
# one-shot/background session (exit status and SQLSTATE). An exit status other than 0, 1 or 3 is a harness error.
outcome() {
    eval "states=\${STATES_$1-unset}"
    if [ "$states" != unset ]; then
        classify $states
        return
    fi
    eval "rc=\${RC_$1-unset}"
    [ "$rc" != unset ] || die "no recorded status for session $1"
    s=$(sqlstate_of "$1")
    case $rc in
        0) [ -z "$s" ] || die "session $1 succeeded but logged SQLSTATE $s"; echo accepted ;;
        1 | 3) [ -n "$s" ] || die "session $1 exited $rc without an ERROR SQLSTATE; logs: $WORK"; classify "$s" ;;
        *) die "session $1 exited with status $rc; logs: $WORK" ;;
    esac
}

orphans() {
    q "SELECT (SELECT count(*) FROM rfc.child AS c
               WHERE c.parent_id IS NOT NULL AND NOT EXISTS (SELECT FROM rfc.parent AS p WHERE p.id = c.parent_id))
            + (SELECT count(*) FROM rfc.cchild AS c
               WHERE c.parent_id IS NOT NULL AND NOT EXISTS (SELECT FROM rfc.parent AS p WHERE p.id = c.parent_id))"
}

cchildren() { q "SELECT count(*) FROM rfc.cchild WHERE parent_id = $1"; }

"$CREATEDB" --maintenance-db=postgres "$DB" >"$WORK/createdb.log" 2>&1 || die "createdb failed: $(cat "$WORK/createdb.log")"
CREATED=1
cat >"$WORK/setup.sql" <<'SQL'
SET client_min_messages = warning;
CREATE EXTENSION sql_saga CASCADE;
CREATE SCHEMA rfc;
CREATE TABLE rfc.parent (id int NOT NULL, label text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('rfc.parent', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('rfc.parent', ARRAY['id'], key_type => 'primary', unique_key_name => 'rfc_parent_id_valid');
CREATE TABLE rfc.child (seq int PRIMARY KEY, parent_id int, payload text);
SELECT sql_saga.add_regular_foreign_key('rfc.child', ARRAY['parent_id'], 'rfc_parent_id_valid');
-- ON DELETE CASCADE child (1.4): goes with the last parent row of its key.
CREATE TABLE rfc.cchild (seq int PRIMARY KEY, parent_id int, payload text);
SELECT sql_saga.add_regular_foreign_key('rfc.cchild', ARRAY['parent_id'], 'rfc_parent_id_valid', delete_action => 'CASCADE');
SQL
"$PSQL" -X --no-psqlrc -q --set=ON_ERROR_STOP=1 --dbname="$DB" --file="$WORK/setup.sql" >"$WORK/setup.log" 2>&1 \
    || die "setup failed: $(cat "$WORK/setup.log")"

# reset: key 1 has one timeline row, key 2 has two; no children.
reset() {
    q "TRUNCATE rfc.child, rfc.cchild; DELETE FROM rfc.parent;
       INSERT INTO rfc.parent (id, label, valid_from, valid_until) VALUES
         (1, 'a', '2020-01-01', 'infinity'),
         (2, 'a', '2020-01-01', '2022-01-01'), (2, 'b', '2022-01-01', 'infinity')" >/dev/null || die "reset failed"
}

# parent_after_child_commit <case> <isolation> <parent statement>: the parent session takes its snapshot, a child
# row for the key is inserted and committed by another session, then the parent statement runs and commits.
parent_after_child_commit() {
    name=$1
    open_session "$name"
    step "$name" "BEGIN ISOLATION LEVEL $2; SELECT count(*) FROM rfc.child;"
    check "$name: the concurrent child INSERT commits" 0 \
        "$(run_session "${name}_child" "INSERT INTO rfc.child VALUES (100, $4, 'concurrent')")"
    try_step "$name" "$3"
    try_step "$name" "COMMIT;"
    close_session "$name"
}

echo "== rr_parent_delete"
reset
parent_after_child_commit rr_parent_delete "REPEATABLE READ" "DELETE FROM rfc.parent WHERE id = 1;" 1
check "rr_parent_delete: the parent DELETE is refused" refused "$(outcome rr_parent_delete)"
check "rr_parent_delete: no orphan" 0 "$(orphans)"

echo "== rc_parent_delete (control)"
reset
parent_after_child_commit rc_parent_delete "READ COMMITTED" "DELETE FROM rfc.parent WHERE id = 1;" 1
check "rc_parent_delete: the parent DELETE is refused" refused "$(outcome rc_parent_delete)"
check "rc_parent_delete: no orphan" 0 "$(orphans)"

echo "== rr_parent_key_update"
reset
parent_after_child_commit rr_parent_key_update "REPEATABLE READ" "UPDATE rfc.parent SET id = 11 WHERE id = 1;" 1
check "rr_parent_key_update: the parent key UPDATE is refused" refused "$(outcome rr_parent_key_update)"
check "rr_parent_key_update: no orphan" 0 "$(orphans)"

echo "== rc_parent_key_update (control)"
reset
parent_after_child_commit rc_parent_key_update "READ COMMITTED" "UPDATE rfc.parent SET id = 11 WHERE id = 1;" 1
check "rc_parent_key_update: the parent key UPDATE is refused" refused "$(outcome rc_parent_key_update)"
check "rc_parent_key_update: no orphan" 0 "$(orphans)"

echo "== rr_parent_segment_delete (another row keeps the key)"
reset
parent_after_child_commit rr_parent_segment_delete "REPEATABLE READ" \
    "DELETE FROM rfc.parent WHERE id = 2 AND valid_from = '2020-01-01';" 2
check "rr_parent_segment_delete: the DELETE of one segment is accepted" accepted "$(outcome rr_parent_segment_delete)"
check "rr_parent_segment_delete: one row of key 2 remains" 1 "$(q "SELECT count(*) FROM rfc.parent WHERE id = 2")"
check "rr_parent_segment_delete: no orphan" 0 "$(orphans)"

echo "== rr_child_insert"
reset
open_session rr_child_insert
step rr_child_insert "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM rfc.parent;"
check "rr_child_insert: the concurrent parent DELETE commits" 0 \
    "$(run_session rr_child_insert_parent "DELETE FROM rfc.parent WHERE id = 1")"
try_step rr_child_insert "INSERT INTO rfc.child VALUES (101, 1, 'stale snapshot');"
try_step rr_child_insert "COMMIT;"
close_session rr_child_insert
check "rr_child_insert: the child INSERT is refused" refused "$(outcome rr_child_insert)"
check "rr_child_insert: no orphan" 0 "$(orphans)"

# child_lock <case> <parent statement>: an open child INSERT; the parent statement must wait for it, then fail.
child_lock() {
    name=$1
    open_session "$name"
    step "$name" "BEGIN; INSERT INTO rfc.child VALUES (102, 1, 'held');"
    start_background "${name}_parent" "$2"
    await_blocked "${name}_parent"
    check "$name: the parent statement waits for the open child INSERT" waiting "$(lock_waiting "${name}_parent")"
    step "$name" "COMMIT;"
    close_session "$name"
    finish_background "${name}_parent"
    check "$name: the parent statement is refused once the child commits" refused "$(outcome "${name}_parent")"
    check "$name: no orphan" 0 "$(orphans)"
}

echo "== child_lock_update"
reset
child_lock child_lock_update "UPDATE rfc.parent SET id = 11 WHERE id = 1"
echo "== child_lock_delete"
reset
child_lock child_lock_delete "DELETE FROM rfc.parent WHERE id = 1"

# segment_deletes <case> <isolation>: two sessions each delete one of the two rows of referenced key 2.
segment_deletes() {
    name=$1
    reset
    q "INSERT INTO rfc.child VALUES (103, 2, 'references 2')" >/dev/null || die "child insert failed"
    open_session "$name"
    step "$name" "BEGIN ISOLATION LEVEL $2; DELETE FROM rfc.parent WHERE id = 2 AND valid_from = '2020-01-01';"
    start_background "${name}_other" "BEGIN ISOLATION LEVEL $2; DELETE FROM rfc.parent WHERE id = 2 AND valid_from = '2022-01-01'; COMMIT;"
    await_blocked "${name}_other"
    check "$name: the second DELETE waits for the first" waiting "$(lock_waiting "${name}_other")"
    try_step "$name" "COMMIT;"
    close_session "$name"
    finish_background "${name}_other"
    check "$name: the first DELETE commits" accepted "$(outcome "$name")"
    check "$name: the second DELETE is refused" refused "$(outcome "${name}_other")"
    check "$name: one row of key 2 remains" 1 "$(q "SELECT count(*) FROM rfc.parent WHERE id = 2")"
    check "$name: no orphan" 0 "$(orphans)"
}

echo "== segment_deletes_rc"
segment_deletes segment_deletes_rc "READ COMMITTED"
echo "== segment_deletes_rr"
segment_deletes segment_deletes_rr "REPEATABLE READ"

# ON DELETE CASCADE (1.4). The contract is unchanged: no orphan whatever the interleaving. In addition the child
# goes with the last parent row of its key, and only then.

# cascade_after_child_commit <case> <isolation>: the parent session takes its snapshot, a CASCADE child of key 1 is
# inserted and committed by another session, then the parent DELETE of key 1 runs and commits. The child the parent
# session could not see must still be deleted (or the DELETE refused), never left as an orphan.
cascade_after_child_commit() {
    name=$1
    reset
    open_session "$name"
    step "$name" "BEGIN ISOLATION LEVEL $2; SELECT count(*) FROM rfc.cchild;"
    check "$name: the concurrent CASCADE child INSERT commits" 0 \
        "$(run_session "${name}_child" "INSERT INTO rfc.cchild VALUES (200, 1, 'concurrent')")"
    try_step "$name" "DELETE FROM rfc.parent WHERE id = 1;"
    try_step "$name" "COMMIT;"
    close_session "$name"
    result=$(outcome "$name")
    case $result in
        accepted) check "$name: key 1 deleted, its CASCADE child deleted with it" "0 0" \
                      "$(q "SELECT count(*) FROM rfc.parent WHERE id = 1") $(cchildren 1)" ;;
        refused) check "$name: DELETE refused, parent and child kept" "1 1" \
                     "$(q "SELECT count(*) FROM rfc.parent WHERE id = 1") $(cchildren 1)" ;;
        *) check "$name: the parent DELETE is accepted or refused" "accepted or refused" "$result" ;;
    esac
    check "$name: no orphan" 0 "$(orphans)"
}

echo "== cascade_rc_after_child_commit"
cascade_after_child_commit cascade_rc_after_child_commit "READ COMMITTED"
echo "== cascade_rr_after_child_commit"
cascade_after_child_commit cascade_rr_after_child_commit "REPEATABLE READ"

# cascade_child_lock: an open CASCADE child INSERT of key 1; the parent DELETE must wait for it, then delete it.
echo "== cascade_child_lock"
reset
open_session cascade_child_lock
step cascade_child_lock "BEGIN; INSERT INTO rfc.cchild VALUES (201, 1, 'held');"
start_background cascade_child_lock_parent "DELETE FROM rfc.parent WHERE id = 1"
await_blocked cascade_child_lock_parent
check "cascade_child_lock: the parent DELETE waits for the open child INSERT" waiting "$(lock_waiting cascade_child_lock_parent)"
step cascade_child_lock "COMMIT;"
close_session cascade_child_lock
finish_background cascade_child_lock_parent
check "cascade_child_lock: the parent DELETE is accepted once the child commits" accepted "$(outcome cascade_child_lock_parent)"
check "cascade_child_lock: the committed child was cascaded" 0 "$(cchildren 1)"
check "cascade_child_lock: no orphan" 0 "$(orphans)"

# cascade_rr_child_insert: REPEATABLE READ CASCADE child INSERT, snapshot taken before a concurrent DELETE of the
# only parent row of key 1 committed: refused (the key is gone), no orphan.
echo "== cascade_rr_child_insert"
reset
open_session cascade_rr_child_insert
step cascade_rr_child_insert "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM rfc.parent;"
check "cascade_rr_child_insert: the concurrent parent DELETE commits" 0 \
    "$(run_session cascade_rr_child_insert_parent "DELETE FROM rfc.parent WHERE id = 1")"
try_step cascade_rr_child_insert "INSERT INTO rfc.cchild VALUES (202, 1, 'stale snapshot');"
try_step cascade_rr_child_insert "COMMIT;"
close_session cascade_rr_child_insert
check "cascade_rr_child_insert: the child INSERT is refused" refused "$(outcome cascade_rr_child_insert)"
check "cascade_rr_child_insert: no orphan" 0 "$(orphans)"

# cascade_segment_deletes <case> <isolation>: two sessions each delete one of the two rows of key 2, which has a
# CASCADE child. The first commits and keeps the child (key 2 remains); the second, the last row, cascades it.
cascade_segment_deletes() {
    name=$1
    reset
    q "INSERT INTO rfc.cchild VALUES (203, 2, 'references 2')" >/dev/null || die "cascade child insert failed"
    open_session "$name"
    step "$name" "BEGIN ISOLATION LEVEL $2; DELETE FROM rfc.parent WHERE id = 2 AND valid_from = '2020-01-01';"
    check "$name: the child is kept while the other row of key 2 exists" 1 "$(cchildren 2)"
    start_background "${name}_other" "BEGIN ISOLATION LEVEL $2; DELETE FROM rfc.parent WHERE id = 2 AND valid_from = '2022-01-01'; COMMIT;"
    await_blocked "${name}_other"
    check "$name: the second DELETE waits for the first" waiting "$(lock_waiting "${name}_other")"
    try_step "$name" "COMMIT;"
    close_session "$name"
    finish_background "${name}_other"
    check "$name: the first DELETE commits" accepted "$(outcome "$name")"
    result=$(outcome "${name}_other")
    case $result in
        accepted) check "$name: last row deleted, CASCADE child deleted" "0 0" \
                      "$(q "SELECT count(*) FROM rfc.parent WHERE id = 2") $(cchildren 2)" ;;
        refused) check "$name: second DELETE refused, one row and the child kept" "1 1" \
                     "$(q "SELECT count(*) FROM rfc.parent WHERE id = 2") $(cchildren 2)" ;;
        *) check "$name: the second DELETE is accepted or refused" "accepted or refused" "$result" ;;
    esac
    check "$name: no orphan" 0 "$(orphans)"
}

echo "== cascade_segment_deletes_rc"
cascade_segment_deletes cascade_segment_deletes_rc "READ COMMITTED"
echo "== cascade_segment_deletes_rr"
cascade_segment_deletes cascade_segment_deletes_rr "REPEATABLE READ"

harness_ok
if [ "$FAILURES" -gt 0 ]; then
    echo "FAILED: $FAILURES check(s)"
    KEEP=1
    exit 1
fi
echo "all regular foreign key concurrency checks passed"
exit 0
