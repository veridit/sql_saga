#!/usr/bin/env bash
#
# Compare metadata-first drop API latency in the working tree against released v1.1.
# This is a local performance gate, not part of the default regression suite or CI.
# Exit status: 0 below the 10 ms hard ceiling, 1 ceiling failure, 2 harness/setup error.
# Compatible with macOS /bin/bash 3.2 and set -u.
#
set -u
export LC_ALL=C

PG_CONFIG=${PG_CONFIG:-pg_config}
GIT=${GIT:-git}
MAKE=${MAKE:-make}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
FIXTURE="$ROOT/devops/drop_api_performance_fixture.sql"
SCRATCH_BASE=${JCODE_SCRATCH_DIR:-${TMPDIR:-/tmp}}
WORK=$(mktemp -d "$SCRATCH_BASE/sql_saga_drop_perf.XXXXXX") || exit 2
PGDATA="$WORK/pgdata"
SOCKET="$WORK/socket"
PORT=$((55000 + ($$ % 1000)))
PG_BINDIR=$($PG_CONFIG --bindir) || exit 2
PSQL="$PG_BINDIR/psql"
CREATEDB="$PG_BINDIR/createdb"
INITDB="$PG_BINDIR/initdb"
PG_CTL="$PG_BINDIR/pg_ctl"
SHAREDIR=$($PG_CONFIG --sharedir) || exit 2
PKGLIBDIR=$($PG_CONFIG --pkglibdir) || exit 2
SERVER_STARTED=0

cleanup() {
    status=$?
    trap - EXIT
    if [ "$SERVER_STARTED" = 1 ]; then
        "$PG_CTL" -D "$PGDATA" stop -m fast >/dev/null 2>&1 || true
    fi
    rm -rf "$WORK"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 2' HUP INT TERM

harness_error() {
    echo "HARNESS ERROR: $*" >&2
    exit 2
}

run_logged() {
    log=$1
    shift
    "$@" >"$log" 2>&1 || {
        echo "HARNESS ERROR: command failed: $*" >&2
        tail -40 "$log" >&2
        exit 2
    }
}

copy_working_tree() {
    destination=$1
    mkdir -p "$destination" || harness_error "creating working-tree copy"
    "$GIT" -C "$ROOT" archive HEAD | tar -xf - -C "$destination" || harness_error "archiving working tree HEAD"
    "$GIT" -C "$ROOT" diff --name-only --diff-filter=ACMRTUXB HEAD | while IFS= read -r path; do
        [ -n "$path" ] || continue
        mkdir -p "$destination/$(dirname "$path")" || exit 2
        cp "$ROOT/$path" "$destination/$path" || exit 2
    done || harness_error "overlaying tracked working-tree changes"
}

build_install() {
    source_dir=$1
    install_root=$2
    label=$3
    run_logged "$WORK/build_${label}.log" "$MAKE" -C "$source_dir" -j2 PG_CONFIG="$PG_CONFIG"
    run_logged "$WORK/install_${label}.log" "$MAKE" -C "$source_dir" install PG_CONFIG="$PG_CONFIG" DESTDIR="$install_root"
}

setup_database() {
    database=$1
    install_root=$2
    version=$3
    control_path="$install_root$SHAREDIR/extension:\$system"
    library_path="$install_root$PKGLIBDIR:\$libdir"
    run_logged "$WORK/setup_${database}.log" "$CREATEDB" -h "$SOCKET" -p "$PORT" "$database"
    "$PSQL" -X --no-psqlrc -h "$SOCKET" -p "$PORT" -d "$database" -v ON_ERROR_STOP=1 \
        -v extversion="$version" \
        -c "SET extension_control_path TO '$control_path'; SET dynamic_library_path TO '$library_path';" \
        -f "$FIXTURE" >"$WORK/fixture_${database}.log" 2>&1 || {
            tail -40 "$WORK/fixture_${database}.log" >&2
            harness_error "setting up $database"
        }
}

run_round() {
    database=$1
    install_root=$2
    round=$3
    control_path="$install_root$SHAREDIR/extension:\$system"
    library_path="$install_root$PKGLIBDIR:\$libdir"
    "$PSQL" -X --no-psqlrc -Atq -h "$SOCKET" -p "$PORT" -d "$database" -v ON_ERROR_STOP=1 \
        -c "SET extension_control_path TO '$control_path'; SET dynamic_library_path TO '$library_path'; SET track_functions = 'none'; SET client_min_messages = warning; SELECT bench.round_once($round);" \
        >>"$WORK/rounds_${database}.log" 2>&1 || harness_error "benchmark round $round for $database"
}

median() {
    database=$1
    api=$2
    "$PSQL" -X --no-psqlrc -Atq -h "$SOCKET" -p "$PORT" -d "$database" \
        -c "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY elapsed_ms) FROM bench.samples WHERE round > 0 AND api = '$api';" || harness_error "reading median for $database $api"
}

[ -f "$FIXTURE" ] || harness_error "missing fixture $FIXTURE"
mkdir -p "$SOCKET" "$WORK/source" "$WORK/install" || harness_error "creating scratch directories"

"$GIT" -C "$ROOT" archive v1.1 | tar -xf - -C "$WORK/source/v11" 2>/dev/null || {
    mkdir -p "$WORK/source/v11" || harness_error "creating v1.1 source directory"
    "$GIT" -C "$ROOT" archive v1.1 | tar -xf - -C "$WORK/source/v11" || harness_error "extracting v1.1"
}
copy_working_tree "$WORK/source/current"

build_install "$WORK/source/v11" "$WORK/install/v11" v11
build_install "$WORK/source/current" "$WORK/install/current" current

run_logged "$WORK/initdb.log" "$INITDB" -D "$PGDATA" --no-locale --encoding=UTF8 --auth=trust
"$PG_CTL" -D "$PGDATA" -l "$WORK/postgres.log" -o "-F -p $PORT -k $SOCKET -c max_locks_per_transaction=256" start >"$WORK/pg_ctl_start.log" 2>&1 || {
    cat "$WORK/pg_ctl_start.log" >&2
    tail -40 "$WORK/postgres.log" >&2
    harness_error "starting private PostgreSQL cluster"
}
SERVER_STARTED=1

setup_database perf_v11 "$WORK/install/v11" 1.1
setup_database perf_current "$WORK/install/current" 1.2

warm=1
while [ "$warm" -le 3 ]; do
    run_round perf_v11 "$WORK/install/v11" 0
    run_round perf_current "$WORK/install/current" 0
    warm=$((warm + 1))
done

round=1
while [ "$round" -le 31 ]; do
    if [ $((round % 2)) -eq 1 ]; then
        run_round perf_v11 "$WORK/install/v11" "$round"
        run_round perf_current "$WORK/install/current" "$round"
    else
        run_round perf_current "$WORK/install/current" "$round"
        run_round perf_v11 "$WORK/install/v11" "$round"
    fi
    round=$((round + 1))
done

FAILED=0
printf '%-25s %12s %12s %10s %10s %s\n' API v1.1_ms current_ms ratio ratio_gate ceiling
for api in drop_foreign_key drop_system_versioning drop_unique_key; do
    baseline=$(median perf_v11 "$api")
    current=$(median perf_current "$api")
    result=$(awk -v baseline="$baseline" -v current="$current" 'BEGIN { ratio=current/baseline; printf "%.3f|%s|%s", ratio, (ratio <= 1.05 ? "PASS" : "INFO_FAIL"), (current < 10.0 ? "PASS" : "FAIL") }')
    ratio=${result%%|*}
    remainder=${result#*|}
    ratio_verdict=${remainder%%|*}
    ceiling_verdict=${remainder#*|}
    printf '%-25s %12.3f %12.3f %10s %10s %s\n' "$api" "$baseline" "$current" "$ratio" "$ratio_verdict" "$ceiling_verdict"
    if [ "$ceiling_verdict" = FAIL ]; then
        FAILED=1
    fi
done

if [ "$FAILED" = 1 ]; then
    echo "RED: at least one drop API median is >= 10 ms"
    exit 1
fi

echo "GREEN: every drop API median is < 10 ms; ratios versus v1.1 are informational"
exit 0
