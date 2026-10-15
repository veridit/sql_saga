#!/usr/bin/env bash
#
# devops/benchmark_settings.sh: switch the local PostgreSQL server between the settings the benchmark
# tests need and the PostgreSQL defaults.
#
#   devops/benchmark_settings.sh status    show the current values (no change)
#   devops/benchmark_settings.sh enable    pg_stat_monitor preloaded, max_locks_per_transaction = 256
#   devops/benchmark_settings.sh disable   remove both settings (back to the server defaults)
#
# enable and disable use ALTER SYSTEM (written to postgresql.auto.conf) and restart the server, because
# both settings only take effect at server start. Run it when nothing else uses the server.
#
# Why: the benchmark tests (make test benchmark) record pg_stat_monitor output in expected/performance/,
# and 111_benchmark_scaling_analysis needs max_locks_per_transaction >= 256. pg_stat_monitor must be
# installed first (see README.md, "pg_stat_monitor").
#
# Environment: PG_CONFIG (default: pg_config on PATH), PGDATA (default: asked from the running server),
# and the usual libpq variables (PGHOST, PGPORT, PGUSER) for the connection.
set -euo pipefail

PG_CONFIG=${PG_CONFIG:-pg_config}
BINDIR=$("$PG_CONFIG" --bindir)
PSQL=("$BINDIR/psql" -X -q -At -v ON_ERROR_STOP=1 -d postgres)

die() { echo "benchmark_settings: $*" >&2; exit 1; }

show() {
    "${PSQL[@]}" -c "SELECT format('%-26s %s', name, setting) FROM pg_settings
                      WHERE name IN ('shared_preload_libraries', 'max_locks_per_transaction') ORDER BY name"
}

restart() {
    local data
    data=${PGDATA:-$("${PSQL[@]}" -c "SHOW data_directory")}
    [ -n "$data" ] || die "could not determine the data directory; set PGDATA"
    echo "Restarting PostgreSQL ($data) ..."
    # -l keeps pg_ctl from holding the terminal while the server runs.
    "$BINDIR/pg_ctl" -D "$data" -l "$data/benchmark_settings_restart.log" restart -m fast -w >/dev/null \
        || die "restart failed; see $data/benchmark_settings_restart.log"
}

case "${1:-}" in
status)
    show
    ;;
enable)
    "${PSQL[@]}" -c "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_stat_monitor'" | grep -q 1 \
        || die "pg_stat_monitor is not installed in this PostgreSQL; see README.md"
    "${PSQL[@]}" -c "ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_monitor'"
    "${PSQL[@]}" -c "ALTER SYSTEM SET max_locks_per_transaction = 256"
    restart
    show
    ;;
disable)
    "${PSQL[@]}" -c "ALTER SYSTEM RESET shared_preload_libraries"
    "${PSQL[@]}" -c "ALTER SYSTEM RESET max_locks_per_transaction"
    restart
    show
    ;;
*)
    sed -n '3,9p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
