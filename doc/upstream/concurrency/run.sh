#!/usr/bin/env bash
# Run the FOR PORTION OF revert study specs (doc/upstream/for-portion-of-revert.md) against one scratch database.
#   doc/upstream/concurrency/run.sh DBNAME
# DBNAME must end in _jhf and have btree_gist and sql_saga installed. Writes <spec>.out next to each spec.
set -eu
db="${1:?usage: run.sh DBNAME (a throwaway *_jhf database with sql_saga installed)}"
case "$db" in *_jhf) ;; *) echo "refusing: $db is not a throwaway *_jhf database" >&2; exit 2 ;; esac
here="$(cd "$(dirname "$0")" && pwd)"
tester="$(dirname "$(pg_config --pgxs)")/../test/isolation/isolationtester"
[ -x "$tester" ] || { echo "isolationtester not found at $tester" >&2; exit 2; }
python3 "$here/gen.py" > /dev/null
for spec in "$here"/*.spec; do
    out="${spec%.spec}.out"
    if "$tester" "dbname=$db" < "$spec" > "$out" 2>&1; then
        echo "$(basename "$spec"): $(grep -c '^ERROR' "$out" || true) error(s)"
    else
        echo "$(basename "$spec"): FAILED TO RUN, see $out"
    fi
done
