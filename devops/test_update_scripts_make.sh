#!/bin/sh
# No-DB check of current update-script wiring in a uniquely owned scratch copy.
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$root/tmp"
d=$(mktemp -d "$root/tmp/mk15_wiring.XXXXXX")
trap 'rm -rf "$d"' EXIT HUP INT TERM
cp -R "$root/src" "$root/versions" "$root/devops" "$root/Makefile" "$root/sql_saga.control" "$root/sql_saga_upgrade_preflight.sql" "$d/"
cd "$d"
out="sql_saga--1.0--1.5.sql sql_saga--1.1--1.5.sql sql_saga--1.2--1.5.sql sql_saga--1.3--1.5.sql sql_saga--1.4--1.5.sql"
frozen=$(shasum -a 256 versions/*.sql)
make -s -j4 $out sql_saga--1.5.sql
first=$(shasum -a 256 $out)
rm sql_saga--1.1--1.5.sql
make -s -j4 $out
[ "$(shasum -a 256 $out)" = "$first" ] || { echo 'FAIL: regenerated outputs differ' >&2; exit 1; }
[ -z "$(make -s -n $out)" ] || { echo 'FAIL: not up to date after regeneration' >&2; exit 1; }
sleep 1  # GNU make 3.81 compares whole-second mtimes
touch src/0g_for_portion_of_trigger.sql
make -n $out | grep -q generate_update_scripts_1_5 || { echo 'FAIL: src change does not regenerate' >&2; exit 1; }
# An omitted source change must invalidate the stamp and fail closed.
printf '\nSELECT 611;\n' >> src/0g_for_portion_of_trigger.sql
if make -s $out > omission.log 2>&1; then echo 'FAIL: omitted change accepted' >&2; exit 1; fi
grep -q 'outside the 1.5 routine manifest' omission.log
make -s clean
for f in $out sql_saga--1.5.sql .update_scripts_1.5.stamp; do [ ! -e "$f" ] || { echo "FAIL: $f not cleaned" >&2; exit 1; }; done
[ "$(shasum -a 256 versions/*.sql)" = "$frozen" ] || { echo 'FAIL: frozen artifact changed' >&2; exit 1; }
echo 'OK: 1.5 update-script make wiring'
