#!/bin/sh
# No-DB check of the 1.3 update-script Makefile wiring, in a scratch copy under tmp/. Usage: devops/test_update_scripts_make.sh
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
d="$root/tmp/mk13_wiring"
rm -rf "$d"
mkdir -p "$d"
cp -R "$root/src" "$root/versions" "$root/devops" "$root/Makefile" "$root/sql_saga.control" "$root/sql_saga_upgrade_preflight.sql" "$d/"
for n in preamble schema repairs; do
  [ -f "$d/devops/upgrade_1_3_$n.sql" ] && continue
  if [ "$n" = preamble ]; then
    printf -- '-- SYNTHETIC P\n-- sql_saga 1.3 generator: insert src/1k_foreign_key_pairing.sql routines here\n' >"$d/devops/upgrade_1_3_$n.sql"
  else
    printf -- '-- SYNTHETIC %s\n' "$n" >"$d/devops/upgrade_1_3_$n.sql"
  fi
done
cd "$d"
out="sql_saga--1.0--1.3.sql sql_saga--1.1--1.3.sql sql_saga--1.2--1.3.sql"
make -s -j4 EXTVERSION=1.3 $out
first=$(shasum -a 256 $out)
rm sql_saga--1.1--1.3.sql
make -s -j4 EXTVERSION=1.3 $out
[ "$(shasum -a 256 $out)" = "$first" ] || { echo "FAIL: regenerated outputs differ" >&2; exit 1; }
[ -z "$(make -s -n EXTVERSION=1.3 $out)" ] || { echo "FAIL: not up to date after regeneration" >&2; exit 1; }
sleep 1  # GNU make 3.81 compares whole-second mtimes
touch src/2l_temporal_merge_execute.sql
make -n EXTVERSION=1.3 $out | grep -q generate_update_scripts || { echo "FAIL: src change does not regenerate" >&2; exit 1; }
make -s EXTVERSION=1.3 clean
for f in $out .update_scripts_1.3.stamp; do [ ! -e "$f" ] || { echo "FAIL: $f not cleaned" >&2; exit 1; }; done
shasum -a 256 versions/sql_saga--1.0--1.2.sql versions/sql_saga--1.1--1.2.sql versions/sql_saga--1.2.sql | awk '{print $1}' | tr '\n' ' ' | grep -q '^11b6174d2a428bed86e82fb5e7560ffa8a0f640affd8146c399c9e1dae49c321 3a33a535b57d1990f29358b5bfa6443a53217787766bd39abc78a8243a5260c5 448ffb0b496578b9243443aefb5ddc2f2c66de7dc6315a5da2bdbddd92a6fa83 $' || { echo "FAIL: frozen file changed" >&2; exit 1; }
cd "$root"
rm -rf "$d"
echo "OK: 1.3 update-script make wiring"
