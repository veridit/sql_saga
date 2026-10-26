#!/bin/sh
# No-DB check of the 1.4 update-script Makefile wiring, in a scratch copy under tmp/. Usage: devops/test_update_scripts_make.sh
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
d="$root/tmp/mk14_wiring"
rm -rf "$d"
mkdir -p "$d"
cp -R "$root/src" "$root/versions" "$root/devops" "$root/Makefile" "$root/sql_saga.control" "$root/sql_saga_upgrade_preflight.sql" "$d/"
cd "$d"
out="sql_saga--1.0--1.4.sql sql_saga--1.1--1.4.sql sql_saga--1.2--1.4.sql sql_saga--1.3--1.4.sql"
make -s -j4 EXTVERSION=1.4 $out
first=$(shasum -a 256 $out)
rm sql_saga--1.1--1.4.sql
make -s -j4 EXTVERSION=1.4 $out
[ "$(shasum -a 256 $out)" = "$first" ] || { echo "FAIL: regenerated outputs differ" >&2; exit 1; }
[ -z "$(make -s -n EXTVERSION=1.4 $out)" ] || { echo "FAIL: not up to date after regeneration" >&2; exit 1; }
sleep 1  # GNU make 3.81 compares whole-second mtimes
touch src/2o_updatable_view_column_follow.sql
make -n EXTVERSION=1.4 $out | grep -q generate_update_scripts_1_4 || { echo "FAIL: src change does not regenerate" >&2; exit 1; }
make -s EXTVERSION=1.4 clean
for f in $out .update_scripts_1.4.stamp; do [ ! -e "$f" ] || { echo "FAIL: $f not cleaned" >&2; exit 1; }; done
shasum -a 256 versions/sql_saga--1.3.sql versions/sql_saga--1.0--1.3.sql versions/sql_saga--1.1--1.3.sql versions/sql_saga--1.2--1.3.sql | awk '{print $1}' | tr '\n' ' ' | grep -q '^c59b9e3f501217dc8364fe8f57da44cfa39e0825a7571bff12082bd5624cfd52 9646114bc85feabc9a03314da1a8298ab8c3607deb5bdc43eb27843ae1f002f1 3b28f9bb314a75f3bae0cf363026e8e23fb1f5184e2aa8811dacb35fd67cd7d6 d670f8e21f8a9b68afb7fbd3fb378eeb998419fef375268f7797f1cd7aae6aee $' || { echo "FAIL: frozen file changed" >&2; exit 1; }
cd "$root"
rm -rf "$d"
echo "OK: 1.4 update-script make wiring"
