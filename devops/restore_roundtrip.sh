#!/usr/bin/env bash
#
# restore_roundtrip.sh - prove that databases using sql_saga survive pg_dump + pg_restore.
#
# For each case a source database is built, dumped with `pg_dump -Fc`, and restored into fresh
# databases with `pg_restore --exit-on-error` (serial, single transaction, --no-owner). A case
# passes when every restore exits 0, the effective ACL digest of the restored database equals the
# source's, and all sql_saga event triggers are still enabled.
#
# The clean* variants restore into a database that already holds the dumped database (restored
# there once first), so pg_restore must tear every sql_saga family down before recreating it
# (tmp/design_restore_clean.md):
#   clean             pg_restore --clean --if-exists (the dump's history policy names carry another
#                     installation's token)
#   clean_single_txn  the same with --single-transaction
#   clean_same        a dump of the destination itself, restored with --clean (no --if-exists): every
#                     dumped name exists
#   clean_psql        pg_restore --clean --if-exists -f - | psql, with the opt-in
#                     PGOPTIONS='-c sql_saga.clean_restore=on' that a restore script run through psql needs
#   clean_psql_without_opt_in
#                     the same without the opt-in: sql_saga's drop protection must refuse it
#
# Parallel restore (`pg_restore -j N`) is NOT supported (PostgreSQL pg_dump drops the load order
# of extension configuration tables from the archive; see doc/decision-log.md). The
# `parallel_known_failure` case still runs `-j 4` restores and reports their outcome, but does not
# fail the run; if they all succeed (PostgreSQL fixed), the summary says so.
#
# Usage:   devops/restore_roundtrip.sh [case ...]      (default: all cases)
#          make test-restore
# Uses the standard libpq environment (PGHOST, PGPORT, PGUSER). Needs a superuser.
# Creates databases prefixed saga_rt_ and roles prefixed saga_rt_, and drops them again.
#
set -u
# pg_restore identifies itself with application_name 'pg_restore', which sql_saga uses to recognize
# the teardown of pg_restore --clean; an inherited PGAPPNAME would hide it.
unset PGAPPNAME

PREFIX=saga_rt
WORK=$(mktemp -d "${TMPDIR:-/tmp}/saga_rt.XXXXXX")
LOG="$WORK/log"
FAILED=()
PASSED=()
KNOWN=()

ALL_CASES=(catalog_only fpo_serial fpo_many inherited_role sv_one_grant custom_history_name hardened_functions rls_history fk_views parallel_known_failure)

psql_q() { psql -X -q -v ON_ERROR_STOP=1 "$@"; }

cleanup() {
    for db in $(psql -X -At -d postgres -c "SELECT datname FROM pg_database WHERE datname LIKE '${PREFIX}\_%'"); do
        dropdb --if-exists "$db" >>"$LOG" 2>&1
    done
    psql -X -q -d postgres >>"$LOG" 2>&1 <<SQL
DO \$\$
DECLARE r record;
BEGIN
    FOR r IN SELECT rolname FROM pg_roles WHERE rolname LIKE '${PREFIX}\_%' ORDER BY rolname DESC LOOP
        EXECUTE format('DROP ROLE %I', r.rolname);
    END LOOP;
END
\$\$;
SQL
}

# Effective ACL of every relation and function outside the system schemas, owner entries excluded
# (so --no-owner restores compare equal), PUBLIC included, grantor and grant option ignored.
# digest <db> [without_extension_functions]: with the second argument, functions that are members of
# an extension are left out (see the clean* variants in restore_variant).
digest() {
    psql -X -At -d "$1" -v skip_ext="${2:+on}" <<'SQL'
\if :{?skip_ext}
\else
\set skip_ext off
\endif
WITH objs AS (
    SELECT format('rel %s', c.oid::regclass) AS obj, c.relowner AS owner,
           COALESCE(c.relacl, acldefault(CASE WHEN c.relkind = 'S' THEN 's' ELSE 'r' END::"char", c.relowner)) AS acl
    FROM pg_class AS c
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND c.relkind IN ('r', 'v', 'm', 'S', 'p')
    UNION ALL
    SELECT format('fn %s', p.oid::regprocedure), p.proowner, COALESCE(p.proacl, acldefault('f', p.proowner))
    FROM pg_proc AS p
    JOIN pg_namespace AS n ON n.oid = p.pronamespace
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND NOT (:'skip_ext' = 'on' AND EXISTS (
          SELECT FROM pg_depend AS d
          WHERE (d.classid, d.objid, d.deptype) = ('pg_proc'::regclass, p.oid, 'e')))
), entries AS (
    SELECT o.obj, CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END AS grantee, a.privilege_type
    FROM objs AS o
    CROSS JOIN LATERAL aclexplode(o.acl) AS a
    WHERE a.grantee <> o.owner
)
SELECT md5(COALESCE(string_agg(format('%s|%s|%s', obj, grantee, privilege_type), E'\n' ORDER BY obj, grantee, privilege_type), ''))
FROM entries;
SQL
}

# Row-level security of every table. Policy copies on history tables (sql_saga_<hash>) are named
# after the hash of their base policy name and the per-installation token (tmp/design_final.md
# section 14), so their names differ after a restore by design. They carry no comment; the base
# policy of a copy is found by recomputing the hash with this database's token.
policies() {
    psql -X -At -d "$1" <<'SQL'
WITH tok AS (
    -- (xpath over query_to_xml so this also parses where sql_saga.installation does not exist yet)
    SELECT (xpath('//row/token/text()', query_to_xml('SELECT token FROM sql_saga.installation', false, false, '')))[1]::text AS token
    WHERE to_regclass('sql_saga.installation') IS NOT NULL
), base_of AS (
    SELECT hp.oid AS copy_oid, bp.polname::text AS base_policy
    FROM pg_policy AS hp
    JOIN sql_saga.system_versioning AS sv
      ON hp.polrelid = format('%I.%I', sv.history_schema_name, sv.history_table_name)::regclass
    JOIN pg_policy AS bp ON bp.polrelid = format('%I.%I', sv.table_schema, sv.table_name)::regclass
    CROSS JOIN tok
    WHERE hp.polname = 'sql_saga_' || left(md5(bp.polname || tok.token), 16)
), pol AS (
    SELECT c.oid::regclass::text AS tbl, c.relrowsecurity AS rls, c.relforcerowsecurity AS force,
           CASE WHEN p.polname ~ '^sql_saga_[0-9a-f]{16}$'
                THEN 'copy-of:' || coalesce(b.base_policy, '?')
                -- the fixed history policies also carry the installation token
                WHEN p.polname ~ '^sql_saga_history_(reader|base_select)_'
                THEN regexp_replace(p.polname, '_[^_]+$', '_<token>')
                ELSE p.polname END AS name,
           p.polpermissive, p.polcmd,
           (SELECT string_agg(CASE WHEN r = 0 THEN 'PUBLIC' ELSE r::regrole::text END, ',' ORDER BY 1) FROM unnest(p.polroles) AS r) AS roles,
           pg_get_expr(p.polqual, p.polrelid) AS qual
    FROM pg_policy AS p JOIN pg_class AS c ON c.oid = p.polrelid
    LEFT JOIN base_of AS b ON b.copy_oid = p.oid
)
SELECT md5(coalesce(string_agg(format('%s|%s|%s|%s|%s|%s|%s|%s', tbl, rls, force, name, polpermissive, polcmd, roles, qual), E'\n' ORDER BY tbl, name), ''))
FROM pol;
SQL
}

# Number of history policy copies that are missing, orphaned (no base policy hashes to their name)
# or stale (definition differs from the base policy: FOR SELECT, roles, permissive, USING).
# The desired USING is normalized on the history side (tmp/design_final.md section 10): each base
# qual is created as a probe policy on the history table inside a rolled-back transaction and
# deparsed there, exactly as a copy would be. 0 = in sync.
policy_copy_faults() {
    psql -X -At -q -v ON_ERROR_STOP=1 -d "$1" <<'SQL' | tail -1
BEGIN;
CREATE TEMP TABLE pg_temp.fam ON COMMIT DROP AS
    SELECT format('%I.%I', sv.table_schema, sv.table_name)::regclass AS base,
           format('%I.%I', sv.history_schema_name, sv.history_table_name)::regclass AS hist
    FROM sql_saga.system_versioning AS sv;
CREATE TEMP TABLE pg_temp.want ON COMMIT DROP AS
    SELECT f.hist, 'sql_saga_' || left(md5(p.polname || t.token), 16) AS copy_name,
           p.polroles, p.polpermissive, pg_get_expr(p.polqual, p.polrelid) AS base_qual,
           NULL::text AS qual, 'sql_saga_probe_' || row_number() OVER () AS probe
    FROM pg_temp.fam AS f JOIN pg_policy AS p ON p.polrelid = f.base
    CROSS JOIN (SELECT (xpath('//row/token/text()', query_to_xml('SELECT token FROM sql_saga.installation', false, false, '')))[1]::text AS token
                WHERE to_regclass('sql_saga.installation') IS NOT NULL) AS t
    WHERE p.polcmd IN ('r', '*');
-- Probe policies: the base qual parsed against the history table (event triggers off, so sql_saga
-- does not react to them; everything is rolled back).
SET LOCAL session_replication_role = replica;
SELECT format('CREATE POLICY %I ON %s FOR SELECT USING (%s)', w.probe, w.hist, w.base_qual)
FROM pg_temp.want AS w WHERE w.base_qual IS NOT NULL \gexec
UPDATE pg_temp.want AS w SET qual = pg_get_expr(p.polqual, p.polrelid)
FROM pg_policy AS p WHERE p.polrelid = w.hist AND p.polname = w.probe;
SELECT (SELECT count(*) FROM pg_temp.want AS w
         WHERE NOT EXISTS (SELECT FROM pg_policy AS p WHERE p.polrelid = w.hist AND p.polname = w.copy_name))
     + (SELECT count(*) FROM pg_temp.fam AS f JOIN pg_policy AS p ON p.polrelid = f.hist
         WHERE p.polname ~ '^sql_saga_[0-9a-f]{16}$'
           AND NOT EXISTS (SELECT FROM pg_temp.want AS w WHERE w.hist = f.hist AND w.copy_name = p.polname))
     + (SELECT count(*) FROM pg_temp.want AS w JOIN pg_policy AS p ON p.polrelid = w.hist AND p.polname = w.copy_name
         WHERE p.polcmd <> 'r' OR p.polroles <> w.polroles OR p.polpermissive <> w.polpermissive
            OR pg_get_expr(p.polqual, p.polrelid) IS DISTINCT FROM w.qual);
ROLLBACK;
SQL
}

# What each test role sees through every _with_history view (rows per role), or 'denied'.
tenant_reads() {
    local db=$1 out="" sv role n
    for sv in $(psql -X -At -d "$db" -c "SELECT format('%I.%I', view_schema_name, view_table_name) FROM sql_saga.system_versioning ORDER BY 1"); do
        for role in ${PREFIX}_r1 ${PREFIX}_regular ${PREFIX}_admin; do
            n=$(psql -X -At -v ON_ERROR_STOP=1 -d "$db" -c "SET ROLE $role" -c "SELECT 'n=' || count(*) FROM $sv" 2>/dev/null | grep '^n=')
            out="$out $sv:$role=${n:-denied}"
        done
    done
    echo "$out"
}

# rls_history only: the tenants' views of rdoc history, as "role:ids" (ids of all rows seen, sorted).
# Designed outcome: r1 sees tenant a (id 1, current + history), regular and admin (inherits regular)
# see tenant b (id 2), saga_rt_none (no grant) is denied. Empty for other cases.
RLS_EXPECTED="${PREFIX}_r1:1,1 ${PREFIX}_regular:2,2 ${PREFIX}_admin:2,2 ${PREFIX}_none:denied"
rls_isolation() {
    local db=$1 out="" role ids
    if [ "$(psql -X -At -d "$db" -c "SELECT to_regclass('public.rdoc_with_history') IS NOT NULL")" != "t" ]; then
        echo ""; return
    fi
    for role in ${PREFIX}_r1 ${PREFIX}_regular ${PREFIX}_admin ${PREFIX}_none; do
        if ids=$(psql -X -At -v ON_ERROR_STOP=1 -d "$db" -c "SET ROLE $role" -c "SELECT 'ids=' || coalesce(string_agg(id::text, ',' ORDER BY id), '') FROM rdoc_with_history" 2>/dev/null | grep '^ids='); then
            ids=${ids#ids=}
        else
            ids=denied
        fi
        out="$out $role:$ids"
    done
    echo "${out# }"
}

# A DDL refusal by one of sql_saga's event triggers (drop_protection, rename_following, health_checks).
REFUSAL_RE='ERROR: +cannot (drop|drop or rename|drop or alter) .*(because it is (used|part of|managed)|call "sql_saga\.drop_)'

event_triggers() {
    psql -X -At -d "$1" -c "SELECT count(*) FILTER (WHERE evtenabled <> 'D') || '/' || count(*) FROM pg_event_trigger WHERE evtfoid::regproc::text LIKE 'sql_saga.%'"
}

# build_<case> <db>: create the source database contents. Must succeed with ON_ERROR_STOP.
common_header() {
    cat <<'SQL'
\set VERBOSITY terse
SET client_min_messages = warning;
CREATE EXTENSION btree_gist;
CREATE EXTENSION sql_saga CASCADE;
SQL
}

# F15: no grants at all. Only sql_saga's own catalog tables (era, system_versioning, updatable_view, ...)
# are restored; their foreign keys require era to be loaded first, also under pg_restore -j.
build_catalog_only() {
    { common_header; cat <<SQL
CREATE TABLE fp (id int, valid_range daterange NOT NULL, x text, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('fp', 'valid_range', 'valid');
SELECT sql_saga.add_for_portion_of_view('fp', 'valid');
CREATE TABLE sv (id int PRIMARY KEY, x text);
SELECT sql_saga.add_system_versioning('sv');
SQL
    } | psql_q -d "$1"
}

build_fpo_serial() {
    { common_header; cat <<SQL
DO \$\$ BEGIN
  FOR i IN 1..20 LOOP
    EXECUTE format('CREATE TABLE fp%s (id int, valid_range daterange NOT NULL, x text, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS))', i);
    PERFORM sql_saga.add_era(format('fp%s', i)::regclass, 'valid_range', 'valid');
    PERFORM sql_saga.add_for_portion_of_view(format('fp%s', i)::regclass, 'valid');
    EXECUTE format('GRANT SELECT, UPDATE ON fp%s TO ${PREFIX}_r1', i);
  END LOOP;
END \$\$;
SQL
    } | psql_q -d "$1"
}

# Row-level security on a versioned table: base policies (SELECT, ALL, INSERT-only), their copies on
# the history table, and a reader per tenant. After every restore there must be exactly one copy per
# SELECT/ALL base policy (the dumped copies carry the old installation token and are replaced), and
# the tenants must read only their own history.
build_rls_history() {
    { common_header; cat <<SQL
CREATE TABLE rdoc (id int PRIMARY KEY, tenant text NOT NULL, body text);
SELECT sql_saga.add_system_versioning('rdoc');
INSERT INTO rdoc VALUES (1, 'a', 'v1'), (2, 'b', 'v1');
UPDATE rdoc SET body = 'v2';
GRANT SELECT ON rdoc TO ${PREFIX}_r1, ${PREFIX}_regular;
ALTER TABLE rdoc ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_a ON rdoc FOR SELECT TO ${PREFIX}_r1 USING (tenant = 'a');
CREATE POLICY tenant_b ON rdoc FOR ALL TO ${PREFIX}_regular USING (tenant = 'b');
CREATE POLICY ins_only ON rdoc FOR INSERT TO ${PREFIX}_r1 WITH CHECK (true);
SQL
    } | psql_q -d "$1"
}

# Temporal and regular foreign keys, current and FOR PORTION OF views with synchronized columns, and a
# versioned regular table referencing a temporal one. The regular foreign key's triggers on p are created
# in post-data, after the catalog rows and after r's history policies are synchronized (the restore must
# not check for them in between; tmp/design_restore_clean.md section 5).
build_fk_views() {
    { common_header; cat <<SQL
CREATE TABLE p (id int, valid_range daterange NOT NULL, valid_from date, valid_until date, x text);
SELECT sql_saga.add_era('p', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('p', ARRAY['id'], key_type => 'primary', unique_key_name => 'p_id_valid');
SELECT sql_saga.add_for_portion_of_view('p', 'valid');
SELECT sql_saga.add_current_view('p');
CREATE TABLE c (id int, p_id int, valid_range daterange NOT NULL, x text);
SELECT sql_saga.add_era('c', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('c', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_foreign_key('c', ARRAY['p_id'], 'p', ARRAY['id']);
CREATE TABLE r (id int PRIMARY KEY, p_id int, note text);
SELECT sql_saga.add_regular_foreign_key('r', ARRAY['p_id'], 'p_id_valid');
SELECT sql_saga.add_system_versioning('r');
INSERT INTO p (id, valid_range, x) VALUES (1, '[2020-01-01,infinity)', 'a');
INSERT INTO c VALUES (1, 1, '[2021-01-01,infinity)', 'c');
INSERT INTO r VALUES (1, 1, 'n1');
UPDATE r SET note = 'n2';
GRANT SELECT, UPDATE ON p TO ${PREFIX}_r1;
GRANT SELECT ON r TO ${PREFIX}_r1;
SQL
    } | psql_q -d "$1"
}

# Known limitation: the catalog_only database, restored with pg_restore -j 4 (see header).
build_parallel_known_failure() { build_catalog_only "$1"; }

# Explicit grants only, no inheritance, 40 families (originally the parallel-restore case F12;
# kept as a larger serial case now that -j is unsupported).
build_fpo_many() {
    { common_header; cat <<SQL
DO \$\$ BEGIN
  FOR i IN 1..40 LOOP
    EXECUTE format('CREATE TABLE fp%s (id int, valid_range daterange NOT NULL, x text, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS))', i);
    PERFORM sql_saga.add_era(format('fp%s', i)::regclass, 'valid_range', 'valid');
    PERFORM sql_saga.add_for_portion_of_view(format('fp%s', i)::regclass, 'valid');
    EXECUTE format('GRANT SELECT, UPDATE ON fp%s TO ${PREFIX}_r1', i);
  END LOOP;
END \$\$;
SQL
    } | psql_q -d "$1"
}

# The downstream incident (statbus-481): a role that inherits another grantee is granted on the
# base table, and an unrelated function whose ACL dump statement is a REVOKE ... FROM PUBLIC.
build_inherited_role() {
    { common_header; cat <<SQL
CREATE TABLE establishment (id int, valid_range daterange NOT NULL, valid_from date, valid_until date, name text,
                            PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('establishment', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_for_portion_of_view('establishment', 'valid');
CREATE FUNCTION public.upgrade_schedule(text, boolean) RETURNS int LANGUAGE sql AS 'SELECT 1';
REVOKE ALL ON FUNCTION public.upgrade_schedule(text, boolean) FROM PUBLIC;
GRANT SELECT, INSERT ON establishment TO ${PREFIX}_regular;
GRANT SELECT, INSERT ON establishment TO ${PREFIX}_admin;
-- A function taking the table's row type makes pg_dump emit the table ACL before the REVOKE above.
CREATE FUNCTION public.existence_from(e public.establishment) RETURNS date LANGUAGE sql AS 'SELECT lower(e.valid_range)';
SQL
    } | psql_q -d "$1"
}

# F5: one SELECT grantee on a system-versioned table.
build_sv_one_grant() {
    { common_header; cat <<SQL
CREATE TABLE sv (id int PRIMARY KEY, x text);
SELECT sql_saga.add_system_versioning('sv');
GRANT SELECT ON sv TO ${PREFIX}_r1;
SQL
    } | psql_q -d "$1"
}

# D2: a history table and view whose custom names sort before the base table.
build_custom_history_name() {
    { common_header; cat <<SQL
CREATE TABLE zbase (id int PRIMARY KEY, x text);
SELECT sql_saga.add_system_versioning('zbase', history_table_name => 'aa_hist', view_name => 'ab_view');
GRANT SELECT ON zbase TO ${PREFIX}_r1;
SQL
    } | psql_q -d "$1"
}

# F13: default privileges revoke EXECUTE from PUBLIC before the as-of functions are created.
build_hardened_functions() {
    { common_header; cat <<SQL
ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
CREATE TABLE hv (id int PRIMARY KEY, x text);
SELECT sql_saga.add_system_versioning('hv');
GRANT SELECT ON hv TO ${PREFIX}_r1;
SQL
    } | psql_q -d "$1"
}

# restore <case> <variant> <dump> <src-digest> <src-triggers>
restore_variant() {
    local case=$1 variant=$2 dump=$3 src_digest=$4 src_trig=$5
    local dst="${PREFIX}_${case}_dst"
    local opts=(--exit-on-error)
    case "$variant" in
        serial) ;;
        parallel*) opts+=(-j 4) ;;
        no_owner) opts+=(--no-owner) ;;
        single_txn) opts+=(--single-transaction) ;;
        clean) opts+=(--clean --if-exists) ;;
        clean_single_txn) opts+=(--clean --if-exists --single-transaction) ;;
        clean_same) opts+=(--clean) ;;
    esac
    dropdb --if-exists "$dst" >>"$LOG" 2>&1
    createdb "$dst" || return 1
    local out="$WORK/${case}_${variant}.restore.log"
    local rc=0
    if [[ $variant == clean* ]]; then
        # The destination already holds the dumped database.
        if ! pg_restore --exit-on-error -d "$dst" "$dump" >"$WORK/${case}_${variant}.seed.log" 2>&1; then
            echo "    $variant: seeding restore FAILED: $(grep -m1 -E 'ERROR|error:' "$WORK/${case}_${variant}.seed.log")"
            dropdb --if-exists "$dst" >>"$LOG" 2>&1
            return 1
        fi
    fi
    case "$variant" in
        clean_same)
            pg_dump -Fc -d "$dst" -f "$WORK/${case}_same.dump" >>"$LOG" 2>&1 || rc=1
            [ $rc = 0 ] && { pg_restore "${opts[@]}" -d "$dst" "$WORK/${case}_same.dump" >"$out" 2>&1 || rc=1; } ;;
        clean_psql)
            ( set -o pipefail
              pg_restore --clean --if-exists -f - "$dump" \
                  | PGOPTIONS="${PGOPTIONS:-} -c sql_saga.clean_restore=on" psql -X -q -v ON_ERROR_STOP=1 -d "$dst" ) >"$out" 2>&1 || rc=1 ;;
        clean_psql_without_opt_in)
            ( set -o pipefail
              pg_restore --clean --if-exists -f - "$dump" | psql -X -q -v ON_ERROR_STOP=1 -d "$dst" ) >"$out" 2>&1 || rc=1 ;;
        *)
            pg_restore "${opts[@]}" -d "$dst" "$dump" >"$out" 2>&1 || rc=1 ;;
    esac
    # The negative variant: without the opt-in, a script run through psql is not recognized as a
    # pg_restore teardown, so sql_saga must refuse it (and nothing else may be what stops it).
    if [ "$variant" = clean_psql_without_opt_in ]; then
        dropdb --if-exists "$dst" >>"$LOG" 2>&1
        if [ $rc = 0 ]; then
            echo "    $variant: the teardown succeeded without the opt-in: sql_saga's drop protection did not hold"
            return 1
        fi
        if ! grep -qE "$REFUSAL_RE" "$out"; then
            echo "    $variant: failed, but not by sql_saga's drop protection: $(grep -m1 -E 'ERROR|error:' "$out")"
            return 1
        fi
        echo "    $variant: ok (refused: $(grep -m1 -oE "$REFUSAL_RE.*" "$out" | cut -c1-80)...)"
        return 0
    fi
    if [ $rc != 0 ]; then
        # Classify, so that unrelated failures are not conflated.
        local class="ACL"
        if grep -qE "$REFUSAL_RE" "$out"; then
            class="refused by sql_saga"
        fi
        if grep -qE 'COPY failed for table "(era|system_time_era|unique_keys|foreign_keys|system_versioning|updatable_view)"' "$out"; then
            class="F15 catalog load order"
        fi
        echo "    $variant: pg_restore FAILED [$class]: $(grep -m1 -E 'ERROR|error:' "$out" | sed -n '1p')"
        grep -m1 -A2 'ERROR' "$out" | sed 's/^/      /'
        dropdb --if-exists "$dst" >>"$LOG" 2>&1
        return 1
    fi
    local d t pol faults reads
    # clean*: a database with ALTER DEFAULT PRIVILEGES (hardened_functions) already has them when
    # pg_restore --clean recreates the extension, so its member functions get the owner-only ACL the
    # operator asked for, while a fresh restore applies the default privileges only at the end. That
    # difference is PostgreSQL's and intended; the comparison leaves extension member functions out.
    # Every other function (the as-of functions in particular) and every relation is still compared.
    if [[ $variant == clean* ]]; then
        d=$(digest "$dst" without_extension_functions); src_digest=$src_digest_without_extension_functions
    else
        d=$(digest "$dst")
    fi
    t=$(event_triggers "$dst"); pol=$(policies "$dst"); faults=$(policy_copy_faults "$dst")
    reads=$(tenant_reads "$dst"); local iso; iso=$(rls_isolation "$dst")
    dropdb --if-exists "$dst" >>"$LOG" 2>&1
    if [ "$d" != "$src_digest" ]; then
        echo "    $variant: ACL digest differs (source $src_digest, restored $d)"
        return 1
    fi
    if [ -n "$iso" ] && [ "$iso" != "$RLS_EXPECTED" ]; then
        echo "    $variant: tenant isolation wrong after restore (expected '$RLS_EXPECTED', got '$iso')"
        return 1
    fi
    if [ "$reads" != "$src_reads" ]; then
        echo "    $variant: tenant reads differ (source '$src_reads', restored '$reads')"
        return 1
    fi
    if [ "$faults" != "0" ]; then
        echo "    $variant: $faults history policy copies missing, duplicated or orphaned"
        return 1
    fi
    if [ "$pol" != "$src_pol" ]; then
        echo "    $variant: policies differ (source $src_pol, restored $pol)"
        return 1
    fi
    if [ "$t" != "$src_trig" ]; then
        echo "    $variant: event triggers differ (source $src_trig, restored $t)"
        return 1
    fi
    echo "    $variant: ok"
    return 0
}

run_case() {
    local case=$1
    local src="${PREFIX}_${case}_src"
    local dump="$WORK/${case}.dump"
    local ok=1
    echo "case $case"
    createdb "$src" || { FAILED+=("$case (createdb)"); return; }
    if ! "build_$case" "$src" >"$WORK/${case}.build.log" 2>&1; then
        echo "    build FAILED: $(grep -m1 ERROR "$WORK/${case}.build.log")"
        FAILED+=("$case (build)")
        dropdb --if-exists "$src" >>"$LOG" 2>&1
        return
    fi
    pg_dump -Fc -d "$src" -f "$dump" || { FAILED+=("$case (pg_dump)"); return; }
    local src_digest src_digest_without_extension_functions src_trig src_pol
    src_reads=''
    src_digest=$(digest "$src"); src_digest_without_extension_functions=$(digest "$src" without_extension_functions); src_trig=$(event_triggers "$src"); src_pol=$(policies "$src"); src_reads=$(tenant_reads "$src")
    local src_faults src_iso
    src_iso=$(rls_isolation "$src")
    if [ -n "$src_iso" ] && [ "$src_iso" != "$RLS_EXPECTED" ]; then
        echo "    source: tenant isolation wrong (expected '$RLS_EXPECTED', got '$src_iso')"
        ok=0
    fi
    src_faults=$(policy_copy_faults "$src")
    if [ "$src_faults" != "0" ]; then
        echo "    source: $src_faults history policy copies missing, duplicated or orphaned"
        ok=0
    fi
    dropdb --if-exists "$src" >>"$LOG" 2>&1
    local variants=(serial single_txn no_owner clean clean_single_txn clean_same clean_psql clean_psql_without_opt_in)
    # parallel_known_failure: the catalog_only database restored with -j 4, 20 times. The ordering
    # race fails about 40% of runs on PG 18.3, so 20 attempts make an accidental pass unlikely.
    if [ "$case" = parallel_known_failure ]; then
        variants=()
        for i in $(seq 1 20); do variants+=("parallel_$i"); done
        local pfail=0 other=0 vout
        for variant in "${variants[@]}"; do
            if ! vout=$(restore_variant "$case" "$variant" "$dump" "$src_digest" "$src_trig" 2>&1); then
                if grep -q 'F15 catalog load order' <<<"$vout"; then pfail=$((pfail+1)); else other=$((other+1)); echo "$vout"; fi
            fi
        done
        if [ $pfail -gt 0 ]; then
            KNOWN+=("$case: $pfail/20 parallel restores failed (PostgreSQL pg_dump bug, see doc/decision-log.md)")
        else
            # The bug is a race (about 15% of runs on PG 18.3), so 0/20 happens by chance about 4% of the
            # time. Check the archive instead: a fixed pg_dump records TABLE DATA -> TABLE DATA dependencies.
            if pg_restore -l -v "$dump" 2>/dev/null | grep -A1 'TABLE DATA sql_saga system_versioning' | grep -q 'depends on:.* .* '; then
                KNOWN+=("$case: archive records data load order: PostgreSQL appears to have fixed pg_dump; revisit doc/decision-log.md")
            else
                KNOWN+=("$case: 0/20 parallel restores failed by chance; the archive still lacks the load order (bug present)")
            fi
        fi
        echo "    parallel -j 4: $pfail/20 failed on catalog load order (known limitation, not counted)"
        if [ $other -gt 0 ]; then FAILED+=("$case ($other/20 parallel restores failed for another reason)"); fi
        return
    fi
    for variant in "${variants[@]}"; do
        restore_variant "$case" "$variant" "$dump" "$src_digest" "$src_trig" || ok=0
    done
    if [ $ok = 1 ]; then PASSED+=("$case"); else FAILED+=("$case"); fi
}

trap cleanup EXIT
cleanup
psql_q -d postgres <<SQL
CREATE ROLE ${PREFIX}_r1;
CREATE ROLE ${PREFIX}_regular;
CREATE ROLE ${PREFIX}_admin IN ROLE ${PREFIX}_regular;
CREATE ROLE ${PREFIX}_none;
GRANT USAGE ON SCHEMA public TO PUBLIC;
SQL

CASES=("$@")
[ ${#CASES[@]} -eq 0 ] && CASES=("${ALL_CASES[@]}")
for c in "${CASES[@]}"; do
    run_case "$c"
done

echo
echo "passed: ${#PASSED[@]}  failed: ${#FAILED[@]}"
for f in ${FAILED[@]+"${FAILED[@]}"}; do echo "  FAILED: $f"; done
for k in ${KNOWN[@]+"${KNOWN[@]}"}; do echo "  KNOWN LIMITATION: $k"; done
echo "logs: $WORK"
[ ${#FAILED[@]} -eq 0 ]
