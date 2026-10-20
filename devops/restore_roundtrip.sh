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
# The split variant (split_1 .. split_20, for the cases in SPLIT_CASES) runs the README's three-invocation
# split restore and its final history policy synchronization EXACTLY as documented: the lines between "# BEGIN split-restore" and "# END split-restore"
# in README.md are extracted at run time and executed with backup.dump and mydb bound to this run's dump
# and destination database. The block runs as a script in a fresh `bash -eu` process, so the first
# failing line stops it and fails the run whatever the caller's errexit context. SPLIT_RUNS (default 20,
# a positive integer) consecutive runs per case; each one is a hard failure if it fails. Fewer than 20
# is a smoke run, not acceptance.
#
# RESTORE_EXTENSION_CONTROL_PATH / RESTORE_DYNAMIC_LIBRARY_PATH (optional) are set on every database
# this script creates, to run the cases against another build (for example the released v1.1).
# BASELINE_V11=1 skips the two checks a 1.1 build cannot answer (check_integrity(), column snapshots);
# SPLIT_ONLY=1 runs only the split variant: only the selected cases that are in SPLIT_CASES are built
# and restored, no other variant and no parallel restore runs, and the other cases are reported as
# SKIPPED (never as passed). A run in which nothing passed exits non-zero.
#
# devops/restore_roundtrip.sh --self-test checks the harness's own control flow with mocked commands
# on PATH and no server (see self_test).
#
# Parallel restore (`pg_restore -j N`) is NOT supported (PostgreSQL pg_dump drops the load order
# of extension configuration tables from the archive; see doc/decision-log.md). The
# `parallel_known_failure` case still runs `-j 4` restores and reports their outcome, but does not
# fail the run; if they all succeed (PostgreSQL fixed), the summary says so.
#
# Old-dump cases (old_dump_10, old_dump_11, old_dump_12; tmp/v13_olddump_design.md): a database made by the RELEASED
# v1.0 / v1.1 / v1.2 build (tag built in a scratch dir, with its own C and native libraries, selected per
# database through extension_control_path / dynamic_library_path) is dumped and restored into the
# candidate package with the procedure documented in doc/upgrade.md between "<!-- BEGIN old-dump-restore -->"
# and "<!-- END old-dump-restore -->": the ```bash block there is extracted at run time and executed in
# a fresh `bash -eu` with backup.dump, mydb and the dump's version (variable OLD_VERSION) bound. Afterwards
# the destination must be at the candidate's default version, check_integrity() must pass, every user
# row and history row must equal the source, and the restored database must equal an in-place
# `ALTER EXTENSION sql_saga UPDATE` of the source (schema dump, history policy tokens normalized).
# With a 1.3+ candidate, a plain pg_restore of an old dump (no preinstalled old version) must fail at the era
# data with the exact catalog_format CHECK ("restore old sql_saga dumps per doc/upgrade.md"), not a later
# incidental error, and a --single-transaction attempt must leave no extension and no rows behind.
# OLD_DUMP_BUILD_DIR (optional) reuses prebuilt tag trees (<dir>/v1.0, <dir>/v1.1, <dir>/v1.2) instead of building.
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
SKIPPED=()
SPLIT_RESULTS=()

ALL_CASES=(catalog_only fpo_serial fpo_many inherited_role sv_one_grant custom_history_name hardened_functions rls_history manual_schema_changes fk_views quoted_names regular_fk_order parallel_known_failure old_dump_10 old_dump_11 old_dump_12)

psql_q() { psql -X -q -v ON_ERROR_STOP=1 "$@"; }

SPLIT_CASES=(catalog_only manual_schema_changes fk_views quoted_names rls_history regular_fk_order)
# Split cases for which a regular-FK CHECK load-order failure is NOT the documented README limitation but a
# hard failure: regular_fk_order states the 1.3 contract (design B, the referencing side is a post-data
# constraint trigger, so the -j data phase cannot trip it; tmp/v13_designB_red2_brief.md N2).
SPLIT_HARD_CHECK_ORDER_CASES=(regular_fk_order)
SPLIT_RUNS=${SPLIT_RUNS:-20}
README_PATH="$(cd "$(dirname "$0")/.." && pwd)/README.md"

# new_db <db>: createdb, plus the optional alternative build paths.
new_db() {
    createdb "$1" || return 1
    if [ -n "${RESTORE_EXTENSION_CONTROL_PATH:-}" ]; then
        psql -X -q -d postgres -c "ALTER DATABASE \"$1\" SET extension_control_path = '${RESTORE_EXTENSION_CONTROL_PATH}'" >>"$LOG" 2>&1 || return 1
    fi
    if [ -n "${RESTORE_DYNAMIC_LIBRARY_PATH:-}" ]; then
        psql -X -q -d postgres -c "ALTER DATABASE \"$1\" SET dynamic_library_path = '${RESTORE_DYNAMIC_LIBRARY_PATH}'" >>"$LOG" 2>&1 || return 1
    fi
}

# The README's split restore block, verbatim (between the markers, markers excluded).
split_block() {
    awk '/^# END split-restore/ { inside = 0 } inside { print } /^# BEGIN split-restore/ { inside = 1 }' "$README_PATH"
}

# run_split_block <splitdir> <dump> <dst>: the README block in a FRESH `bash -eu` process (so no
# caller context, such as `cmd || rc=1` or `if ! x=$(...)`, can switch errexit off inside it), in
# <splitdir> holding a copy of <dump> as backup.dump, with `-d mydb` bound to <dst>. Returns non-zero
# if mkdir or cp fails (nothing is run then) or if any README line fails (later lines are not run).
run_split_block() {
    local splitdir=$1 dump=$2 dst=$3 block
    if ! mkdir -p "$splitdir"; then echo "split: mkdir $splitdir failed"; return 1; fi
    if ! cp "$dump" "$splitdir/backup.dump"; then echo "split: cp $dump failed"; return 1; fi
    if ! block=$(split_block); then echo "split: cannot read $README_PATH"; return 1; fi
    if ! printf '%s\n' "$block" | sed -e 's/ -d mydb / -d "$mydb" /g' >"$splitdir/split_restore.sh"; then
        echo "split: cannot write $splitdir/split_restore.sh"; return 1
    fi
    ( cd "$splitdir" && mydb=$dst /bin/bash -eu ./split_restore.sh )
}

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

# Every SYSTEM_TIME family must carry the portable before-shape used for relation-scoped attribution.
# Return "missing" on pre-1.2 candidates so the RED harness reports the intended schema gap cleanly.
snapshot_faults() {
    local has_column
    has_column=$(psql -X -At -d "$1" -c "SELECT EXISTS (SELECT FROM pg_catalog.pg_attribute WHERE attrelid = 'sql_saga.system_time_era'::regclass AND attname = 'table_columns_snapshot' AND attnum > 0 AND NOT attisdropped)")
    if [ "$has_column" != t ]; then
        echo missing
        return
    fi
    psql -X -At -d "$1" <<'SQL'
SELECT count(*)
FROM sql_saga.system_time_era AS ste
WHERE ste.table_columns_snapshot IS DISTINCT FROM (
    SELECT array_agg(a.attname ORDER BY a.attnum)
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS ns ON ns.oid = c.relnamespace
    JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
    WHERE (ns.nspname, c.relname) = (ste.table_schema, ste.table_name)
      AND a.attnum > 0
      AND NOT a.attisdropped
);
SQL
}

# Schema and data fingerprint for the manual_schema_changes case. Other cases return an empty value.
manual_schema_state() {
    # A CASE does not stop 'manual_sv'::regclass from being resolved at plan time, so check first.
    if [ "$(psql -X -At -d "$1" -c "SELECT to_regclass('public.manual_sv') IS NOT NULL")" != t ]; then
        echo ''
        return
    fi
    psql -X -At -d "$1" <<'SQL'
SELECT md5(concat_ws(E'\n',
    (SELECT string_agg(format('%s|%s|%s', c.relname, a.attname,
                                    format_type(a.atttypid, a.atttypmod)), E'\n'
                       ORDER BY c.relname, a.attnum)
       FROM pg_class AS c
       JOIN pg_attribute AS a ON a.attrelid = c.oid
      WHERE c.oid IN ('manual_sv'::regclass, 'manual_sv_history'::regclass,
                      'manual_sv_with_history'::regclass, 'manual_sv_history_gated'::regclass)
        AND a.attnum > 0 AND NOT a.attisdropped),
    (SELECT jsonb_agg(to_jsonb(r) ORDER BY r.id, r.system_valid_range, r.body)::text
       FROM (SELECT id, account, body, system_valid_range::text, legacy_note
               FROM manual_sv_with_history) AS r),
    (SELECT format('%s|%s', c.relrowsecurity, c.relforcerowsecurity)
       FROM pg_class AS c WHERE c.oid = 'manual_sv'::regclass),
    (SELECT string_agg(format('%s|%s|%s', p.polcmd,
                              pg_get_expr(p.polqual, p.polrelid),
                              (SELECT string_agg(CASE WHEN role_oid = 0 THEN 'PUBLIC' ELSE role_oid::regrole::text END, ',' ORDER BY 1)
                                 FROM unnest(p.polroles) AS role_oid)), E'\n' ORDER BY p.polcmd, p.polname)
       FROM pg_policy AS p WHERE p.polrelid = 'manual_sv'::regclass)
));
SQL
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

# A system-versioned table after manual schema changes: history-only data, an RLS policy whose
# referenced column is renamed in history first, and rebuilt views/functions.
build_manual_schema_changes() {
    { common_header; cat <<SQL
CREATE TABLE manual_sv (id int PRIMARY KEY, tenant text NOT NULL, body text);
SELECT sql_saga.add_system_versioning('manual_sv');
INSERT INTO manual_sv VALUES (1, 'a', 'a1'), (2, 'b', 'b1');
UPDATE manual_sv SET body = body || '+';
GRANT SELECT ON manual_sv TO ${PREFIX}_r1;
ALTER TABLE manual_sv ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_a ON manual_sv FOR SELECT TO ${PREFIX}_r1 USING (tenant = 'a');
ALTER TABLE manual_sv_history ADD COLUMN legacy_note text;
UPDATE manual_sv_history SET legacy_note = 'old ' || tenant;
SELECT sql_saga.rebuild_system_versioning_views('manual_sv', ARRAY['tenant']);
ALTER TABLE manual_sv_history RENAME COLUMN tenant TO account;
ALTER TABLE manual_sv RENAME COLUMN tenant TO account;
SELECT sql_saga.rebuild_system_versioning_views('manual_sv');
UPDATE manual_sv SET body = 'a2' WHERE id = 1;
SQL
    } | psql_q -d "$1"
}

# Schema and table names that need quoting, including spaces: pg_restore --clean then issues
# ALTER TABLE ONLY "my schema"."My Table" DROP CONSTRAINT ..., which must still be recognized as teardown.
build_quoted_names() {
    { common_header; cat <<SQL
CREATE SCHEMA "my schema";
CREATE TABLE "my schema"."My Table" (id int, valid_range daterange NOT NULL, x text);
SELECT sql_saga.add_era('"my schema"."My Table"', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('"my schema"."My Table"', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_for_portion_of_view('"my schema"."My Table"', 'valid');
CREATE TABLE "my schema"."Ref Table" (id int PRIMARY KEY, p_id int, note text);
SELECT sql_saga.add_regular_foreign_key('"my schema"."Ref Table"', ARRAY['p_id'],
    (SELECT unique_key_name FROM sql_saga.unique_keys WHERE table_name = 'My Table'));
SELECT sql_saga.add_system_versioning('"my schema"."Ref Table"');
INSERT INTO "my schema"."My Table" VALUES (1, '[2020-01-01,infinity)', 'a');
INSERT INTO "my schema"."Ref Table" VALUES (1, 1, 'n1');
UPDATE "my schema"."Ref Table" SET note = 'n2';
GRANT USAGE ON SCHEMA "my schema" TO ${PREFIX}_r1;
GRANT SELECT ON "my schema"."My Table", "my schema"."Ref Table" TO ${PREFIX}_r1;
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

# Regular foreign keys whose child table sorts before its parent ("activity" and "contact" load before
# "legal_unit"; "stat" after it), StatBus-shaped: a single-column and a composite key, NULL references
# and a temporal sibling (establishment) with a temporal FK to the same parent. pg_dump loads table
# data in name order, so a referencing-side check that reads the parent while the child's COPY runs
# fails even a serial or --single-transaction restore (1.0 to 1.2: a CHECK constraint calling the
# helper; tmp/fable_audit_v13_brief.md defect B).
build_regular_fk_order() {
    { common_header; cat <<SQL
CREATE TABLE legal_unit (id int NOT NULL, ident_type text NOT NULL, ident text NOT NULL,
                         valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('legal_unit', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('legal_unit', ARRAY['id'], key_type => 'primary', unique_key_name => 'legal_unit_id_valid');
SELECT sql_saga.add_unique_key('legal_unit', ARRAY['ident_type', 'ident'], key_type => 'natural', unique_key_name => 'legal_unit_ident_valid');
CREATE TABLE activity (id int PRIMARY KEY, legal_unit_id int);
CREATE TABLE contact (id int PRIMARY KEY, ident_type text, ident text);
CREATE TABLE stat (id int PRIMARY KEY, legal_unit_id int);
SELECT sql_saga.add_foreign_key('activity', ARRAY['legal_unit_id'], 'legal_unit', ARRAY['id']);
SELECT sql_saga.add_foreign_key('stat', ARRAY['legal_unit_id'], 'legal_unit', ARRAY['id']);
SELECT sql_saga.add_regular_foreign_key('contact', ARRAY['ident_type', 'ident'], 'legal_unit_ident_valid');
-- A temporal child (native temporal FK, deferrable) that also sorts before its parent.
CREATE TABLE establishment (id int NOT NULL, legal_unit_id int,
                            valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('establishment', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('establishment', ARRAY['id'], key_type => 'primary', unique_key_name => 'establishment_id_valid');
SELECT sql_saga.add_foreign_key('establishment', ARRAY['legal_unit_id'], 'legal_unit', ARRAY['id']);
INSERT INTO legal_unit (id, ident_type, ident, valid_from, valid_until)
    SELECT g, 'tax', g::text, '2020-01-01', 'infinity' FROM generate_series(1, 50) AS g;
INSERT INTO activity SELECT g, 1 + g % 50 FROM generate_series(1, 100) AS g;
INSERT INTO activity VALUES (101, NULL);
INSERT INTO stat SELECT g, 1 + g % 50 FROM generate_series(1, 100) AS g;
INSERT INTO contact SELECT g, 'tax', (1 + g % 50)::text FROM generate_series(1, 100) AS g;
INSERT INTO contact VALUES (101, NULL, '999');
INSERT INTO establishment (id, legal_unit_id, valid_from, valid_until)
    SELECT g, g, '2021-01-01', 'infinity' FROM generate_series(1, 50) AS g;
GRANT SELECT ON legal_unit, activity, contact, stat TO ${PREFIX}_r1;
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

# ---------------------------------------------------------------------------------------------
# Old dumps: databases made by the released v1.0 / v1.1 builds, restored into the candidate package
# with the documented procedure, then updated (tmp/v13_olddump_design.md).
# ---------------------------------------------------------------------------------------------
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OLD_DUMP_DOC_PATH=${OLD_DUMP_DOC_PATH:-$REPO_ROOT/doc/upgrade.md}
OLD_DUMP_TAGS="v1.0|1.0|9e39fcc v1.1|1.1|9c72e66 v1.2|1.2|835058e"

# The doc's old-dump procedure: the ```bash block between the old-dump-restore markers, fences excluded.
old_dump_block() {
    awk '/<!-- END old-dump-restore -->/ { inside = 0 }
         inside && /^```/ { fence = !fence; next }
         inside && fence { print }
         /<!-- BEGIN old-dump-restore -->/ { inside = 1 }' "$OLD_DUMP_DOC_PATH"
}

# old_dump_build <tag> <version> <commit>: build the released tag (C, SQL and native library) and assemble
# <WORK>/old_<version>/{share/extension,lib} holding only that release. Prints the root.
old_dump_build() {
    local tag=$1 version=$2 commit=$3 root="$WORK/old_$2" tree tag_commit dlsuffix native
    tag_commit=$(git -C "$REPO_ROOT" rev-parse --verify -q "$tag^{commit}") || { echo "tag $tag not found" >&2; return 1; }
    case "$tag_commit" in "$commit"*) ;; *) echo "$tag is $tag_commit, not released commit $commit" >&2; return 1 ;; esac
    if [ -n "${OLD_DUMP_BUILD_DIR:-}" ] && [ -d "$OLD_DUMP_BUILD_DIR/$tag" ]; then
        tree="$OLD_DUMP_BUILD_DIR/$tag"
    else
        tree="$WORK/build_$tag"
        mkdir -p "$tree" && git -C "$REPO_ROOT" archive "$tag" | tar -x -C "$tree" || return 1
        make -C "$tree" PG_CONFIG="${PG_CONFIG:-pg_config}" >"$WORK/build_$tag.log" 2>&1 || { echo "building $tag failed (see $WORK/build_$tag.log)" >&2; return 1; }
    fi
    case "$(uname)" in Darwin) dlsuffix=.dylib ;; *) dlsuffix=.so ;; esac
    native="$tree/native/target/release/libsql_saga_native$dlsuffix"
    if [ ! -f "$native" ]; then
        # v1.0's Makefile left the native library to cargo-pgrx; build it with plain cargo like later releases.
        if [ "$(uname)" = Darwin ]; then
            export LIBCLANG_PATH=${LIBCLANG_PATH:-$(dirname "$(xcrun --find clang)")/../lib}
            export BINDGEN_EXTRA_CLANG_ARGS=${BINDGEN_EXTRA_CLANG_ARGS:--isysroot $(xcrun --show-sdk-path)}
        fi
        ( cd "$tree/native" && PGRX_PG_CONFIG_PATH="$(command -v "${PG_CONFIG:-pg_config}")" cargo build --release --lib --locked ) \
            >>"$WORK/build_$tag.log" 2>&1 || { echo "building the $tag native library failed (see $WORK/build_$tag.log)" >&2; return 1; }
    fi
    mkdir -p "$root/share/extension" "$root/lib" || return 1
    cp "$tree/sql_saga.control" "$tree/sql_saga--$version.sql" "$root/share/extension/" || return 1
    grep -q "^default_version = '$version'" "$root/share/extension/sql_saga.control" || { echo "$tag control is not $version" >&2; return 1; }
    cp "$tree/sql_saga$dlsuffix" "$root/lib/" && cp "$native" "$root/lib/sql_saga_native$dlsuffix" || return 1
    chmod -R a+rX "$WORK"
    echo "$root"
}

# The source database, valid on 1.0 and 1.1: eras with synchronized columns, primary and natural keys,
# a native temporal FK, single and composite regular FKs (child names sort after their parents, so the
# inline CHECK of class B does not interfere), a for_portion_of view, system versioning with history,
# and grants. No XOR key (defect A1 is tested elsewhere).
build_old_dump_source() {
    psql_q -d "$1" <<SQL
\set VERBOSITY terse
SET client_min_messages = warning;
CREATE EXTENSION btree_gist;
CREATE EXTENSION sql_saga;
CREATE SCHEMA app;
CREATE TABLE app.legal_unit (id int GENERATED BY DEFAULT AS IDENTITY,
    valid_range daterange NOT NULL, valid_from date NOT NULL, valid_to date, valid_until date,
    name text, edit_comment text, edit_by_user_id int, edit_at timestamptz DEFAULT now());
SELECT sql_saga.add_era('app.legal_unit', 'valid_range', 'valid', ephemeral_columns => ARRAY['edit_comment','edit_by_user_id','edit_at']);
SELECT sql_saga.add_unique_key(table_oid => 'app.legal_unit', key_type => 'primary', column_names => ARRAY['id'], unique_key_name => 'legal_unit_id_valid');
SELECT sql_saga.add_for_portion_of_view('app.legal_unit');
CREATE TABLE app.region (code text NOT NULL, part int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('app.region', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'app.region', key_type => 'natural', column_names => ARRAY['code','part'], unique_key_name => 'region_code_part_valid');
CREATE TABLE app.establishment (id int GENERATED BY DEFAULT AS IDENTITY,
    valid_range daterange NOT NULL, valid_from date NOT NULL, valid_to date, valid_until date, name text, legal_unit_id int);
SELECT sql_saga.add_era('app.establishment', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'app.establishment', key_type => 'primary', column_names => ARRAY['id'], unique_key_name => 'establishment_id_valid');
SELECT sql_saga.add_foreign_key(fk_table_oid => 'app.establishment', fk_column_names => ARRAY['legal_unit_id'], pk_table_oid => 'app.legal_unit', pk_column_names => ARRAY['id']);
CREATE TABLE app.lu_tag (id int PRIMARY KEY, legal_unit_id int, note text);
SELECT sql_saga.add_foreign_key(fk_table_oid => 'app.lu_tag', fk_column_names => ARRAY['legal_unit_id'], pk_table_oid => 'app.legal_unit', pk_column_names => ARRAY['id']);
CREATE TABLE app.region_ref (id int PRIMARY KEY, code text, part int);
SELECT sql_saga.add_foreign_key(fk_table_oid => 'app.region_ref', fk_column_names => ARRAY['code','part'], pk_table_oid => 'app.region', pk_column_names => ARRAY['code','part']);
CREATE TABLE app.setting (key text PRIMARY KEY, value text);
SELECT sql_saga.add_system_versioning('app.setting');
INSERT INTO app.legal_unit (id, valid_from, valid_until, name) SELECT g, '2020-01-01', 'infinity', 'LU ' || g FROM generate_series(1, 20) AS g;
UPDATE app.legal_unit__for_portion_of_valid SET name = 'LU 3 mid', valid_from = '2021-01-01', valid_until = '2022-01-01' WHERE id = 3;
INSERT INTO app.region VALUES ('A', 1, '[2020-01-01,infinity)'), ('B', 2, '[2020-01-01,infinity)');
INSERT INTO app.establishment (id, valid_from, valid_until, name, legal_unit_id) SELECT g, '2021-01-01', 'infinity', 'EST ' || g, 1 + g % 20 FROM generate_series(1, 30) AS g;
INSERT INTO app.lu_tag SELECT g, 1 + g % 20, 't' FROM generate_series(1, 40) AS g;
INSERT INTO app.region_ref VALUES (1, 'A', 1), (2, 'B', 2);
INSERT INTO app.setting VALUES ('a', '1');
UPDATE app.setting SET value = '2';
UPDATE app.setting SET value = '3';
GRANT USAGE ON SCHEMA app TO ${PREFIX}_r1;
GRANT SELECT ON app.legal_unit, app.lu_tag TO ${PREFIX}_r1;
SQL
}

# User rows, history rows and catalog row counts: equal on the source and on every correct restore.
old_dump_fingerprint() {
    psql -X -At -v ON_ERROR_STOP=1 -d "$1" <<'SQL'
SELECT md5(concat_ws(E'\n',
    (SELECT string_agg(format('%s|%s|%s|%s|%s', id, valid_range, valid_from, valid_until, name), ',' ORDER BY id, valid_range) FROM app.legal_unit),
    (SELECT string_agg(format('%s|%s|%s', id, valid_range, legal_unit_id), ',' ORDER BY id, valid_range) FROM app.establishment),
    (SELECT string_agg(format('%s|%s', id, legal_unit_id), ',' ORDER BY id) FROM app.lu_tag),
    (SELECT string_agg(format('%s|%s|%s', id, code, part), ',' ORDER BY id) FROM app.region_ref),
    (SELECT string_agg(format('%s|%s|%s', key, value, system_valid_range), ',' ORDER BY key) FROM app.setting),
    (SELECT string_agg(format('%s|%s|%s', key, value, system_valid_range), ',' ORDER BY system_valid_range) FROM app.setting_history),
    (SELECT format('era=%s uk=%s fk=%s sv=%s views=%s', (SELECT count(*) FROM sql_saga.era), (SELECT count(*) FROM sql_saga.unique_keys),
            (SELECT count(*) FROM sql_saga.foreign_keys), (SELECT count(*) FROM sql_saga.system_versioning), (SELECT count(*) FROM sql_saga.updatable_view)))));
SQL
}

# Full dump with the per-installation history policy names normalized (they differ by design).
old_dump_normalized_dump() {
    pg_dump -d "$1" | grep -v -e '^\\' -e '^--' \
        | sed -E 's/sql_saga_[0-9a-f]{16}/sql_saga_<hash>/g; s/sql_saga_history_(reader|base_select)_[0-9a-zA-Z]+/sql_saga_history_\1_<token>/g'
}

# old_dump_strict <candidate version>: 1.3 and later refuse an old dump loaded without its own version.
old_dump_strict() { case "$1" in 1.0|1.1|1.2|'') return 1 ;; *) return 0 ;; esac; }

run_old_dump_case() {
    local case=$1 spec tag version commit rest root src ref dst dump block dir out ok=1 v want=''
    case "$case" in old_dump_10) want=1.0 ;; old_dump_11) want=1.1 ;; old_dump_12) want=1.2 ;; esac
    for spec in $OLD_DUMP_TAGS; do
        tag=${spec%%|*}; rest=${spec#*|}; version=${rest%%|*}; commit=${rest#*|}
        [ "$version" = "$want" ] && break
    done
    echo "case $case ($tag dump restored into the candidate package, then ALTER EXTENSION UPDATE)"
    if ! root=$(old_dump_build "$tag" "$version" "$commit" 2>"$WORK/$case.build.err"); then
        echo "    build FAILED: $(cat "$WORK/$case.build.err")"; FAILED+=("$case (building $tag)"); return
    fi
    src="${PREFIX}_${case}_src"; ref="${PREFIX}_${case}_ref"; dst="${PREFIX}_${case}_dst"; dump="$WORK/$case.dump"
    createdb "$src" || { FAILED+=("$case (createdb)"); return; }
    psql -X -q -d postgres -c "ALTER DATABASE \"$src\" SET extension_control_path = '$root/share:\$system'" \
                           -c "ALTER DATABASE \"$src\" SET dynamic_library_path = '$root/lib:\$libdir'" >>"$LOG" 2>&1
    if ! build_old_dump_source "$src" >"$WORK/$case.source.log" 2>&1; then
        echo "    source build FAILED: $(grep -m1 ERROR "$WORK/$case.source.log")"; FAILED+=("$case (source)"); return
    fi
    v=$(psql -X -At -d "$src" -c "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")
    [ "$v" = "$version" ] || { echo "    source is sql_saga $v, not $version"; FAILED+=("$case (source version)"); return; }
    pg_dump -Fc -d "$src" -f "$dump" || { FAILED+=("$case (pg_dump)"); return; }
    local src_fp; src_fp=$(old_dump_fingerprint "$src")

    # Reference: the same database updated in place with the candidate package.
    createdb -T "$src" "$ref" >>"$LOG" 2>&1 && psql -X -q -d postgres -c "ALTER DATABASE \"$ref\" RESET ALL" >>"$LOG" 2>&1 || { FAILED+=("$case (reference copy)"); return; }
    for v in extension_control_path dynamic_library_path; do
        case $v in extension_control_path) out=${RESTORE_EXTENSION_CONTROL_PATH:-} ;; *) out=${RESTORE_DYNAMIC_LIBRARY_PATH:-} ;; esac
        [ -z "$out" ] || psql -X -q -d postgres -c "ALTER DATABASE \"$ref\" SET $v = '$out'" >>"$LOG" 2>&1
    done
    if ! psql -X -q -v ON_ERROR_STOP=1 -d "$ref" -c "ALTER EXTENSION sql_saga UPDATE" >"$WORK/$case.ref.log" 2>&1; then
        echo "    in-place update of the source FAILED: $(grep -m1 ERROR "$WORK/$case.ref.log")"; FAILED+=("$case (in-place update)"); return
    fi
    local candidate_version; candidate_version=$(psql -X -At -d "$ref" -c "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")

    # A plain pg_restore (no preinstalled old version) into the candidate package. Up to a 1.2 candidate this
    # is informational. From 1.3 the dump's era rows (no catalog_format) must be refused by the named
    # catalog_format CHECK, first and exactly, and the single transaction leaves nothing behind.
    new_db "$dst" || { FAILED+=("$case (createdb)"); return; }
    if pg_restore --exit-on-error --single-transaction -d "$dst" "$dump" >"$WORK/$case.plain.log" 2>&1; then
        if old_dump_strict "$candidate_version"; then
            echo "    FAIL: plain pg_restore of a $version dump into the $candidate_version package succeeded (must be refused by catalog_format)"; ok=0
        else
            KNOWN+=("$case: plain pg_restore succeeded as sql_saga $(psql -X -At -d "$dst" -c "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'") without running any update script")
        fi
    elif old_dump_strict "$candidate_version"; then
        local first_err; first_err=$(grep -m1 -E 'ERROR' "$WORK/$case.plain.log" | sed 's/^.*ERROR: *//')
        case "$first_err" in
            'new row for relation "era" violates check constraint "restore old sql_saga dumps per doc/upgrade.md"'*)
                echo "    plain restore refused first by the catalog_format constraint: ok" ;;
            *) echo "    FAIL: plain restore refused, but the first error is not the catalog_format constraint: $first_err"; ok=0 ;;
        esac
        out=$(psql -X -At -d "$dst" -c "SELECT (SELECT count(*) FROM pg_extension WHERE extname = 'sql_saga') || '|' || (SELECT count(*) FROM pg_namespace WHERE nspname = 'app')" 2>&1)
        if [ "$out" = "0|0" ]; then echo "    refused single-transaction restore left no extension, schema or rows: ok"
        else echo "    FAIL: refused single-transaction restore left state behind (extension|app schema: $out)"; ok=0; fi
        # Serial restore: the same named refusal comes first (no transaction to roll back, version state named only).
        dropdb --if-exists "$dst" >>"$LOG" 2>&1; new_db "$dst" || { FAILED+=("$case (createdb)"); return; }
        pg_restore --exit-on-error -d "$dst" "$dump" >"$WORK/$case.plain_serial.log" 2>&1
        first_err=$(grep -m1 -E 'ERROR' "$WORK/$case.plain_serial.log" | sed 's/^.*ERROR: *//')
        case "$first_err" in
            'new row for relation "era" violates check constraint "restore old sql_saga dumps per doc/upgrade.md"'*)
                echo "    serial plain restore refused first by the catalog_format constraint: ok" ;;
            *) echo "    FAIL: serial plain restore: first error is not the catalog_format constraint: $first_err"; ok=0 ;;
        esac
        out=$(psql -X -At -d "$dst" -c "SELECT (SELECT extversion FROM pg_extension WHERE extname = 'sql_saga') || '|' || (SELECT count(*) FROM sql_saga.era)" 2>&1)
        if [ "$out" = "$candidate_version|0" ]; then echo "    serial refusal: candidate version, no era row loaded: ok"
        else echo "    FAIL: serial refusal state (extversion|era rows) is '$out', expected '$candidate_version|0'"; ok=0; fi
    else
        KNOWN+=("$case: plain pg_restore into the candidate package fails: $(grep -m1 -E 'ERROR' "$WORK/$case.plain.log" | cut -c1-140)")
    fi
    dropdb --if-exists "$dst" >>"$LOG" 2>&1

    # The documented procedure.
    block=$(old_dump_block)
    if [ -z "$block" ]; then
        echo "    FAIL: no documented old-dump procedure (a \`\`\`bash block between <!-- BEGIN old-dump-restore --> and <!-- END old-dump-restore --> in $OLD_DUMP_DOC_PATH)"
        FAILED+=("$case (no documented procedure for restoring a $version dump)"); return
    fi
    if [ "$(printf '%s\n' "$block" | grep -c '^OLD_VERSION=')" != 1 ]; then
        echo "    FAIL: the documented procedure must set OLD_VERSION= on exactly one line"; FAILED+=("$case (procedure shape)"); return
    fi
    new_db "$dst" || { FAILED+=("$case (createdb)"); return; }
    dir="$WORK/$case.procedure"; mkdir -p "$dir"; cp "$dump" "$dir/backup.dump"
    printf '%s\n' "$block" | sed -E "s/^OLD_VERSION=[^ ]*/OLD_VERSION=$version/; s/(^|[^A-Za-z0-9_\$\"])mydb([^A-Za-z0-9_]|\$)/\\1\"\$mydb\"\\2/g" >"$dir/procedure.sh"
    if ! ( cd "$dir" && mydb=$dst /bin/bash -eu ./procedure.sh ) >"$WORK/$case.procedure.log" 2>&1; then
        echo "    documented procedure FAILED: $(grep -m1 -E 'ERROR|error:' "$WORK/$case.procedure.log")"; ok=0
    else
        v=$(psql -X -At -d "$dst" -c "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")
        if [ "$v" = "$candidate_version" ]; then echo "    restored and updated to $v: ok"; else echo "    FAIL: restored database is sql_saga $v, expected $candidate_version"; ok=0; fi
        out=$(psql -X -At -d "$dst" -c "SELECT sql_saga.check_integrity()::text = ''" 2>&1)
        if [ "$out" = t ]; then echo "    check_integrity: ok"; else echo "    FAIL: check_integrity: $out"; ok=0; fi
        if [ "$(old_dump_fingerprint "$dst")" = "$src_fp" ]; then echo "    user data, history and catalog rows equal the source: ok"; else echo "    FAIL: data differs from the source"; ok=0; fi
        if diff <(old_dump_normalized_dump "$ref") <(old_dump_normalized_dump "$dst") >"$WORK/$case.vs_inplace.diff"; then
            echo "    equals the in-place update of the source: ok"
        else
            echo "    FAIL: differs from the in-place update of the source ($(grep -c '^[<>]' "$WORK/$case.vs_inplace.diff") lines, $WORK/$case.vs_inplace.diff)"; ok=0
        fi
        # (any error: 1.3 may enforce the child side with a constraint trigger instead of the CHECK)
        out=$(psql -X -q -At -d "$dst" -v ON_ERROR_STOP=1 -c "BEGIN" -c "INSERT INTO app.lu_tag VALUES (999, 9999, 'orphan')" -c "ROLLBACK" 2>&1 | grep -c 'ERROR')
        [ "$out" = 1 ] && echo "    regular FK still enforced: ok" || { echo "    FAIL: regular FK not enforced after restore"; ok=0; }
        out=$(psql -X -q -At -d "$dst" -v ON_ERROR_STOP=1 -c "BEGIN" -c "UPDATE app.setting SET value = '4'" -c "SELECT count(*) FROM app.setting_history" -c "ROLLBACK" 2>&1)
        [ "$out" = 3 ] && echo "    system versioning writes history: ok" || { echo "    FAIL: history rows after update: '$out' (expected 3)"; ok=0; }
    fi
    dropdb --if-exists "$dst" >>"$LOG" 2>&1; dropdb --if-exists "$ref" >>"$LOG" 2>&1; dropdb --if-exists "$src" >>"$LOG" 2>&1
    if [ $ok = 1 ]; then PASSED+=("$case"); else FAILED+=("$case"); fi
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
    new_db "$dst" || return 1
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
        split_*)
            # The README block, run line by line as written, in a scratch directory holding
            # backup.dump; mydb is this run's destination. Stops at the first failing line.
            run_split_block "$WORK/${case}_${variant}.split" "$dump" "$dst" >"$out" 2>&1 || rc=1 ;;
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
        elif grep -qE 'COPY failed for table .* violates check constraint' "$out"; then
            class="user data load order (CHECK constraint)"
        elif grep -q 'deadlock detected' "$out"; then
            class="deadlock"
            grep -qE '(health_checks|drop_protection|rename_following|sync_history_policies|__internal_history_policy_copies)' "$out" && class="deadlock in sql_saga event trigger"
        elif grep -qE 'PL/pgSQL function (health_checks|drop_protection|rename_following)\(' "$out"; then
            class="sql_saga event trigger"
        fi
        echo "    $variant: pg_restore FAILED [$class]: $(grep -m1 -E 'ERROR|error:' "$out" | sed -n '1p')"
        grep -m1 -A2 'ERROR' "$out" | sed 's/^/      /'
        dropdb --if-exists "$dst" >>"$LOG" 2>&1
        return 1
    fi
    local d t pol faults reads manual_state snapshots
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
    manual_state=$(manual_schema_state "$dst"); snapshots=$(snapshot_faults "$dst")
    local integrity
    integrity=$(psql -X -At -d "$dst" -c "SELECT sql_saga.check_integrity()::text = ''" 2>&1)
    # BASELINE_V11=1: a released 1.1 build has neither check_integrity() nor column snapshots.
    if [ "${BASELINE_V11:-0}" = 1 ]; then integrity=t; snapshots=0; fi
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
    if [ "$snapshots" != "0" ]; then
        echo "    $variant: SYSTEM_TIME column snapshots invalid ($snapshots)"
        return 1
    fi
    if [ "$manual_state" != "$src_manual_state" ]; then
        echo "    $variant: manual schema state differs (source $src_manual_state, restored $manual_state)"
        return 1
    fi
    if [ "$integrity" != "t" ]; then
        echo "    $variant: check_integrity failed ($integrity)"
        return 1
    fi
    echo "    $variant: ok"
    return 0
}

run_case() {
    local case=$1
    case "$case" in old_dump_*) run_old_dump_case "$case"; return ;; esac
    local src="${PREFIX}_${case}_src"
    local dump="$WORK/${case}.dump"
    local ok=1
    echo "case $case"
    new_db "$src" || { FAILED+=("$case (createdb)"); return; }
    if ! "build_$case" "$src" >"$WORK/${case}.build.log" 2>&1; then
        echo "    build FAILED: $(grep -m1 ERROR "$WORK/${case}.build.log")"
        FAILED+=("$case (build)")
        dropdb --if-exists "$src" >>"$LOG" 2>&1
        return
    fi
    pg_dump -Fc -d "$src" -f "$dump" || { FAILED+=("$case (pg_dump)"); return; }
    local src_digest src_digest_without_extension_functions src_trig src_pol src_manual_state
    src_reads=''
    src_digest=$(digest "$src"); src_digest_without_extension_functions=$(digest "$src" without_extension_functions); src_trig=$(event_triggers "$src"); src_pol=$(policies "$src"); src_reads=$(tenant_reads "$src")
    src_manual_state=$(manual_schema_state "$src")
    local src_faults src_iso src_snapshots
    src_iso=$(rls_isolation "$src")
    if [ -n "$src_iso" ] && [ "$src_iso" != "$RLS_EXPECTED" ]; then
        echo "    source: tenant isolation wrong (expected '$RLS_EXPECTED', got '$src_iso')"
        ok=0
    fi
    src_faults=$(policy_copy_faults "$src")
    src_snapshots=$(snapshot_faults "$src")
    local src_integrity
    src_integrity=$(psql -X -At -d "$src" -c "SELECT sql_saga.check_integrity()::text = ''" 2>&1)
    if [ "${BASELINE_V11:-0}" = 1 ]; then src_integrity=t; src_snapshots=0; fi
    if [ "$src_snapshots" != "0" ]; then
        echo "    source: SYSTEM_TIME column snapshots invalid ($src_snapshots)"
        ok=0
    fi
    if [ "$src_faults" != "0" ]; then
        echo "    source: $src_faults history policy copies missing, duplicated or orphaned"
        ok=0
    fi
    if [ "$src_integrity" != "t" ]; then
        echo "    source: check_integrity failed ($src_integrity)"
        ok=0
    fi
    dropdb --if-exists "$src" >>"$LOG" 2>&1
    local variants=(serial single_txn no_owner clean clean_single_txn clean_same clean_psql clean_psql_without_opt_in)
    # SPLIT_ONLY=1: only the README split restore (for example against a baseline build). select_cases
    # has already reduced the cases to SPLIT_CASES; never run the parallel branch.
    [ "${SPLIT_ONLY:-0}" = 1 ] && variants=()
    # parallel_known_failure: the catalog_only database restored with -j 4, 20 times. The ordering
    # race fails about 40% of runs on PG 18.3, so 20 attempts make an accidental pass unlikely.
    if [ "$case" = parallel_known_failure ] && [ "${SPLIT_ONLY:-0}" != 1 ]; then
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
    for variant in ${variants[@]+"${variants[@]}"}; do
        restore_variant "$case" "$variant" "$dump" "$src_digest" "$src_trig" || ok=0
    done
    if is_split_case "$case"; then
        local sfail=0 sknown=0 svout
        for i in $(seq 1 "$SPLIT_RUNS"); do
            if ! svout=$(restore_variant "$case" "split_$i" "$dump" "$src_digest" "$src_trig" 2>&1); then
                echo "$svout"
                # README: regular foreign keys to temporal tables need a serial or --single-transaction
                # restore; with -j their CHECK can run before the parent's data is loaded. Only that
                # documented failure is known; every other failure (deadlock included) is hard.
                if printf '%s\n' "$svout" | grep -q 'FAILED \[user data load order (CHECK constraint)\]' \
                   && [[ " ${SPLIT_HARD_CHECK_ORDER_CASES[*]} " != *" $case "* ]]; then
                    sknown=$((sknown+1))
                else
                    sfail=$((sfail+1))
                fi
            fi
        done
        echo "    split (README procedure): $((SPLIT_RUNS - sfail - sknown))/$SPLIT_RUNS ok, $sknown documented CHECK load-order failures, $sfail other failures"
        SPLIT_RESULTS+=("$case: $((SPLIT_RUNS - sfail - sknown))/$SPLIT_RUNS ok, $sknown known (regular-FK CHECK load order, README), $sfail other")
        [ $sknown = 0 ] || KNOWN+=("$case: $sknown/$SPLIT_RUNS split restores failed on the documented regular-FK CHECK load order (README: restore such databases serially)")
        [ $sfail = 0 ] || ok=0
    fi
    local ran=${#variants[@]}
    is_split_case "$case" && ran=$((ran + SPLIT_RUNS))
    # A case without a single restore is not a pass.
    if [ "$ran" = 0 ]; then SKIPPED+=("$case (no restore variant ran)"); return; fi
    if [ $ok = 1 ]; then PASSED+=("$case"); else FAILED+=("$case"); fi
}

is_split_case() { [[ " ${SPLIT_CASES[*]} " == *" $1 "* ]]; }

# select_cases [case ...]: CASES from the arguments (default ALL_CASES). With SPLIT_ONLY=1 only the
# SPLIT_CASES among them remain; the others go to SKIPPED (reported, never counted as passed).
select_cases() {
    local c
    CASES=()
    [ $# -eq 0 ] && set -- "${ALL_CASES[@]}"
    for c in "$@"; do
        if [ "${SPLIT_ONLY:-0}" = 1 ] && ! is_split_case "$c"; then
            SKIPPED+=("$c (SPLIT_ONLY=1: not in SPLIT_CASES)")
        else
            CASES+=("$c")
        fi
    done
}

# run_cases: run CASES and print the summary; the exit status is the run's verdict.
run_cases() {
    local c f r k s
    for c in ${CASES[@]+"${CASES[@]}"}; do
        run_case "$c"
    done
    echo
    echo "passed: ${#PASSED[@]}  failed: ${#FAILED[@]}  skipped: ${#SKIPPED[@]}"
    for f in ${FAILED[@]+"${FAILED[@]}"}; do echo "  FAILED: $f"; done
    for s in ${SKIPPED[@]+"${SKIPPED[@]}"}; do echo "  SKIPPED: $s"; done
    for r in ${SPLIT_RESULTS[@]+"${SPLIT_RESULTS[@]}"}; do echo "  SPLIT: $r"; done
    for k in ${KNOWN[@]+"${KNOWN[@]}"}; do echo "  KNOWN LIMITATION: $k"; done
    [ ${#FAILED[@]} -eq 0 ] && [ ${#PASSED[@]} -gt 0 ]
}

# self_test: shell-only checks of the harness's own control flow; no server, nothing outside a scratch
# directory. pg_restore, grep, createdb, dropdb, pg_dump and psql are mocks on PATH (grep delegates to
# the real grep except for the README's two list-filtering lines).
#   A. Each of the README's seven lines, made to fail in turn, fails the split variant, and no later
#      line runs; this is checked through restore_variant itself (the `cmd || rc=1` context inside
#      `if ! out=$(...)` that switched errexit off in the old eval). Without a failure all seven run,
#      with -d bound to the run's destination. A failing mkdir or cp fails the variant before any line.
#   B. SPLIT_ONLY=1 is split-only: the default selection builds and restores only SPLIT_CASES, runs no
#      other variant and no parallel restore, and reports the other cases as skipped, not passed; an
#      explicit parallel_known_failure runs nothing and is not a pass. Without SPLIT_ONLY the mocks do
#      see the normal and parallel variants (so B's absence checks can fail).
self_test() {
    local st fails=0 k expect got rc out
    st=$(mktemp -d "${TMPDIR:-/tmp}/saga_rt_selftest.XXXXXX") || return 1
    mkdir -p "$st/bin"
    REAL_GREP=$(command -v grep); export REAL_GREP
    cat >"$st/bin/mock" <<'MOCK'
#!/bin/bash
# Identify which README line (if any) this invocation is, log it, fail if it is MOCK_FAIL.
me=${0##*/} id='' dst=''
args=" $* "
prev=''
for a in "$@"; do [ "$prev" = -d ] && dst=$a; prev=$a; done
case "$me" in
    pg_restore)
        case "$args" in
            *' -l backup.dump '*) id=line1 ;;
            *' --section=pre-data '*) id=line4 ;;
            *' -L saga.list '*) id=line5 ;;
            *' -j 4 --section=data --section=post-data -L rest.list '*) id=line6 ;;
            *) id=pg_restore_other ;;
        esac ;;
    grep)
        case "$args" in
            *' -v TABLE DATA sql_saga  '*) id=line3 ;;
            *' TABLE DATA sql_saga  '*) id=line2 ;;
            *) exec "$REAL_GREP" "$@" ;;
        esac ;;
    psql)
        case "$args" in
            *' -c SELECT sql_saga.sync_all_history_policies() -c SELECT sql_saga.check_integrity() '*) id=line7 ;;
            *) case "$args" in *check_integrity*) echo t ;; esac
               exit 0 ;;
        esac ;;
    *) exit 0 ;;
esac
echo "$id${dst:+ dst=$dst}" >>"$MOCK_LOG"
[ "$id" = "${MOCK_FAIL:-}" ] && { echo "MOCK $id FAILED" >&2; exit 7; }
case "$id" in
    line1) printf '%s\n' '1; 0 0 TABLE DATA sql_saga era postgres' '2; 0 0 TABLE DATA public t postgres' ;;
    line2|line3) exec "$REAL_GREP" "$@" ;;
esac
exit 0
MOCK
    chmod +x "$st/bin/mock"
    for k in pg_restore grep createdb dropdb pg_dump psql; do ln -s mock "$st/bin/$k"; done

    st_check() { # st_check <description> <expected> <got>
        if [ "$2" = "$3" ]; then echo "  ok: $1"; else echo "  FAIL: $1"; echo "    expected: $2"; echo "    got:      $3"; fails=$((fails+1)); fi
    }

    echo "self-test A: README split block failure propagation (README_PATH=$README_PATH)"
    if [ "$(split_block | grep -c .)" != 7 ]; then echo "  FAIL: README block is not seven lines"; return 1; fi
    : >"$st/dump"
    for k in 0 1 2 3 4 5 6 7; do
        : >"$st/calls"
        rc=0
        # The harness's own calling context: if ! x=$(restore_variant ...) with `|| rc=1` inside.
        if ! out=$( ( export PATH="$st/bin:$PATH" MOCK_LOG="$st/calls" MOCK_FAIL="line$k"
                      WORK="$st/work$k" LOG="$st/log"; mkdir -p "$WORK"
                      restore_variant selftest split_1 "$st/dump" x x ) 2>&1); then
            rc=1
        fi
        got=$(sed 's/ dst=.*//' "$st/calls" | tr '\n' ' ')
        if [ $k = 0 ]; then
            expect="line1 line2 line3 line4 line5 line6 line7 "
            # Without a failure the mocks return nothing for the comparisons, so restore_variant fails
            # afterwards (ACL digest); here only the seven invocations and the binding matter.
            st_check "no failure: all seven lines run in order" "$expect" "$got"
            st_check "no failure: -d bound to the run's destination on lines 4-7" "4" "$(grep -c " dst=${PREFIX}_selftest_dst\$" "$st/calls")"
        else
            expect=$(seq 1 "$k" | sed 's/^/line/' | tr '\n' ' ')
            st_check "line $k fails: variant fails" "1" "$rc"
            st_check "line $k fails: lines 1..$k ran, none after" "$expect" "$got"
            st_check "line $k fails: reported as pg_restore FAILED" "1" "$(printf '%s\n' "$out" | grep -c 'split_1: pg_restore FAILED')"
        fi
    done
    # mkdir fails: the split directory's parent is a file. cp fails: the dump does not exist.
    : >"$st/calls"; : >"$st/notadir"
    rc=0; ( export PATH="$st/bin:$PATH" MOCK_LOG="$st/calls"; run_split_block "$st/notadir/x" "$st/dump" d ) >"$st/mkdir.out" 2>&1 || rc=1
    st_check "mkdir fails: variant fails" "1" "$rc"
    st_check "mkdir fails: no README line ran" "" "$(cat "$st/calls")"
    rc=0; ( export PATH="$st/bin:$PATH" MOCK_LOG="$st/calls"; run_split_block "$st/cpdir" "$st/missing.dump" d ) >"$st/cp.out" 2>&1 || rc=1
    st_check "cp fails: variant fails" "1" "$rc"
    st_check "cp fails: no README line ran" "" "$(cat "$st/calls")"

    echo "self-test B: SPLIT_ONLY selection"
    # sim <SPLIT_ONLY> [case ...]: select_cases + run_cases with every server touch mocked; restore_variant
    # only records the variant. Prints the run's summary; $st/built and $st/variants record what ran.
    sim() {
        local so=$1; shift
        : >"$st/built"; : >"$st/variants"
        ( export PATH="$st/bin:$PATH" MOCK_LOG="$st/calls"
          SPLIT_ONLY=$so; SPLIT_RUNS=3; WORK="$st/simwork"; LOG="$st/log"; mkdir -p "$WORK"
          new_db() { echo "$1" >>"$st/built"; }
          restore_variant() { echo "$1:$2" >>"$st/variants"; return 0; }
          digest() { :; }; event_triggers() { :; }; policies() { :; }; tenant_reads() { :; }
          manual_schema_state() { :; }; rls_isolation() { :; }
          policy_copy_faults() { echo 0; }; snapshot_faults() { echo 0; }
          select_cases "$@"
          run_cases; echo "exit=$?" )
    }
    local split_built split_variants
    split_built=$(for c in "${SPLIT_CASES[@]}"; do echo "${PREFIX}_${c}_src"; done | sort | tr '\n' ' ')
    split_variants=$(for c in "${SPLIT_CASES[@]}"; do for i in 1 2 3; do echo "$c:split_$i"; done; done | sort | tr '\n' ' ')
    out=$(sim 1 2>&1)
    st_check "default selection: only SPLIT_CASES built" "$split_built" "$(sed 's/ .*//' "$st/built" | sort | tr '\n' ' ')"
    st_check "default selection: only split variants of SPLIT_CASES ran" "$split_variants" "$(sort "$st/variants" | tr '\n' ' ')"
    st_check "default selection: summary" "passed: ${#SPLIT_CASES[@]}  failed: 0  skipped: $(( ${#ALL_CASES[@]} - ${#SPLIT_CASES[@]} ))" "$(printf '%s\n' "$out" | grep '^passed:')"
    st_check "default selection: parallel_known_failure skipped, not passed" "1" "$(printf '%s\n' "$out" | grep -c 'SKIPPED: parallel_known_failure (SPLIT_ONLY=1')"
    st_check "default selection: no parallel restore" "0" "$(grep -c parallel "$st/variants")"
    out=$(sim 1 parallel_known_failure 2>&1)
    st_check "explicit parallel_known_failure: nothing built" "" "$(cat "$st/built")"
    st_check "explicit parallel_known_failure: nothing restored" "" "$(cat "$st/variants")"
    st_check "explicit parallel_known_failure: summary" "passed: 0  failed: 0  skipped: 1" "$(printf '%s\n' "$out" | grep '^passed:')"
    st_check "explicit parallel_known_failure: not a successful run" "exit=1" "$(printf '%s\n' "$out" | grep '^exit=')"
    out=$(sim 1 parallel_known_failure catalog_only 2>&1)
    st_check "parallel_known_failure catalog_only: only catalog_only split runs" "catalog_only:split_1 catalog_only:split_2 catalog_only:split_3 " "$(sort "$st/variants" | tr '\n' ' ')"
    # run_case guards on its own too (a caller bypassing select_cases).
    out=$( ( export PATH="$st/bin:$PATH"; SPLIT_ONLY=1; SPLIT_RUNS=3; WORK="$st/simwork"; LOG="$st/log"
             new_db() { :; }; restore_variant() { echo "$1:$2" >>"$st/variants"; }
             digest() { :; }; event_triggers() { :; }; policies() { :; }; tenant_reads() { :; }
             manual_schema_state() { :; }; rls_isolation() { :; }; policy_copy_faults() { echo 0; }; snapshot_faults() { echo 0; }
             : >"$st/variants"; run_case parallel_known_failure >/dev/null
             echo "passed=${#PASSED[@]} skipped=${SKIPPED[*]-} variants=$(cat "$st/variants")" ) 2>&1)
    st_check "run_case parallel_known_failure under SPLIT_ONLY=1: no restore, skipped" "passed=0 skipped=parallel_known_failure (no restore variant ran) variants=" "$out"
    # Control: without SPLIT_ONLY the same mocks see the normal and the parallel variants.
    out=$(sim 0 catalog_only parallel_known_failure 2>&1)
    st_check "control without SPLIT_ONLY: 20 parallel restores" "20" "$(grep -c '^parallel_known_failure:parallel_' "$st/variants")"
    st_check "control without SPLIT_ONLY: serial variant runs" "1" "$(grep -c '^catalog_only:serial$' "$st/variants")"
    # A failing split run fails the case.
    : >"$st/variants"
    out=$( ( export PATH="$st/bin:$PATH"; SPLIT_ONLY=1; SPLIT_RUNS=3; WORK="$st/simwork"; LOG="$st/log"
             new_db() { :; }; restore_variant() { [ "$2" != split_2 ]; }
             digest() { :; }; event_triggers() { :; }; policies() { :; }; tenant_reads() { :; }
             manual_schema_state() { :; }; rls_isolation() { :; }; policy_copy_faults() { echo 0; }; snapshot_faults() { echo 0; }
             select_cases catalog_only; run_cases; echo "exit=$?" ) 2>&1)
    st_check "one failing split run: case fails, 2/3 reported" "  SPLIT: catalog_only: 2/3 ok, 0 known (regular-FK CHECK load order, README), 1 other|exit=1" "$(printf '%s\n' "$out" | grep -E '^  SPLIT:|^exit=' | tr '\n' '|' | sed 's/|$//')"

    rm -rf "$st"
    echo "self-test: $fails failure(s)"
    [ $fails = 0 ]
}

case "$SPLIT_RUNS" in
    ''|*[!0-9]*|0*) echo "SPLIT_RUNS must be a positive integer (got '$SPLIT_RUNS'); acceptance needs 20" >&2; exit 2 ;;
esac
[ "$SPLIT_RUNS" = 20 ] || echo "NOTE: SPLIT_RUNS=$SPLIT_RUNS: a reduced smoke run, not Step 5 acceptance (20 runs per case)"

if [ "${1:-}" = --self-test ]; then
    self_test
    exit $?
fi

select_cases "$@"
if [ ${#CASES[@]} -eq 0 ]; then
    echo "no case to run: ${SKIPPED[*]-}" >&2
    exit 2
fi

trap cleanup EXIT
cleanup
SPLIT_BLOCK=$(split_block)
if [ "$(printf '%s\n' "$SPLIT_BLOCK" | grep -c .)" != 7 ] || ! printf '%s\n' "$SPLIT_BLOCK" | grep -q -- '-j 4 --section=data --section=post-data -L rest.list -d mydb backup.dump$' \
   || ! printf '%s\n' "$SPLIT_BLOCK" | grep -q -- 'sql_saga.sync_all_history_policies()'; then
    echo "README.md split-restore block not found or not the documented seven lines (see README_PATH=$README_PATH)" >&2
    exit 2
fi
printf '%s\n' "$SPLIT_BLOCK" >"$WORK/split_block.sh"
psql_q -d postgres <<SQL
CREATE ROLE ${PREFIX}_r1;
CREATE ROLE ${PREFIX}_regular;
CREATE ROLE ${PREFIX}_admin IN ROLE ${PREFIX}_regular;
CREATE ROLE ${PREFIX}_none;
GRANT USAGE ON SCHEMA public TO PUBLIC;
SQL

run_cases; rc=$?
echo "logs: $WORK"
exit $rc
