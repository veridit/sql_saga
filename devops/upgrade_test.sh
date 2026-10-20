#!/usr/bin/env bash
#
# upgrade_test.sh - prove that databases created with each released sql_saga baseline reach the
# candidate design with one ALTER EXTENSION command. PostgreSQL selects the direct generated
# 1.0--1.2 script for the v1.0 path, while the v1.1 path uses 1.1--1.2.
# Design: tmp/design_drop_protection_v12_r4.md section 6 and the retained 1.1 upgrade assertions.
#
# What it does
#   1. Builds each selected released baseline from its tag, including its install SQL and old binaries.
#   2. Assembles a private extension directory containing that tag-built baseline plus candidate 1.2
#      files (control, fresh install SQL, and both update hops) from UPGRADE_CANDIDATE_DIR
#      (default: the installed extension dir, i.e. what `make install` deployed). Every scratch database
#      gets `ALTER DATABASE .. SET extension_control_path = '<private dir>:$system'`, so it sees
#      exactly these scripts. NOTHING is written into the global extension directory. Released
#      install scripts are built from their tags and compared byte-for-byte with frozen artifacts.
#   3. Builds old-state fixtures in saga_up_old while that database loads the selected release binaries
#      (per-database dynamic_library_path), then resets the path: the "restart with new binaries".
#   4. Runs ALTER EXTENSION sql_saga UPDATE TO '1.2', then asserts shape and behaviour,
#      fresh-1.2 versus upgraded equivalence, idempotence, atomic rollback, preflight acceptance and
#      refusal, query-function preservation, and dump/restore after the upgrade.
#
# Exit status: 0 all assertions pass (GREEN), 1 assertions failed (RED), 2 harness/setup error.
#
# Usage:   devops/upgrade_test.sh            or   make test-upgrade
# Environment:
#   libpq (PGHOST, PGPORT, PGUSER, ...)  needs a superuser (extension_control_path is superuser-only)
#   UPGRADE_BASELINE_TAG       default v1.0
#   UPGRADE_BASELINE_COMMIT    default 9e39fcc (the tag must point here)
#   UPGRADE_CANDIDATE_DIR      default $(pg_config --sharedir)/extension
#   UPGRADE_RELEASE_ARTIFACT_DIR default repository versions/; private override for probe candidates
#   UPGRADE_PREFLIGHT          default $UPGRADE_CANDIDATE_DIR/sql_saga_upgrade_preflight.sql
#   UPGRADE_OLD_BINARIES       default 1: build fixtures with the selected release binaries
#   UPGRADE_KEEP               default 0; 1 keeps this run's databases, roles and work dir
#   PG_CONFIG                  default pg_config
# Creates and drops only databases and roles bearing this invocation's unique saga_up_* prefix.
# Compatible with macOS /bin/bash 3.2 and set -u.
#
set -u

# md5 of stdin as a bare hex digest, on macOS (md5) and Linux (md5sum) alike. Without it, a missing
# command would turn every fingerprint into the same empty string and make comparisons meaningless.
if ! command -v md5 >/dev/null 2>&1; then
    md5() {
        if [ "${1:-}" = -q ]; then md5sum "$2" | cut -d' ' -f1; else md5sum | cut -d' ' -f1; fi
    }
fi
[ -n "$(printf x | md5)" ] || { echo "HARNESS ERROR: no working md5/md5sum" >&2; exit 2; }

REPO=$(cd "$(dirname "$0")/.." && pwd)
PG_CONFIG=${PG_CONFIG:-pg_config}
CANDIDATE_DIR=${UPGRADE_CANDIDATE_DIR:-$($PG_CONFIG --sharedir)/extension}
RELEASE_ARTIFACT_DIR=${UPGRADE_RELEASE_ARTIFACT_DIR:-$REPO/versions}
NEW_VERSION=1.2

# The public entry point first verifies the complete candidate artifact set, then runs the same
# assertion suite independently from each released baseline. Keeping each path in a child process
# gives it a unique database/role prefix and cleanup trap without requiring bash newer than 3.2.
if [ "${UPGRADE_PATH_RUNNER:-0}" != 1 ]; then
    ARTIFACT_WORK=$(mktemp -d "${TMPDIR:-/tmp}/saga_up_artifact.XXXXXX") || exit 2
    umask 022
    chmod 755 "$ARTIFACT_WORK"
    artifact_fails=0
    cleanup_artifacts() { rm -rf "$ARTIFACT_WORK"; }
    trap cleanup_artifacts EXIT

    echo "== candidate 1.2 artifacts"
    control_version=""
    if [ -f "$CANDIDATE_DIR/sql_saga.control" ]; then
        control_version=$(sed -n "s/^default_version[[:space:]]*=[[:space:]]*'\([^']*\)'.*/\1/p" "$CANDIDATE_DIR/sql_saga.control")
    fi
    if [ "$control_version" = "$NEW_VERSION" ]; then
        echo "    ok    candidate control default_version $NEW_VERSION"
    else
        echo "    FAIL [expected RED] candidate control default_version (expected '$NEW_VERSION', got '${control_version:-missing}')"
        artifact_fails=$((artifact_fails + 1))
    fi

    for spec in "v1.0|1.0|9e39fcc" "v1.1|1.1|9c72e66"; do
        tag=${spec%%|*}; rest=${spec#*|}; version=${rest%%|*}; commit=${rest#*|}
        tag_dir="$ARTIFACT_WORK/$version"
        mkdir -p "$tag_dir"
        tag_commit=$(git -C "$REPO" rev-parse --verify -q "$tag^{commit}") || {
            echo "HARNESS ERROR: tag $tag not found" >&2
            exit 2
        }
        case "$tag_commit" in
            "$commit"*) echo "    ok    $tag is released commit $commit" ;;
            *) echo "    FAIL [prerequisite] $tag is $tag_commit, not released commit $commit"; artifact_fails=$((artifact_fails + 1)) ;;
        esac
        git -C "$REPO" archive "$tag" | tar -x -C "$tag_dir" || exit 2
        make -C "$tag_dir" PG_CONFIG="$PG_CONFIG" "sql_saga--$version.sql" >"$ARTIFACT_WORK/$version.build.log" 2>&1 || {
            echo "HARNESS ERROR: building $tag install script (see $ARTIFACT_WORK/$version.build.log)" >&2
            exit 2
        }
        frozen="$RELEASE_ARTIFACT_DIR/sql_saga--$version.sql"
        if [ -f "$frozen" ] && cmp -s "$tag_dir/sql_saga--$version.sql" "$frozen"; then
            echo "    ok    versions/sql_saga--$version.sql is byte-identical to $tag"
        elif [ ! -f "$frozen" ]; then
            echo "    FAIL [expected RED] versions/sql_saga--$version.sql is missing"
            artifact_fails=$((artifact_fails + 1))
        else
            echo "    FAIL [prerequisite] versions/sql_saga--$version.sql differs from $tag"
            artifact_fails=$((artifact_fails + 1))
        fi
    done

    for artifact in "sql_saga--$NEW_VERSION.sql" "sql_saga--1.0--1.1.sql" "sql_saga--1.0--$NEW_VERSION.sql" "sql_saga--1.1--$NEW_VERSION.sql"; do
        if [ -f "$CANDIDATE_DIR/$artifact" ]; then
            echo "    ok    candidate ships $artifact"
        else
            echo "    FAIL [expected RED] candidate ships $artifact (missing in $CANDIDATE_DIR)"
            artifact_fails=$((artifact_fails + 1))
        fi
    done

    if [ "$artifact_fails" -ne 0 ]; then
        echo
        echo "RED. Candidate 1.2 artifact gate has $artifact_fails failure(s); upgrade fixtures were not created."
        exit 1
    fi

    overall=0
    for spec in "v1.0|1.0|9e39fcc" "v1.1|1.1|9c72e66"; do
        tag=${spec%%|*}; rest=${spec#*|}; version=${rest%%|*}; commit=${rest#*|}
        echo
        echo "################################################################################"
        echo "# upgrade path $tag ($version) -> $NEW_VERSION"
        echo "################################################################################"
        UPGRADE_PATH_RUNNER=1 UPGRADE_BASELINE_TAG="$tag" UPGRADE_BASELINE_VERSION="$version" \
            UPGRADE_BASELINE_COMMIT="$commit" "$0" || overall=1
    done
    exit "$overall"
fi

RUN_SUFFIX="${$}_${RANDOM}"
PREFIX="saga_up_${RUN_SUFFIX}"
BASELINE_TAG=${UPGRADE_BASELINE_TAG:-v1.0}
BASELINE_VERSION=${UPGRADE_BASELINE_VERSION:-1.0}
BASELINE_COMMIT=${UPGRADE_BASELINE_COMMIT:-9e39fcc}
PREFLIGHT=${UPGRADE_PREFLIGHT:-$CANDIDATE_DIR/sql_saga_upgrade_preflight.sql}
OLD_BINARIES=${UPGRADE_OLD_BINARIES:-1}
KEEP=${UPGRADE_KEEP:-0}

# The server process reads extension scripts and released libraries from the work tree; it may run as
# another OS user (postgres on Linux packages), so everything under it must be readable and searchable by
# others, whatever umask the caller has.
umask 022
WORK=$(mktemp -d "${TMPDIR:-/tmp}/saga_up.XXXXXX")
chmod 755 "$WORK"
LOG="$WORK/log"
# extension_control_path entries name a directory that CONTAINS extension/ (like $system = sharedir).
EXT_ROOT="$WORK/ext"                       # private extension dir: selected tag + candidate 1.2
EXT="$EXT_ROOT/extension"
FOREIGN_ROOT="$WORK/ext_foreign"           # a non-release build claiming the selected baseline
FOREIGN_EXT="$FOREIGN_ROOT/extension"
FAILS=()
PASSES=0
RED_REASON=""
UPDATE_ERROR=""
FAILURE_CLASS=prerequisite
CREATED_DBS=()
CREATED_ROLES=()
CLEANUP_FAILURES=()

# ---------------------------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------------------------
ok()   { PASSES=$((PASSES + 1)); echo "    ok    $*"; }
fail() { FAILS+=("$FAILURE_CLASS|$SECTION: $*"); echo "    FAIL [$FAILURE_CLASS] $*"; [ -n "$RED_REASON" ] || RED_REASON="$FAILURE_CLASS: $SECTION: $*"; }
section() { SECTION=$1; FAILURE_CLASS=${2:-prerequisite}; echo; echo "== $1"; }
die() { echo "HARNESS ERROR: $*" >&2; echo "logs: $WORK" >&2; exit 2; }
# check <description> <expected> <actual>
check() { if [ "$2" = "$3" ]; then ok "$1"; else fail "$1 (expected '$2', got '$3')"; fi; }

# ---------------------------------------------------------------------------------------------
# psql helpers
# ---------------------------------------------------------------------------------------------
# sql <db> [psql args]: script on stdin, unaligned, errors folded to "ERROR: <message>" lines.
sql() {
    local db=$1; shift
    psql -X -At -q -v ON_ERROR_STOP=1 -d "$db" "$@" 2>&1 | sed -e 's/^psql:[^ ]*:[0-9]*: //'
}
# q <db> <statement>: one statement, first error line only when it fails.
q() { printf '%s\n' "$2" | sql "$1" | sed -n '1,/ERROR/p' | grep -m1 -E '^ERROR|.' ; }
# as_role <db> <role> <statement>: result rows joined by ',', or "denied: <message>".
as_role() {
    local out
    out=$(printf 'SET ROLE %s;\n%s\n' "$2" "$3" | sql "$1")
    if printf '%s\n' "$out" | grep -q '^ERROR'; then
        echo "denied: $(printf '%s\n' "$out" | grep -m1 '^ERROR' | sed 's/^ERROR:  *//')"
    else
        printf '%s\n' "$out" | paste -sd, -
    fi
}
db_exists() { [ "$(psql -X -At -d postgres -c "SELECT count(*) FROM pg_database WHERE datname = '$1'")" = 1 ]; }

new_role() {  # new_role <name> [IN ROLE <name>]
    local role=$1 clause=${2:-}
    psql -X -q -v ON_ERROR_STOP=1 -d postgres -c "CREATE ROLE \"$role\" $clause" >>"$LOG" 2>&1 || die "creating role $role"
    CREATED_ROLES+=("$role")
}

new_db() {  # new_db <name> <extension_control_path root or empty for $system> [libdir for 1.0 binaries]
    local db=$1 ext=$2 lib=${3:-}
    createdb "$db" >>"$LOG" 2>&1 || die "createdb $db"
    CREATED_DBS+=("$db")
    if [ -n "$ext" ]; then
        psql -X -q -d postgres -c "ALTER DATABASE $db SET extension_control_path = '$ext:\$system'" >>"$LOG" 2>&1 || die "extension_control_path on $db"
    fi
    if [ -n "$lib" ]; then
        psql -X -q -d postgres -c "ALTER DATABASE $db SET dynamic_library_path = '$lib:\$libdir'" >>"$LOG" 2>&1 || die "dynamic_library_path on $db"
    fi
}

cleanup() {
    local saved_status=$? db role out
    if [ "$KEEP" = 1 ]; then
        echo "UPGRADE_KEEP=1: kept this run's databases: ${CREATED_DBS[*]:-(none)}"
        echo "UPGRADE_KEEP=1: kept this run's roles: ${CREATED_ROLES[*]:-(none)}"
        echo "UPGRADE_KEEP=1: kept work directory: $WORK"
        return $saved_status
    fi
    for db in "${CREATED_DBS[@]-}"; do
        [ -n "$db" ] || continue
        if ! out=$(psql -X -At -d postgres -v ON_ERROR_STOP=1 -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$db' AND pid <> pg_backend_pid()" 2>&1); then
            CLEANUP_FAILURES+=("terminate sessions for database $db: $out")
        fi
        if ! out=$(dropdb --if-exists "$db" 2>&1); then
            CLEANUP_FAILURES+=("drop database $db: $out")
        fi
    done
    for role in "${CREATED_ROLES[@]-}"; do
        [ -n "$role" ] || continue
        if ! out=$(psql -X -At -d postgres -v ON_ERROR_STOP=1 -c "DROP OWNED BY \"$role\"; DROP ROLE \"$role\"" 2>&1); then
            CLEANUP_FAILURES+=("drop role $role: $out")
        fi
    done
    if [ ${#CLEANUP_FAILURES[@]} -gt 0 ]; then
        echo "cleanup left scratch objects:" >&2
        printf '    %s\n' "${CLEANUP_FAILURES[@]}" >&2
        saved_status=2
    else
        echo "cleanup complete for run prefix $PREFIX"
    fi
    rm -rf "$WORK" || { echo "cleanup failed to remove work directory $WORK" >&2; saved_status=2; }
    return $saved_status
}

# ---------------------------------------------------------------------------------------------
# Fixtures (section 5.3). Core fixtures run identically on the old and the fresh database; the
# old-only fixtures need 1.0 (cache API) or 1.0 behaviour (derived grants it could not see).
# ---------------------------------------------------------------------------------------------
fixture_core() {  # $1 = extension version to CREATE
    cat <<SQL
\set VERBOSITY terse
SET client_min_messages = warning;
SET datestyle = 'ISO, YMD';
-- (8) hardened defaults: no PUBLIC EXECUTE on new functions, set before anything is created
ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE ${PREFIX}_owner REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
GRANT CREATE ON SCHEMA public TO ${PREFIX}_owner;
CREATE EXTENSION btree_gist;
CREATE EXTENSION sql_saga VERSION '$1';

-- (1) FOR PORTION OF + current view, no RLS, named-role grants that 1.0 mirrors onto the views.
-- The temporal PRIMARY KEY with a range-only era is the P2 current-view identity case.
CREATE TABLE fp (id int, valid_range daterange NOT NULL, x text, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('fp', 'valid_range', 'valid');
SELECT sql_saga.add_for_portion_of_view('fp', 'valid');
SELECT sql_saga.add_current_view('fp', 'valid');
INSERT INTO fp VALUES (1, '[2024-01-01,infinity)', 'a'), (2, '[2024-01-01,infinity)', 'b');
GRANT SELECT, INSERT, UPDATE, DELETE ON fp TO ${PREFIX}_r1;
GRANT SELECT ON fp TO ${PREFIX}_regular;

-- (1)(7) system versioning, grants mirrored by 1.0, inherited membership (admin IN ROLE regular)
CREATE TABLE sv (id int PRIMARY KEY, x text);
SELECT sql_saga.add_system_versioning('sv');
INSERT INTO sv VALUES (1, 'v1'), (2, 'v1');
UPDATE sv SET x = 'v2';

-- (2)(7) RLS base owned by a non-superuser: permissive SELECT, permissive ALL, restrictive SELECT
-- with a role array, and an INSERT-only policy that must not be copied.
SET ROLE ${PREFIX}_owner;
CREATE TABLE rdoc (id int PRIMARY KEY, tenant text NOT NULL, body text);
RESET ROLE;
SELECT sql_saga.add_system_versioning('rdoc');
INSERT INTO rdoc VALUES (1, 'a', 'v1'), (2, 'b', 'v1');
UPDATE rdoc SET body = 'v2';
ALTER TABLE rdoc ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_a ON rdoc FOR SELECT TO ${PREFIX}_r1 USING (tenant = 'a');
CREATE POLICY tenant_b ON rdoc FOR ALL TO ${PREFIX}_regular USING (tenant = 'b');
CREATE POLICY has_body ON rdoc AS RESTRICTIVE FOR SELECT TO ${PREFIX}_r1, ${PREFIX}_regular USING (body IS NOT NULL);
CREATE POLICY ins_only ON rdoc FOR INSERT TO ${PREFIX}_r1 WITH CHECK (true);

-- (3)(4) FORCE RLS; base and pre-created history share a dropped-column attnum hole (same layout,
-- so the 1.0 C library is not exercised on mismatched layouts). The history-only column is added
-- after the 1.0 writes (the 1.0 writer maps positionally and refuses it); after the upgrade the
-- by-name writer must accept it (behaviour section).
SET ROLE ${PREFIX}_owner;
CREATE TABLE fdoc (id int PRIMARY KEY, junk int, tenant text NOT NULL, body text);
ALTER TABLE fdoc DROP COLUMN junk;
CREATE TABLE fdoc_history (id int NOT NULL, junk int, tenant text NOT NULL, body text, system_valid_range tstzrange NOT NULL);
ALTER TABLE fdoc_history DROP COLUMN junk;
RESET ROLE;
SELECT sql_saga.add_system_versioning('fdoc');
INSERT INTO fdoc (id, tenant, body) VALUES (1, 'a', 'v1'), (2, 'b', 'v1');
UPDATE fdoc SET body = 'v2';
ALTER TABLE fdoc_history ADD COLUMN archived_note text;
ALTER TABLE fdoc ENABLE ROW LEVEL SECURITY;
ALTER TABLE fdoc FORCE ROW LEVEL SECURITY;
CREATE POLICY only_a ON fdoc FOR ALL TO PUBLIC USING (tenant = 'a');

-- (5) custom history/view/function names sorting before the base
CREATE TABLE zbase (id int PRIMARY KEY, x text);
SELECT sql_saga.add_system_versioning('zbase', history_table_name => 'aa_hist', view_name => 'ab_view',
    function_as_of_name => 'zb_at', function_between_name => 'zb_between',
    function_between_symmetric_name => 'zb_between_sym', function_from_to_name => 'zb_from_to');
INSERT INTO zbase VALUES (1, 'v1');

-- (11) foreign keys: temporal-to-temporal and regular-to-temporal (C triggers, helper function)
CREATE TABLE par (id int, valid_range daterange NOT NULL, valid_from date, valid_until date, name text);
SELECT sql_saga.add_era('par', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('par', ARRAY['id']::name[], 'valid', key_type => 'natural');
CREATE TABLE pred (id int, category text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('pred', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('pred', ARRAY['id']::name[], 'valid', key_type => 'predicated', predicate => 'category IS NOT NULL', unique_key_name => 'pred_id_valid');
CREATE TABLE pkey (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('pkey', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('pkey', ARRAY['id']::name[], 'valid', key_type => 'primary', unique_key_name => 'pkey_id_valid');
SELECT sql_saga.add_for_portion_of_view('par', 'valid');
CREATE TABLE tchild (id int, par_id int, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('tchild', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
CREATE TABLE rchild (id int PRIMARY KEY, par_id int);
INSERT INTO par (id, valid_from, valid_until, name) VALUES (1, '2024-01-01', 'infinity', 'p1'), (2, '2024-01-01', '2025-01-01', 'p2');
SELECT sql_saga.add_foreign_key('tchild', ARRAY['par_id']::name[], 'par', ARRAY['id']::name[]);
SELECT sql_saga.add_foreign_key('rchild', ARRAY['par_id']::name[], 'par', ARRAY['id']::name[]);
INSERT INTO tchild (id, par_id, valid_from, valid_until) VALUES (1, 1, '2024-06-01', 'infinity');
INSERT INTO rchild VALUES (1, 2);
GRANT SELECT, INSERT, UPDATE, DELETE ON par, tchild, rchild TO ${PREFIX}_r1;
GRANT INSERT ON rchild TO ${PREFIX}_blind;

-- Grants on system-versioned tables, all in ONE statement and after every other GRANT: 1.0's
-- health_checks refuses any GRANT once a history table has an explicit ACL (it counts the owner's
-- own DELETE there as a grant to a read-only object), so this is the only shape a 1.0 database
-- with several granted families can have. Same statement on the fresh 1.1 side.
GRANT SELECT ON sv, rdoc, fdoc, zbase TO ${PREFIX}_r1, ${PREFIX}_regular;

-- (12) managed table inheritance, after every GRANT (1.0 refuses later GRANTs, see above). A
-- system-versioned parent with an independently system-versioned child (the child inherits the
-- parent's system_valid_range column), and an era parent with an era child carrying its own natural
-- key and FOR PORTION OF view. Writes through the parent reach the child's family as well.
CREATE TABLE ip (id int PRIMARY KEY, x text);
CREATE TABLE ic (y text) INHERITS (ip);
ALTER TABLE ic ADD PRIMARY KEY (id);
SELECT sql_saga.add_system_versioning('ip');
SELECT sql_saga.add_system_versioning('ic');
INSERT INTO ip VALUES (1, 'a');
INSERT INTO ic VALUES (2, 'b', 'c');
UPDATE ip SET x = x || '2';
CREATE TABLE ep (id int, valid_range daterange NOT NULL, valid_from date, valid_until date, x text);
CREATE TABLE ec (y text) INHERITS (ep);
SELECT sql_saga.add_era('ep', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era('ec', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('ec', ARRAY['id']::name[], 'valid', key_type => 'natural');
SELECT sql_saga.add_for_portion_of_view('ec', 'valid');
INSERT INTO ec (id, valid_from, valid_until, x, y) VALUES (1, '2024-01-01', 'infinity', 'a', 'b');

-- temporal_merge on the range-only era (populates the 1.0 shared cache)
CREATE TEMP TABLE src_fp (row_id int, id int, valid_range daterange, x text);
INSERT INTO src_fp VALUES (1, 3, '[2024-06-01,infinity)', 'c');
CALL sql_saga.temporal_merge(target_table => 'fp', source_table => 'src_fp',
    primary_identity_columns => '{id}', mode => 'MERGE_ENTITY_UPSERT', era_name => 'valid');
SQL
}

fixture_old_only() {
    cat <<SQL
\set VERBOSITY terse
SET client_min_messages = warning;
-- (6) administrator-added direct grants on derived objects. 1.0 refused these while its event
-- triggers ran, so they exist only where they were added with event triggers off.
SET session_replication_role = replica;
GRANT SELECT ON sv_with_history TO ${PREFIX}_none;
GRANT SELECT ON fp__for_portion_of_valid TO ${PREFIX}_none;
SELECT format('GRANT EXECUTE ON FUNCTION %s TO ${PREFIX}_none', sv.func_as_of)
FROM sql_saga.system_versioning AS sv WHERE sv.table_name = 'sv' \gexec
RESET session_replication_role;

-- (9) every old cache API, leaving rows in the shared cache
SELECT sql_saga.temporal_merge_source_columns_hash('fp', '{id}', '{valid_range}', 'row_id') IS NOT NULL;
CALL sql_saga.temporal_merge_cache_store('upgrade-test-key', 'h', '[]'::jsonb);
SELECT sql_saga.temporal_merge_cache_lookup('upgrade-test-key', 'h') IS NOT NULL;
SELECT sql_saga.temporal_merge_cache_maybe_purge(1000, 0, 30) >= 0;
CALL sql_saga.temporal_merge_invalidate_cache('sv');
SELECT total_entries > 0 FROM sql_saga.temporal_merge_cache_stats();
SQL
}

# (10) a user policy on a history table: preflight must stop, the update must fail atomically.
fixture_histpol() {
    cat <<SQL
\set VERBOSITY terse
SET client_min_messages = warning;
CREATE EXTENSION btree_gist;
CREATE EXTENSION sql_saga VERSION '$BASELINE_VERSION';
CREATE TABLE hp (id int PRIMARY KEY, x text);
SELECT sql_saga.add_system_versioning('hp');
INSERT INTO hp VALUES (1, 'v1');
UPDATE hp SET x = 'v2';
GRANT SELECT ON hp TO ${PREFIX}_r1;
ALTER TABLE hp_history ENABLE ROW LEVEL SECURITY;
SET session_replication_role = replica;
CREATE POLICY user_history_policy ON hp_history FOR SELECT USING (true);
RESET session_replication_role;
SQL
}

# ---------------------------------------------------------------------------------------------
# Observations
# ---------------------------------------------------------------------------------------------
# Effective ACL of every relation and function outside system schemas, owners excluded.
acl_digest() {
    sql "$1" <<'SQL'
WITH objs AS (
    SELECT format('rel %s', c.oid::regclass) AS obj, c.relowner AS owner,
           COALESCE(c.relacl, acldefault(CASE WHEN c.relkind = 'S' THEN 's' ELSE 'r' END::"char", c.relowner)) AS acl
    FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND c.relkind IN ('r', 'v', 'm', 'S', 'p')
    UNION ALL
    SELECT format('fn %s', p.oid::regprocedure), p.proowner, COALESCE(p.proacl, acldefault('f', p.proowner))
    FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
), entries AS (
    SELECT o.obj, CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END AS grantee, a.privilege_type
    FROM objs AS o CROSS JOIN LATERAL aclexplode(o.acl) AS a
    WHERE a.grantee <> o.owner
)
SELECT md5(COALESCE(string_agg(format('%s|%s|%s', obj, grantee, privilege_type), E'\n' ORDER BY obj, grantee, privilege_type), ''))
FROM entries;
SQL
}

# Extension-level semantic snapshot (pg_dump omits extension members): routines with body digest,
# relations with columns/ACL/options, enum labels, event triggers, configuration tables, members.
ext_snapshot() {
    sql "$1" <<'SQL'
WITH ns AS (SELECT oid FROM pg_namespace WHERE nspname = 'sql_saga'),
acl AS (
    SELECT 'fn' AS k, p.oid, (SELECT string_agg(CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END || ':' || a.privilege_type, ',' ORDER BY 1)
                              FROM aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) AS a) AS acl
    FROM pg_proc AS p WHERE p.pronamespace = (SELECT oid FROM ns)
    UNION ALL
    SELECT 'rel', c.oid, (SELECT string_agg(CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END || ':' || a.privilege_type, ',' ORDER BY 1)
                          FROM aclexplode(COALESCE(c.relacl, acldefault(CASE WHEN c.relkind = 'S' THEN 's' ELSE 'r' END::"char", c.relowner))) AS a)
    FROM pg_class AS c WHERE c.relnamespace = (SELECT oid FROM ns)
)
SELECT line FROM (
    SELECT format('routine %s kind=%s ret=%s secdef=%s vol=%s config=%s owner=%s acl=%s body=%s', p.oid::regprocedure, p.prokind,
                  pg_get_function_result(p.oid), p.prosecdef, p.provolatile, p.proconfig, p.proowner::regrole,
                  a.acl, md5(COALESCE(p.prosrc, '') || COALESCE(p.probin, ''))) AS line
    FROM pg_proc AS p JOIN acl AS a ON a.k = 'fn' AND a.oid = p.oid
    WHERE p.pronamespace = (SELECT oid FROM ns)
    UNION ALL
    SELECT format('relation %s kind=%s options=%s owner=%s acl=%s cols=%s', c.oid::regclass, c.relkind, c.reloptions, c.relowner::regrole, a.acl,
                  (SELECT string_agg(format('%s:%s:%s', at.attname, format_type(at.atttypid, at.atttypmod), at.attnotnull), ',' ORDER BY at.attnum)
                   FROM pg_attribute AS at WHERE at.attrelid = c.oid AND at.attnum > 0 AND NOT at.attisdropped))
    FROM pg_class AS c JOIN acl AS a ON a.k = 'rel' AND a.oid = c.oid
    WHERE c.relnamespace = (SELECT oid FROM ns) AND c.relkind <> 'i'
    UNION ALL
    SELECT format('view %s %s', c.oid::regclass, md5(pg_get_viewdef(c.oid)))
    FROM pg_class AS c WHERE c.relnamespace = (SELECT oid FROM ns) AND c.relkind = 'v'
    UNION ALL
    SELECT format('constraint %s.%s %s', co.conrelid::regclass, co.conname, pg_get_constraintdef(co.oid))
    FROM pg_constraint AS co WHERE co.connamespace = (SELECT oid FROM ns) AND co.conrelid <> 0
    UNION ALL
    SELECT format('index %s', pg_get_indexdef(i.indexrelid))
    FROM pg_index AS i JOIN pg_class AS c ON c.oid = i.indrelid WHERE c.relnamespace = (SELECT oid FROM ns)
    UNION ALL
    SELECT format('type %s labels=%s', t.oid::regtype, (SELECT string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder) FROM pg_enum AS e WHERE e.enumtypid = t.oid))
    FROM pg_type AS t WHERE t.typnamespace = (SELECT oid FROM ns) AND t.typtype IN ('e', 'c', 'd', 'r') AND t.typrelid = 0
    UNION ALL
    SELECT format('trigger %s.%s %s enabled=%s', t.tgrelid::regclass, t.tgname, pg_get_triggerdef(t.oid), t.tgenabled)
    FROM pg_trigger AS t JOIN pg_class AS c ON c.oid = t.tgrelid WHERE c.relnamespace = (SELECT oid FROM ns) AND NOT t.tgisinternal
    UNION ALL
    SELECT format('routine_def %s %s', p.oid::regprocedure, md5(pg_get_functiondef(p.oid)))
    FROM pg_proc AS p WHERE p.pronamespace = (SELECT oid FROM ns) AND p.prokind IN ('f', 'p')
    UNION ALL
    SELECT format('event_trigger %s on=%s fn=%s enabled=%s tags=%s', e.evtname, e.evtevent, e.evtfoid::regproc, e.evtenabled, e.evttags)
    FROM pg_event_trigger AS e
    UNION ALL
    SELECT format('extension %s config=%s', x.extname, (SELECT array_agg(c::regclass::text ORDER BY c::regclass::text) FROM unnest(x.extconfig) AS c))
    FROM pg_extension AS x WHERE x.extname = 'sql_saga'
    UNION ALL
    SELECT format('member %s', pg_describe_object(d.classid, d.objid, d.objsubid))
    FROM pg_depend AS d WHERE d.refclassid = 'pg_extension'::regclass AND d.deptype = 'e'
      AND d.refobjid = (SELECT oid FROM pg_extension WHERE extname = 'sql_saga')
) AS s ORDER BY line;
SQL
}

# Exhaustive semantic state compared between fresh and upgraded installations. Normalization is
# deliberately limited to nondeterministic installation-token material: the token value becomes
# <token>, fixed policy suffixes become <token>, and copied-policy hashes become copy-of-<base-name>.
# No timestamps are normalized because this is schema/catalog state, not volatile history data.
# OID references are rendered as qualified reg* names so object identity is preserved across DBs.
# Aggregates have no pg_get_functiondef: their routine line marks them, and a separate aggregate line
# carries every pg_aggregate field. Initial privileges of current members are compared in full.
semantic_sql() {
    cat <<'SQL'
WITH token AS (
    SELECT CASE WHEN to_regclass('sql_saga.installation') IS NULL THEN NULL
                ELSE (xpath('//row/token/text()', query_to_xml('SELECT token FROM sql_saga.installation', false, false, '')))[1]::text END AS value
), families AS (
    SELECT format('%I.%I', sv.table_schema, sv.table_name)::regclass AS base,
           format('%I.%I', sv.history_schema_name, sv.history_table_name)::regclass AS hist
    FROM sql_saga.system_versioning AS sv
), policy_names AS (
    SELECT hp.oid, 'copy-of-' || bp.polname AS normalized
    FROM families AS f JOIN pg_policy AS hp ON hp.polrelid = f.hist
    JOIN pg_policy AS bp ON bp.polrelid = f.base
    CROSS JOIN token AS t
    WHERE hp.polname = 'sql_saga_' || left(md5(bp.polname || t.value), 16)
), lines AS (
    SELECT format('relation|%I.%I|kind=%s|persistence=%s|replica_identity=%s|access_method=%s|options=%s|rls=%s|force=%s',
                  n.nspname, c.relname, c.relkind, c.relpersistence, c.relreplident,
                  coalesce(am.amname, ''), coalesce(c.reloptions::text, ''), c.relrowsecurity, c.relforcerowsecurity) AS line
    FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace
    LEFT JOIN pg_am AS am ON am.oid = c.relam
    WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast') AND n.nspname !~ '^pg_(toast_)?temp' AND c.relkind <> 'i'
    UNION ALL
    SELECT format('column|%I.%I|%s|type=%s|notnull=%s|default=%s|generated=%s|identity=%s|collation=%s',
                  n.nspname, c.relname, a.attname, format_type(a.atttypid,a.atttypmod), a.attnotnull,
                  coalesce(pg_get_expr(d.adbin,d.adrelid),''), a.attgenerated, a.attidentity,
                  CASE WHEN a.attcollation = 0 THEN '' ELSE a.attcollation::regcollation::text END)
    FROM pg_attribute AS a JOIN pg_class AS c ON c.oid=a.attrelid JOIN pg_namespace AS n ON n.oid=c.relnamespace
    LEFT JOIN pg_attrdef AS d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
    WHERE a.attnum>0 AND NOT a.attisdropped AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast') AND n.nspname !~ '^pg_(toast_)?temp'
    UNION ALL
    SELECT format('policy|%I.%I|name=%s|cmd=%s|permissive=%s|roles=%s|qual=%s|with_check=%s',
                  n.nspname,c.relname,coalesce(pn.normalized,regexp_replace(p.polname,'^(sql_saga_history_(reader|base_select))_[0-9a-z]+$','\1_<token>')),
                  p.polcmd,p.polpermissive,(SELECT string_agg(CASE WHEN r=0 THEN 'PUBLIC' ELSE r::regrole::text END,',' ORDER BY CASE WHEN r=0 THEN 'PUBLIC' ELSE r::regrole::text END) FROM unnest(p.polroles) AS r),
                  coalesce(pg_get_expr(p.polqual,p.polrelid),''),coalesce(pg_get_expr(p.polwithcheck,p.polrelid),''))
    FROM pg_policy AS p JOIN pg_class AS c ON c.oid=p.polrelid JOIN pg_namespace AS n ON n.oid=c.relnamespace
    LEFT JOIN policy_names AS pn ON pn.oid=p.oid
    WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast') AND n.nspname !~ '^pg_(toast_)?temp'
    UNION ALL
    SELECT format('routine|%s|kind=%s|secdef=%s|volatile=%s|parallel=%s|leakproof=%s|config=%s|return=%s|args=%s|owner=%s|acl=%s|definition=%s',
                  p.oid::regprocedure,p.prokind,p.prosecdef,p.provolatile,p.proparallel,p.proleakproof,coalesce(p.proconfig::text,''),
                  p.prorettype::regtype,pg_get_function_arguments(p.oid),p.proowner::regrole,
                  coalesce((SELECT string_agg(CASE WHEN x.grantee=0 THEN 'PUBLIC' ELSE x.grantee::regrole::text END||':'||x.privilege_type,',' ORDER BY 1) FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) AS x),''),
                  CASE WHEN p.prokind IN ('f','p','w') THEN md5(pg_get_functiondef(p.oid)) ELSE '<aggregate: see aggregate line>' END)
    FROM pg_proc AS p WHERE p.pronamespace='sql_saga'::regnamespace
    UNION ALL
    SELECT format('type|%s|kind=%s|enum=%s|domain_base=%s|domain_default=%s|domain_notnull=%s|composite=%s',t.oid::regtype,t.typtype,
                  coalesce((SELECT string_agg(e.enumlabel,',' ORDER BY e.enumsortorder) FROM pg_enum AS e WHERE e.enumtypid=t.oid),''),
                  CASE WHEN t.typtype='d' THEN t.typbasetype::regtype::text ELSE '' END,coalesce(t.typdefault,''),t.typnotnull,
                  coalesce((SELECT string_agg(a.attname||':'||format_type(a.atttypid,a.atttypmod),',' ORDER BY a.attnum) FROM pg_attribute AS a WHERE a.attrelid=t.typrelid AND a.attnum>0 AND NOT a.attisdropped),''))
    FROM pg_type AS t WHERE t.typnamespace='sql_saga'::regnamespace AND t.typtype IN ('e','d','c')
    UNION ALL
    SELECT format('comment|%s|%s',pg_describe_object(d.classoid,d.objoid,d.objsubid),d.description)
    FROM pg_description AS d WHERE d.classoid IN ('pg_class'::regclass,'pg_proc'::regclass,'pg_type'::regclass,'pg_namespace'::regclass,'pg_constraint'::regclass,'pg_trigger'::regclass,'pg_policy'::regclass)
      AND pg_describe_object(d.classoid,d.objoid,d.objsubid) !~ 'pg_catalog|information_schema|pg_toast'
    UNION ALL
    SELECT format('security_label|%s|%s|%s',pg_describe_object(s.classoid,s.objoid,s.objsubid),s.provider,s.label) FROM pg_seclabel AS s
    UNION ALL
    SELECT 'catalog-era|' || ((to_jsonb(e)-'range_type'-'multirange_type'-'range_subtype') || jsonb_build_object('range_type',e.range_type::text,'multirange_type',e.multirange_type::text,'range_subtype',e.range_subtype::text))::text FROM sql_saga.era AS e
    UNION ALL SELECT 'catalog-unique_keys|'||to_jsonb(u)::text FROM sql_saga.unique_keys AS u
    UNION ALL SELECT 'catalog-foreign_keys|'||to_jsonb(f)::text FROM sql_saga.foreign_keys AS f
    UNION ALL SELECT 'catalog-updatable_view|'||to_jsonb(v)::text FROM sql_saga.updatable_view AS v
    UNION ALL SELECT 'catalog-system_versioning|'||to_jsonb(s)::text FROM sql_saga.system_versioning AS s
    UNION ALL SELECT 'catalog-system_time_era|'||to_jsonb(s)::text FROM sql_saga.system_time_era AS s
    UNION ALL
    SELECT format('aggregate|%s|kind=%s|transfn=%s|finalfn=%s|combinefn=%s|serialfn=%s|deserialfn=%s|mtransfn=%s|minvtransfn=%s|mfinalfn=%s|finalextra=%s|mfinalextra=%s|finalmodify=%s|mfinalmodify=%s|sortop=%s|transtype=%s|transspace=%s|mtranstype=%s|mtransspace=%s|initval=%s|minitval=%s|numdirect=%s|parallel=%s|strict=%s|volatile=%s|owner=%s|acl=%s',
                  p.oid::regprocedure, ag.aggkind, ag.aggtransfn::regprocedure,
                  CASE WHEN ag.aggfinalfn = 0 THEN '' ELSE ag.aggfinalfn::regprocedure::text END,
                  CASE WHEN ag.aggcombinefn = 0 THEN '' ELSE ag.aggcombinefn::regprocedure::text END,
                  CASE WHEN ag.aggserialfn = 0 THEN '' ELSE ag.aggserialfn::regprocedure::text END,
                  CASE WHEN ag.aggdeserialfn = 0 THEN '' ELSE ag.aggdeserialfn::regprocedure::text END,
                  CASE WHEN ag.aggmtransfn = 0 THEN '' ELSE ag.aggmtransfn::regprocedure::text END,
                  CASE WHEN ag.aggminvtransfn = 0 THEN '' ELSE ag.aggminvtransfn::regprocedure::text END,
                  CASE WHEN ag.aggmfinalfn = 0 THEN '' ELSE ag.aggmfinalfn::regprocedure::text END,
                  ag.aggfinalextra, ag.aggmfinalextra, ag.aggfinalmodify, ag.aggmfinalmodify,
                  CASE WHEN ag.aggsortop = 0 THEN '' ELSE ag.aggsortop::regoperator::text END,
                  ag.aggtranstype::regtype, ag.aggtransspace,
                  CASE WHEN ag.aggmtranstype = 0 THEN '' ELSE ag.aggmtranstype::regtype::text END, ag.aggmtransspace,
                  coalesce(ag.agginitval, '<null>'), coalesce(ag.aggminitval, '<null>'), ag.aggnumdirectargs,
                  p.proparallel, p.proisstrict, p.provolatile, p.proowner::regrole,
                  coalesce((SELECT string_agg(CASE WHEN x.grantee=0 THEN 'PUBLIC' ELSE x.grantee::regrole::text END||':'||x.privilege_type,',' ORDER BY 1) FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) AS x),''))
    FROM pg_proc AS p JOIN pg_aggregate AS ag ON ag.aggfnoid = p.oid
    WHERE p.pronamespace='sql_saga'::regnamespace
    UNION ALL
    -- Initial privileges of every current sql_saga member, fresh and upgraded alike.
    SELECT format('initpriv|%s|privtype=%s|acl=%s', pg_describe_object(i.classoid, i.objoid, i.objsubid), i.privtype,
                  coalesce((SELECT string_agg(CASE WHEN x.grantee=0 THEN 'PUBLIC' ELSE x.grantee::regrole::text END||':'||x.privilege_type||CASE WHEN x.is_grantable THEN '*' ELSE '' END||'/'||CASE WHEN x.grantor=0 THEN 'PUBLIC' ELSE x.grantor::regrole::text END,',' ORDER BY 1) FROM aclexplode(i.initprivs) AS x),''))
    FROM pg_init_privs AS i
    WHERE EXISTS (SELECT FROM pg_depend AS d JOIN pg_extension AS e ON e.oid = d.refobjid
                  WHERE e.extname = 'sql_saga' AND d.deptype = 'e'
                    AND (d.classid, d.objid) = (i.classoid, i.objoid))
)
-- One record per output line: embedded newlines (multi-line comments, expressions) are escaped.
SELECT replace(replace(replace(line,(SELECT value FROM token),'<token>'), E'\\', E'\\\\'), E'\n', E'\\n') FROM lines ORDER BY 1;
SQL
}
semantic_snapshot() { semantic_sql | sql "$1"; }

# Minimum line count per semantic_snapshot section for the core fixture, fresh or upgraded. Catalog
# sections are exact fixture counts (13 eras: 6 system_time + fp, par, pred, pkey, tchild, ep, ec;
# 6 system-versioned families; 4 unique keys; 2 foreign keys; 4 updatable views). Object sections are
# floors below the observed counts, so routine growth does not break them, while an erroring, empty
# or truncated snapshot (the ERROR-only false positive of the step 6 audit) fails at once. Every
# section the snapshot emits must be listed; an unlisted section or a continuation line fails too.
SEMANTIC_COVERAGE="relation:45 column:250 policy:20 routine:100 aggregate:2 type:20 comment:1000 catalog-era:13 catalog-unique_keys:4 catalog-foreign_keys:2 catalog-updatable_view:4 catalog-system_versioning:6 catalog-system_time_era:6 initpriv:40"
# semantic_coverage_faults <file>: one line per missing/thin section or foreign line; empty = covered.
semantic_coverage_faults() {
    local file=$1 spec sec min n
    if [ ! -s "$file" ]; then echo "snapshot is empty"; return; fi
    if grep -q -E '^(ERROR|FATAL|WARNING|psql:)' "$file"; then
        echo "snapshot contains diagnostics: $(grep -m1 -E '^(ERROR|FATAL|WARNING|psql:)' "$file")"
    fi
    for spec in $SEMANTIC_COVERAGE; do
        sec=${spec%%:*}; min=${spec#*:}
        n=$(grep -c "^$sec|" "$file" || true)
        [ "$n" -ge "$min" ] || echo "section $sec has $n lines, expected at least $min"
    done
    cut -d'|' -f1 "$file" | LC_ALL=C sort -u | while IFS= read -r sec; do
        case " $SEMANTIC_COVERAGE " in *" $sec:"*) ;; *) echo "unexpected section or line '$sec'" ;; esac
    done
}
# semantic_capture <db> <file>: run semantic_snapshot with its real exit status (no stderr folding),
# then require full coverage. Any failure is fatal for the comparison: prints faults and returns 1.
semantic_capture() {
    local db=$1 file=$2 status faults
    semantic_snapshot_raw "$db" >"$file" 2>"$file.stderr"
    status=$?
    faults=$(semantic_coverage_faults "$file")
    if [ "$status" -ne 0 ] || [ -s "$file.stderr" ] || [ -n "$faults" ]; then
        printf 'semantic snapshot of %s failed (psql status %s): %s %s\n' "$db" "$status" \
            "$(head -1 "$file.stderr" 2>/dev/null)" "$(printf '%s' "$faults" | paste -sd';' -)"
        return 1
    fi
    return 0
}
# The snapshot query itself, unfolded: psql's own status and stderr are what semantic_capture checks.
semantic_snapshot_raw() {
    semantic_sql | psql -X -At -q -v ON_ERROR_STOP=1 -d "$1"
}

# All four generated query functions of every family. This captures exact definitions, execution
# properties and EXPLAIN plan text so a 1.2 update cannot silently regenerate the released 1.1 form.
query_function_snapshot() {
    sql "$1" <<'SQL'
BEGIN;
CREATE TEMP TABLE pg_temp.query_function_snapshot (line text) ON COMMIT DROP;
DO $snapshot$
DECLARE
    r record;
    fn_oid oid;
    call_sql text;
    plan_line text;
    plan_text text;
BEGIN
    FOR r IN
        SELECT format('%I.%I', sv.table_schema, sv.table_name) AS family, q.kind, q.identity,
               split_part(q.identity, '(', 1) || q.args AS call_sql
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL (VALUES
            ('as_of', sv.func_as_of, '(now())'),
            ('between', sv.func_between, '(now() - interval ''1 day'', now())'),
            ('between_symmetric', sv.func_between_symmetric, '(now(), now() - interval ''1 day'')'),
            ('from_to', sv.func_from_to, '(now() - interval ''1 day'', now())')
        ) AS q(kind, identity, args)
        ORDER BY 1, 2
    LOOP
        fn_oid := to_regprocedure(r.identity);
        INSERT INTO pg_temp.query_function_snapshot (line)
        SELECT format('function|%s|%s|definition=%s|volatility=%s|strict=%s|parallel=%s|security=%s|config=%s|owner=%s|acl=%s',
                      r.family, r.kind, md5(pg_get_functiondef(p.oid)), p.provolatile, p.proisstrict,
                      p.proparallel, p.prosecdef, coalesce(p.proconfig::text, ''), p.proowner::regrole,
                      coalesce((SELECT string_agg(CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END || ':' || a.privilege_type,
                                                 ',' ORDER BY CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END, a.privilege_type)
                                FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a), ''))
        FROM pg_proc AS p WHERE p.oid = fn_oid;
        plan_text := '';
        call_sql := r.call_sql;
        FOR plan_line IN EXECUTE format('EXPLAIN (COSTS OFF) SELECT * FROM %s', call_sql)
        LOOP
            plan_text := plan_text || CASE WHEN plan_text = '' THEN '' ELSE E'\n' END || plan_line;
        END LOOP;
        INSERT INTO pg_temp.query_function_snapshot (line)
        VALUES (format('plan|%s|%s|%s', r.family, r.kind, md5(plan_text)));
    END LOOP;
END;
$snapshot$;
SELECT line FROM pg_temp.query_function_snapshot ORDER BY line;
COMMIT;
SQL
}

# Schema dump of the user side. Remove only volatile headers. CREATE POLICY statements are sorted at
# the comparison site because their pg_dump order follows object OIDs. ACL statements remain intact:
# omitting them would hide pg_init_privs defects that suppress GRANT/REVOKE output.
dump_schema() {
    pg_dump -s -d "$1" 2>>"$LOG" | grep -v -E '^-- Dumped (from|by)|^\\(un)?restrict '
}

# Token-normalizing sed script: installation token -> <token>, policy copy -> copy-of-<base policy>.
token_sed() {
    sql "$1" <<'SQL'
SELECT 's/' || hp.polname || '/sql_saga_copy_of_' || bp.polname || '/g'
FROM sql_saga.system_versioning AS sv
JOIN pg_policy AS hp ON hp.polrelid = format('%I.%I', sv.history_schema_name, sv.history_table_name)::regclass
JOIN pg_policy AS bp ON bp.polrelid = format('%I.%I', sv.table_schema, sv.table_name)::regclass
CROSS JOIN (SELECT (xpath('//row/token/text()', query_to_xml('SELECT token FROM sql_saga.installation', false, false, '')))[1]::text AS token
            WHERE to_regclass('sql_saga.installation') IS NOT NULL) AS t
WHERE hp.polname = 'sql_saga_' || left(md5(bp.polname || t.token), 16)
UNION ALL
SELECT 's/' || (xpath('//row/token/text()', query_to_xml('SELECT token FROM sql_saga.installation', false, false, '')))[1]::text || '/<token>/g'
WHERE to_regclass('sql_saga.installation') IS NOT NULL;
SQL
}

# Catalog rows touched since a point in time: xmin of everything user- and extension-defined.
xmin_digest() {
    sql "$1" <<'SQL'
SELECT md5(string_agg(x, ',' ORDER BY x)) FROM (
    SELECT 'class' || c.oid || ':' || c.xmin::text FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace
     WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND n.nspname !~ '^pg_(toast_)?temp'
    UNION ALL SELECT 'proc' || p.oid || ':' || p.xmin::text FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace
     WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
    UNION ALL SELECT 'policy' || oid || ':' || xmin::text FROM pg_policy
    UNION ALL SELECT 'rewrite' || oid || ':' || xmin::text FROM pg_rewrite WHERE ev_class >= 16384
    UNION ALL SELECT 'trigger' || oid || ':' || xmin::text FROM pg_trigger WHERE NOT tgisinternal
    UNION ALL SELECT 'evt' || oid || ':' || xmin::text FROM pg_event_trigger
    UNION ALL SELECT 'ext' || oid || ':' || xmin::text FROM pg_extension
) AS s(x);
SQL
}

# Missing, orphaned or stale history policy copies (0 = in sync); see devops/restore_roundtrip.sh.
policy_copy_faults() {
    sql "$1" <<'SQL' | tail -1
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
SET LOCAL session_replication_role = replica;
SELECT format('CREATE POLICY %I ON %s FOR SELECT USING (%s)', w.probe, w.hist, w.base_qual)
FROM pg_temp.want AS w WHERE w.base_qual IS NOT NULL \gexec
UPDATE pg_temp.want AS w SET qual = pg_get_expr(p.polqual, p.polrelid)
FROM pg_policy AS p WHERE p.polrelid = w.hist AND p.polname = w.probe;
SELECT CASE WHEN to_regclass('sql_saga.installation') IS NULL THEN 'no installation token' ELSE (
       (SELECT count(*) FROM pg_temp.want AS w
         WHERE NOT EXISTS (SELECT FROM pg_policy AS p WHERE p.polrelid = w.hist AND p.polname = w.copy_name))
     + (SELECT count(*) FROM pg_temp.fam AS f JOIN pg_policy AS p ON p.polrelid = f.hist
         WHERE p.polname ~ '^sql_saga_[0-9a-f]{16}$'
           AND NOT EXISTS (SELECT FROM pg_temp.want AS w WHERE w.hist = f.hist AND w.copy_name = p.polname))
     + (SELECT count(*) FROM pg_temp.want AS w JOIN pg_policy AS p ON p.polrelid = w.hist AND p.polname = w.copy_name
         WHERE p.polcmd <> 'r' OR p.polroles <> w.polroles OR p.polpermissive <> w.polpermissive
            OR pg_get_expr(p.polqual, p.polrelid) IS DISTINCT FROM w.qual))::text END;
ROLLBACK;
SQL
}

# Missing or stale SYSTEM_TIME base-column snapshots (0 = every family's snapshot equals its live
# columns as a set). The snapshot only detects added/removed columns, so column order is not part of
# the contract: a restore recreates inherited children with inherited columns first (coordinator
# decision, step 6). Snapshot writes stay attnum-ordered; only comparisons are order-independent.
snapshot_faults() {
    local db=$1
    if [ "$(q "$db" "SELECT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = 'sql_saga.system_time_era'::regclass AND a.attname = 'table_columns_snapshot' AND a.attnum > 0 AND NOT a.attisdropped)")" != t ]; then
        echo missing
        return
    fi
    q "$db" "SELECT count(*) FROM sql_saga.system_time_era AS ste WHERE (SELECT array_agg(s ORDER BY s COLLATE \"C\") FROM unnest(ste.table_columns_snapshot) AS s) IS DISTINCT FROM (SELECT array_agg(a.attname ORDER BY a.attname COLLATE \"C\") FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS ns ON ns.oid = c.relnamespace JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid WHERE (ns.nspname, c.relname) = (ste.table_schema, ste.table_name) AND a.attnum > 0 AND NOT a.attisdropped)"
}

ids_as() {  # ids_as <db> <role> <relation>: sorted ids, or denied
    local r
    r=$(as_role "$1" "$2" "SELECT 'ids=' || coalesce(string_agg(id::text, ',' ORDER BY id), '') FROM $3")
    case "$r" in ids=*) echo "${r#ids=}" ;; *) echo denied ;; esac
}

# What every role sees and may do through every user relation and as-of function: the behavioural
# fingerprint compared between the fresh and the upgraded database (rows are counted, not listed).
role_matrix() {
    local db=$1 role rel out family kind call
    for rel in $(q "$db" "SELECT string_agg(format('%I', c.relname), ' ' ORDER BY c.relname) FROM pg_class AS c WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'v')"); do
        for role in ${PREFIX}_owner ${PREFIX}_r1 ${PREFIX}_regular ${PREFIX}_admin ${PREFIX}_none ${PREFIX}_blind; do
            out="$(as_role "$db" "$role" "SELECT count(*) FROM $rel")"
            echo "$rel $role select=$out"
            for priv in INSERT UPDATE DELETE; do
                echo "$rel $role $priv=$(q "$db" "SELECT has_table_privilege('$role', 'public.$rel', '$priv')")"
            done
        done
    done
    # Every registered query function, including custom names, is executed for every family/role.
    while IFS='|' read -r family kind call; do
        [ -n "$call" ] || continue
        for role in ${PREFIX}_owner ${PREFIX}_r1 ${PREFIX}_regular ${PREFIX}_admin ${PREFIX}_none ${PREFIX}_blind; do
            out="$(as_role "$db" "$role" "SELECT count(*) FROM $call")"
            echo "$family $kind $role execute=$out"
        done
    done <<EOF
$(q "$db" "SELECT string_agg(format('%I.%I|%s|%s', sv.table_schema, sv.table_name, q.kind, CASE q.kind WHEN 'as_of' THEN sv.func_as_of || '(now())' WHEN 'between' THEN sv.func_between || '(now() - interval ''1 day'', now())' WHEN 'between_symmetric' THEN sv.func_between_symmetric || '(now(), now() - interval ''1 day'')' ELSE sv.func_from_to || '(now() - interval ''1 day'', now())' END), E'\\n' ORDER BY sv.table_schema,sv.table_name,q.ord) FROM sql_saga.system_versioning AS sv CROSS JOIN (VALUES (1,'as_of'),(2,'between'),(3,'between_symmetric'),(4,'from_to')) AS q(ord,kind)")
EOF
}

tenant_isolation() {
    local db=$1 out="" role
    for role in ${PREFIX}_r1 ${PREFIX}_regular ${PREFIX}_admin ${PREFIX}_none; do
        out="$out $role:$(ids_as "$db" "$role" rdoc_with_history)"
    done
    echo "${out# }"
}
ISOLATION_EXPECTED="${PREFIX}_r1:1,1 ${PREFIX}_regular:2,2 ${PREFIX}_admin:2,2 ${PREFIX}_none:denied"

# Extension initial privileges on a managed user object are valid only while that object is an
# extension member. The upgrade must leave all family objects and FK helpers detached and free of
# privtype='e' rows, just like a fresh installation.
nonmember_managed_initprivs() {
    q "$1" "WITH managed(classoid,objoid) AS (
        SELECT 'pg_class'::regclass::oid, x.oid::oid FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL unnest(ARRAY[
            to_regclass(format('%I.%I',sv.history_schema_name,sv.history_table_name)),
            to_regclass(format('%I.%I',sv.history_schema_name,sv.history_table_name || '_gated')),
            to_regclass(format('%I.%I',sv.view_schema_name,sv.view_table_name))]) AS x(oid)
        UNION SELECT 'pg_class'::regclass::oid, to_regclass(format('%I.%I',uv.view_schema,uv.view_name))::oid FROM sql_saga.updatable_view AS uv
        UNION SELECT 'pg_proc'::regclass::oid, x.oid::oid FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL unnest(ARRAY[to_regprocedure(sv.func_as_of),to_regprocedure(sv.func_between),to_regprocedure(sv.func_between_symmetric),to_regprocedure(sv.func_from_to)]) AS x(oid)
        UNION SELECT 'pg_proc'::regclass::oid, to_regprocedure(fk.fk_helper_function)::oid FROM sql_saga.foreign_keys AS fk WHERE fk.type='regular_to_temporal'
    ) SELECT count(*) FROM pg_init_privs AS i JOIN managed AS m ON (m.classoid,m.objoid)=(i.classoid,i.objoid)
      WHERE i.privtype='e' AND NOT EXISTS (SELECT FROM pg_depend AS d JOIN pg_extension AS e ON e.oid=d.refobjid
        WHERE e.extname='sql_saga' AND (d.classid,d.objid,d.deptype)=(i.classoid,i.objoid,'e'::\"char\"))"
}

# ---------------------------------------------------------------------------------------------
# Section 5.5 structural assertions: one line per violated invariant.
# ---------------------------------------------------------------------------------------------
structural_violations() {
    sql "$1" <<SQL
CREATE TEMP TABLE violation (n serial, msg text);
DO \$chk\$
DECLARE
    v text;
    n bigint;
    tok text;
    r record;
    fam record;
    obj record;
    acl text;
    def text;
    want text;
    rw text := 'PUBLIC:DELETE,PUBLIC:INSERT,PUBLIC:SELECT,PUBLIC:UPDATE';
BEGIN
    SELECT x.extversion INTO v FROM pg_extension AS x WHERE x.extname = 'sql_saga';
    IF v IS DISTINCT FROM '$NEW_VERSION' THEN
        INSERT INTO violation (msg) VALUES (format('extversion is %s, expected $NEW_VERSION', v));
    END IF;

    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = 'sql_saga.system_time_era'::regclass
          AND a.attname = 'table_columns_snapshot'
          AND a.attnum > 0
          AND NOT a.attisdropped
    ) THEN
        INSERT INTO violation (msg) VALUES ('system_time_era.table_columns_snapshot is missing');
    ELSE
        EXECUTE \$snapshot\$
            SELECT count(*)
            FROM sql_saga.system_time_era AS ste
            WHERE (SELECT array_agg(s ORDER BY s COLLATE "C") FROM unnest(ste.table_columns_snapshot) AS s) IS DISTINCT FROM (
                SELECT array_agg(a.attname ORDER BY a.attname COLLATE "C")
                FROM pg_catalog.pg_class AS c
                JOIN pg_catalog.pg_namespace AS ns ON ns.oid = c.relnamespace
                JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
                WHERE (ns.nspname, c.relname) = (ste.table_schema, ste.table_name)
                  AND a.attnum > 0
                  AND NOT a.attisdropped
            )
        \$snapshot\$ INTO n;
        IF n <> 0 THEN
            INSERT INTO violation (msg) VALUES (format('%s system_time_era snapshots differ from live columns', n));
        END IF;
    END IF;

    IF to_regclass('sql_saga.installation') IS NULL THEN
        INSERT INTO violation (msg) VALUES ('sql_saga.installation is missing (no installation token)');
    ELSE
        EXECUTE 'SELECT count(*), min(token) FROM sql_saga.installation' INTO n, tok;
        IF n <> 1 THEN
            INSERT INTO violation (msg) VALUES (format('sql_saga.installation has %s rows, expected exactly 1', n));
        END IF;
        IF EXISTS (SELECT FROM pg_extension AS x WHERE x.extname = 'sql_saga' AND to_regclass('sql_saga.installation') = ANY (x.extconfig)) THEN
            INSERT INTO violation (msg) VALUES ('sql_saga.installation is an extension configuration table (it must not be dumped)');
        END IF;
    END IF;

    IF to_regclass('sql_saga.temporal_merge_cache') IS NOT NULL THEN
        INSERT INTO violation (msg) VALUES ('obsolete table sql_saga.temporal_merge_cache still exists');
    END IF;
    FOR r IN
        SELECT p.oid::regprocedure AS routine FROM pg_proc AS p
        WHERE p.pronamespace = 'sql_saga'::regnamespace
          AND p.proname IN ('temporal_merge_source_columns_hash', 'temporal_merge_cache_key', 'temporal_merge_cache_lookup',
                            'temporal_merge_cache_maybe_purge', 'temporal_merge_cache_store', 'temporal_merge_invalidate_cache',
                            'temporal_merge_cache_stats', 'temporal_merge_cache_invalidation')
        ORDER BY 1
    LOOP
        INSERT INTO violation (msg) VALUES (format('obsolete cache routine %s still exists', r.routine));
    END LOOP;
    IF EXISTS (SELECT FROM pg_event_trigger WHERE evtname = 'sql_saga_cache_invalidation') THEN
        INSERT INTO violation (msg) VALUES ('obsolete event trigger sql_saga_cache_invalidation still exists');
    END IF;
    FOR r IN SELECT evtname FROM pg_event_trigger WHERE evtfoid::regproc::text LIKE 'sql_saga.%' AND evtenabled = 'D' LOOP
        INSERT INTO violation (msg) VALUES (format('event trigger %s is disabled', r.evtname));
    END LOOP;
    -- every routine and relation in schema sql_saga is an extension member
    FOR r IN
        SELECT format('function %s', p.oid::regprocedure) AS o FROM pg_proc AS p
        WHERE p.pronamespace = 'sql_saga'::regnamespace
          AND NOT EXISTS (SELECT FROM pg_depend AS d WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e')
          -- per-table synchronization functions generated by add_era are user-table objects by design
          AND p.proname NOT IN (SELECT e.sync_temporal_trg_function_name FROM sql_saga.era AS e WHERE e.sync_temporal_trg_function_name IS NOT NULL)
        UNION ALL
        SELECT format('relation %s', c.oid::regclass) FROM pg_class AS c
        WHERE c.relnamespace = 'sql_saga'::regnamespace AND c.relkind IN ('r', 'v', 'm', 'p')
          AND NOT EXISTS (SELECT FROM pg_depend AS d WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e')
    LOOP
        INSERT INTO violation (msg) VALUES (format('%s in schema sql_saga is not an extension member', r.o));
    END LOOP;

    -- system-versioning families
    FOR fam IN
        SELECT sv.*, b.oid AS base, h.oid AS hist, w.oid AS wh, b.relowner AS owner,
               b.relforcerowsecurity AS base_force, h.relrowsecurity AS hist_rls, h.relforcerowsecurity AS hist_force,
               to_regclass(format('%I.%I', sv.history_schema_name, sv.history_table_name || '_gated')) AS gated
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS b ON b.oid = to_regclass(format('%I.%I', sv.table_schema, sv.table_name))
        JOIN pg_class AS h ON h.oid = to_regclass(format('%I.%I', sv.history_schema_name, sv.history_table_name))
        LEFT JOIN pg_class AS w ON w.oid = to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name))
        ORDER BY b.oid
    LOOP
        -- ownership and fixed ACLs of every derived object
        FOR obj IN
            SELECT 'history '  || fam.hist::regclass AS what, c.relowner AS owner, c.relacl AS acl, 'r'::"char" AS k, 'PUBLIC:SELECT' AS want FROM pg_class AS c WHERE c.oid = fam.hist
            UNION ALL SELECT 'with-history view ' || c.oid::regclass, c.relowner, c.relacl, 'r', 'PUBLIC:SELECT' FROM pg_class AS c WHERE c.oid = fam.wh
            UNION ALL SELECT 'history-gated view ' || c.oid::regclass, c.relowner, c.relacl, 'r', 'PUBLIC:SELECT' FROM pg_class AS c WHERE c.oid = fam.gated
            UNION ALL SELECT 'function ' || p.oid::regprocedure, p.proowner, p.proacl, 'f', 'PUBLIC:EXECUTE'
                FROM pg_proc AS p WHERE p.oid = ANY (ARRAY[to_regprocedure(fam.func_as_of), to_regprocedure(fam.func_between),
                                                         to_regprocedure(fam.func_between_symmetric), to_regprocedure(fam.func_from_to)])
        LOOP
            IF obj.owner <> fam.owner THEN
                INSERT INTO violation (msg) VALUES (format('%s is owned by %s, not by the base owner %s', obj.what, obj.owner::regrole, fam.owner::regrole));
            END IF;
            SELECT string_agg(CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END || ':' || a.privilege_type, ','
                              ORDER BY CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END, a.privilege_type)
            INTO acl FROM aclexplode(COALESCE(obj.acl, acldefault(obj.k, obj.owner))) AS a WHERE a.grantee <> obj.owner;
            IF acl IS DISTINCT FROM obj.want THEN
                INSERT INTO violation (msg) VALUES (format('%s ACL is {%s}, expected the fixed {%s}', obj.what, acl, obj.want));
            END IF;
        END LOOP;

        IF fam.gated IS NULL THEN
            INSERT INTO violation (msg) VALUES (format('family %I.%I has no history-gated view %s_gated', fam.table_schema, fam.table_name, fam.history_table_name));
        ELSE
            def := pg_get_viewdef(fam.gated);
            IF def !~ 'has_table_privilege' OR def !~ 'row_security_active' OR def !~ '::regclass' THEN
                INSERT INTO violation (msg) VALUES (format('%s is not the canonical gate (has_table_privilege + row_security_active with regclass constants)', fam.gated));
            END IF;
            IF def ~ '''?[0-9]{4,}''?::oid' THEN
                INSERT INTO violation (msg) VALUES (format('%s persists a bare OID instead of a qualified regclass constant', fam.gated));
            END IF;
            IF EXISTS (SELECT FROM pg_class AS c WHERE c.oid = fam.gated AND 'security_invoker=on' = ANY (coalesce(c.reloptions, '{}'))) THEN
                INSERT INTO violation (msg) VALUES (format('%s must be owner-rights, not security_invoker', fam.gated));
            END IF;
        END IF;
        IF fam.wh IS NULL THEN
            INSERT INTO violation (msg) VALUES (format('family %I.%I lost its with-history view', fam.table_schema, fam.table_name));
        ELSE
            def := pg_get_viewdef(fam.wh);
            IF (SELECT count(*) FROM regexp_matches(def, 'UNION ALL', 'g')) <> 2
               OR fam.gated IS NULL OR position(fam.history_table_name || '_gated' IN def) = 0
               OR def !~ 'row_security_active' OR def !~ '__internal_history_policies_synchronized' THEN
                INSERT INTO violation (msg) VALUES (format('%s is not the three-branch with-history view (base, gated, RLS history)', fam.wh::regclass));
            END IF;
            IF def ~ '''?[0-9]{4,}''?::oid' THEN
                INSERT INTO violation (msg) VALUES (format('%s persists a bare OID instead of a qualified regclass constant', fam.wh::regclass));
            END IF;
            IF NOT EXISTS (SELECT FROM pg_class AS c WHERE c.oid = fam.wh AND c.reloptions && ARRAY['security_invoker=on', 'security_invoker=true']) THEN
                INSERT INTO violation (msg) VALUES (format('%s is not security_invoker', fam.wh::regclass));
            END IF;
        END IF;
        FOR r IN
            SELECT p.oid::regprocedure AS routine FROM pg_proc AS p
            WHERE p.oid = ANY (ARRAY[to_regprocedure(fam.func_as_of), to_regprocedure(fam.func_between),
                                     to_regprocedure(fam.func_between_symmetric), to_regprocedure(fam.func_from_to)])
              AND p.prosecdef
        LOOP
            INSERT INTO violation (msg) VALUES (format('%s must be SECURITY INVOKER', r.routine));
        END LOOP;

        -- history RLS, FORCE parity, fixed and copied policies
        IF NOT fam.hist_rls THEN
            INSERT INTO violation (msg) VALUES (format('history %s does not have row level security enabled', fam.hist::regclass));
        END IF;
        IF fam.hist_force <> fam.base_force THEN
            INSERT INTO violation (msg) VALUES (format('history %s FORCE row level security is %s, base %s has %s', fam.hist::regclass, fam.hist_force, fam.base::regclass, fam.base_force));
        END IF;
        IF tok IS NOT NULL THEN
            SELECT count(*) INTO n FROM pg_policy AS p WHERE p.polrelid = fam.hist AND p.polname = 'sql_saga_history_reader_' || tok AND p.polpermissive AND p.polcmd = 'r';
            IF n <> 1 THEN
                INSERT INTO violation (msg) VALUES (format('history %s has %s permissive reader policies for the current token, expected 1', fam.hist::regclass, n));
            END IF;
            SELECT count(*) INTO n FROM pg_policy AS p WHERE p.polrelid = fam.hist AND p.polname = 'sql_saga_history_base_select_' || tok AND NOT p.polpermissive AND p.polcmd = 'r';
            IF n <> 1 THEN
                INSERT INTO violation (msg) VALUES (format('history %s has %s restrictive base-select policies for the current token, expected 1', fam.hist::regclass, n));
            END IF;
            FOR r IN SELECT p.polname FROM pg_policy AS p WHERE p.polrelid = fam.hist AND p.polname ~ '^sql_saga_history_(reader|base_select)_'
                       AND p.polname NOT IN ('sql_saga_history_reader_' || tok, 'sql_saga_history_base_select_' || tok) LOOP
                INSERT INTO violation (msg) VALUES (format('history %s keeps fixed policy %s of another installation token', fam.hist::regclass, r.polname));
            END LOOP;
        END IF;
        FOR r IN SELECT p.polname FROM pg_policy AS p WHERE p.polrelid = fam.hist
                   AND p.polname !~ '^sql_saga_([0-9a-f]{16}|history_reader_[0-9a-z]+|history_base_select_[0-9a-z]+)$' LOOP
            INSERT INTO violation (msg) VALUES (format('history %s has non-managed policy %s', fam.hist::regclass, r.polname));
        END LOOP;
    END LOOP;

    -- FOR PORTION OF and current views
    FOR obj IN
        SELECT uv.*, c.oid AS vid, c.relowner AS vowner, c.relacl AS vacl, c.reloptions AS opts, b.relowner AS bowner, e.range_column_name,
               e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name
        FROM sql_saga.updatable_view AS uv
        JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
        JOIN pg_class AS c ON c.oid = to_regclass(format('%I.%I', uv.view_schema, uv.view_name))
        JOIN pg_class AS b ON b.oid = to_regclass(format('%I.%I', uv.table_schema, uv.table_name))
        ORDER BY 1, 2
    LOOP
        IF obj.vowner <> obj.bowner THEN
            INSERT INTO violation (msg) VALUES (format('%s view %s is owned by %s, not by the base owner', obj.view_type, obj.vid::regclass, obj.vowner::regrole));
        END IF;
        SELECT string_agg(CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END || ':' || a.privilege_type, ','
                          ORDER BY CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END, a.privilege_type)
        INTO acl FROM aclexplode(COALESCE(obj.vacl, acldefault('r', obj.vowner))) AS a WHERE a.grantee <> obj.vowner;
        IF acl IS DISTINCT FROM rw THEN
            INSERT INTO violation (msg) VALUES (format('%s view %s ACL is {%s}, expected the fixed {%s}', obj.view_type, obj.vid::regclass, acl, rw));
        END IF;
        IF NOT coalesce(obj.opts, '{}') && ARRAY['security_invoker=on', 'security_invoker=true'] THEN
            INSERT INTO violation (msg) VALUES (format('%s view %s is not security_invoker', obj.view_type, obj.vid::regclass));
        END IF;
        IF obj.view_type = 'current' THEN
            IF NOT coalesce(obj.opts, '{}') && ARRAY['security_barrier=true', 'security_barrier=on'] THEN
                INSERT INTO violation (msg) VALUES (format('current view %s is not security_barrier', obj.vid::regclass));
            END IF;
            FOR r IN
                SELECT a.arg FROM pg_trigger AS t
                CROSS JOIN LATERAL unnest(string_to_array(encode(t.tgargs, 'escape'), '\\000')) AS a(arg)
                WHERE t.tgrelid = obj.vid AND t.tgname = obj.trigger_name
                  AND a.arg IN (obj.range_column_name, obj.valid_from_column_name, obj.valid_until_column_name, obj.valid_to_column_name)
            LOOP
                INSERT INTO violation (msg) VALUES (format('current view %s identity still includes era column %s (P2 range-only identity fix not applied)', obj.vid::regclass, r.arg));
            END LOOP;
        END IF;
    END LOOP;
END;
\$chk\$;
SELECT msg FROM violation ORDER BY n;
SQL
}

# ---------------------------------------------------------------------------------------------
# Section 5.6 behaviour (run against both the upgraded and the fresh database)
# ---------------------------------------------------------------------------------------------
behaviour() {
    local db=$1 r out tok copy
    echo "  -- behaviour on $db"
    check "tenant isolation through rdoc_with_history" "$ISOLATION_EXPECTED" "$(tenant_isolation "$db")"
    check "r1 reads only tenant a history directly from rdoc_history" "1" "$(ids_as "$db" ${PREFIX}_r1 rdoc_history)"
    check "regular reads only tenant b history directly from rdoc_history" "2" "$(ids_as "$db" ${PREFIX}_regular rdoc_history)"
    r=$(ids_as "$db" ${PREFIX}_none rdoc_history)
    case "$r" in ""|denied) ok "a role without SELECT on rdoc reads no rows from rdoc_history" ;; *) fail "saga_up_none read rdoc_history rows '$r'" ;; esac
    r=$(ids_as "$db" ${PREFIX}_none sv_history)
    case "$r" in ""|denied) ok "a role without SELECT on sv reads no rows from sv_history" ;; *) fail "saga_up_none read sv_history rows '$r'" ;; esac
    for rel in sv sv_with_history sv_history_gated "sv__as_of(now())" "sv__between(now() - interval '1 day', now())"; do
        r=$(as_role "$db" ${PREFIX}_none "SELECT count(*) FROM $rel")
        check "saga_up_none is refused by the base table through $rel" "denied: permission denied for table sv" "$r"
    done
    check "FORCE owner is filtered identically on fdoc_with_history" "1,1" "$(ids_as "$db" ${PREFIX}_owner fdoc_with_history)"
    check "FORCE owner is filtered identically on fdoc_history" "1" "$(ids_as "$db" ${PREFIX}_owner fdoc_history)"
    check "r1 reads custom-named family through zb_at()" "1" "$(as_role "$db" ${PREFIX}_r1 "SELECT count(*) FROM zb_at(now())")"
    check "r1 (hardened defaults) runs the as-of function sv__as_of()" "2" "$(as_role "$db" ${PREFIX}_r1 "SELECT count(*) FROM sv__as_of(now())")"
    check "admin (inherits regular) reads sv_with_history" "4" "$(as_role "$db" ${PREFIX}_admin "SELECT count(*) FROM sv_with_history")"

    out=$(sql "$db" <<SQL
BEGIN;
CREATE TEMP TABLE acl0 AS
    SELECT string_agg(c.oid::regclass || '=' || coalesce(c.relacl::text, ''), ';' ORDER BY c.oid::regclass::text) AS a
    FROM pg_class AS c WHERE c.relname IN ('sv_with_history', 'sv_history', 'sv_history_gated');
GRANT SELECT ON sv TO ${PREFIX}_none;
SET ROLE ${PREFIX}_none;
SELECT 'n=' || count(*) FROM sv_with_history;
RESET ROLE;
SELECT 'acl_unchanged=' || ((SELECT a FROM acl0) = (SELECT string_agg(c.oid::regclass || '=' || coalesce(c.relacl::text, ''), ';' ORDER BY c.oid::regclass::text)
                                                    FROM pg_class AS c WHERE c.relname IN ('sv_with_history', 'sv_history', 'sv_history_gated')));
ROLLBACK;
SQL
)
    check "GRANT on the base takes effect at once and leaves derived ACLs alone" "n=4,acl_unchanged=true" "$(printf '%s\n' "$out" | paste -sd, -)"
    out=$(sql "$db" <<SQL
BEGIN;
REVOKE SELECT ON sv FROM ${PREFIX}_r1;
SET ROLE ${PREFIX}_r1;
SELECT count(*) FROM sv_with_history;
ROLLBACK;
SQL
)
    check "REVOKE on the base takes effect at once" "ERROR:  permission denied for table sv" "$(printf '%s\n' "$out" | grep -m1 ERROR)"

    tok=$(printf "SELECT (xpath('//row/token/text()', query_to_xml('SELECT token FROM sql_saga.installation', false, false, '')))[1]::text WHERE to_regclass('sql_saga.installation') IS NOT NULL\n" | sql "$db")
    copy="sql_saga_$(printf '%s' "tenant_c$tok" | md5 2>/dev/null || printf '%s' "tenant_c$tok" | md5sum)"
    copy=$(printf '%s' "$copy" | cut -c1-25)
    out=$(sql "$db" <<SQL
BEGIN;
SET client_min_messages = notice;
CREATE POLICY tenant_c ON rdoc FOR SELECT TO ${PREFIX}_none USING (tenant = 'c');
DROP POLICY tenant_c ON rdoc;
ROLLBACK;
SQL
)
    check "CREATE POLICY on the base creates the tokenized history copy (exact NOTICE)" \
        "NOTICE:  sql_saga: Created policy $copy on rdoc_history as the history copy of policy tenant_c on rdoc" \
        "$(printf '%s\n' "$out" | grep -m1 'Created policy')"
    check "DROP POLICY on the base drops the history copy (exact NOTICE)" \
        "NOTICE:  sql_saga: Dropped policy $copy on rdoc_history, the history copy of policy tenant_c on rdoc" \
        "$(printf '%s\n' "$out" | grep -m1 'Dropped policy')"
    out=$(sql "$db" -v VERBOSITY=default <<SQL
BEGIN;
SET LOCAL session_replication_role = replica;
SELECT format('DROP POLICY %I ON rdoc_history', 'sql_saga_' || left(md5('tenant_a' || '$tok'), 16)) \gexec
SET LOCAL session_replication_role = origin;
SET ROLE ${PREFIX}_r1;
SELECT count(*) FROM rdoc_with_history;
ROLLBACK;
SQL
)
    check "a missing policy copy fails closed" "ERROR:  sql_saga: history of rdoc is not synchronized (1 policy copies missing, 0 stale)" "$(printf '%s\n' "$out" | grep -m1 ERROR)"
    check "the fail-closed error tells how to repair" "HINT:  Run: SELECT sql_saga.sync_history_policies('rdoc');" "$(printf '%s\n' "$out" | grep -m1 HINT)"

    out=$(sql "$db" <<SQL
BEGIN;
SET ROLE ${PREFIX}_r1;
UPDATE fp__for_portion_of_valid SET x = 'by r1' WHERE id = 1;
SELECT 'fpo_ok';
INSERT INTO fp__current_valid (id, x) VALUES (9, 'new');
SELECT 'current_ok=' || count(*) FROM fp WHERE id = 9;
ROLLBACK;
SQL
)
    check "authorized FOR PORTION OF update and current-view insert (range-only identity) succeed" "fpo_ok,current_ok=1" "$(printf '%s\n' "$out" | paste -sd, -)"
    r=$(as_role "$db" ${PREFIX}_regular "UPDATE fp__for_portion_of_valid SET x = 'no' WHERE id = 1")
    check "unauthorized FOR PORTION OF update is refused at the base table" "denied: permission denied for table fp" "$r"
    out=$(sql "$db" <<SQL
BEGIN;
SET ROLE ${PREFIX}_r1;
CREATE TEMP TABLE src_r1 (row_id int, id int, valid_range daterange, x text) ON COMMIT DROP;
INSERT INTO src_r1 VALUES (1, 7, '[2025-01-01,infinity)', 'merged');
CALL sql_saga.temporal_merge(target_table => 'fp', source_table => 'src_r1',
    primary_identity_columns => '{id}', mode => 'MERGE_ENTITY_UPSERT', era_name => 'valid');
SELECT 'merged=' || count(*) FROM fp WHERE id = 7;
ROLLBACK;
SQL
)
    check "temporal_merge works for a hardened non-superuser without the shared cache" "merged=1" "$(printf '%s\n' "$out" | grep -m1 -E 'merged=|ERROR')"
    check "history policy copies in sync" "0" "$(policy_copy_faults "$db")"

    # Writes and history after the upgrade (1.1 by-name writer: history-only column, dropped slot).
    out=$(sql "$db" <<SQL
BEGIN;
UPDATE fdoc SET body = 'v3' WHERE id = 1;
SET LOCAL row_security = off;
SELECT 'fdoc_hist=' || count(*) || ',' || count(*) FILTER (WHERE archived_note IS NULL) FROM fdoc_history WHERE id = 1;
UPDATE sv SET x = 'v3' WHERE id = 1;
SELECT 'sv_hist=' || count(*) FROM sv_history WHERE id = 1;
ROLLBACK;
SQL
)
    check "writes record history by column name (history-only column, dropped slot)" "fdoc_hist=2,2,sv_hist=2" "$(printf '%s\n' "$out" | grep -E '^(fdoc_hist|sv_hist)=|ERROR' | paste -sd, -)"
    out=$(sql "$db" <<SQL
BEGIN;
SET client_min_messages = notice;
ALTER TABLE sv ADD COLUMN y int;
ALTER TABLE sv_history ADD COLUMN y int;
SELECT sql_saga.rebuild_system_versioning_views('sv');
SELECT 'cols=' || count(*) FROM pg_attribute WHERE attrelid = 'sv_with_history'::regclass AND attname = 'y';
SET ROLE ${PREFIX}_r1;
SELECT 'r1=' || count(*) FROM sv__as_of(now());
ROLLBACK;
SQL
)
    check "rebuild_system_versioning_views after a manual column add" "NOTICE:  sql_saga: Rebuilt SYSTEM VERSIONING views and query functions for table sv,cols=1,r1=2" \
        "$(printf '%s\n' "$out" | grep -E 'Rebuilt|^cols=|^r1=|ERROR' | paste -sd, -)"

    # Managed table inheritance: the parent and child families both stay managed, a write through the
    # parent records history in the child's own family, and a parent column add propagates to the
    # child's snapshot (the inheritance branch of the rename/shape follower) and rebuilds cleanly.
    check "inheritance fixture: ip and ic are a managed parent/child pair, ep and ec too" "ic<ip,ec<ep|ic,ip|ec,ep" \
        "$(q "$db" "SELECT (SELECT string_agg(i.inhrelid::regclass || '<' || i.inhparent::regclass, ',' ORDER BY 1) FROM pg_inherits AS i WHERE i.inhparent IN ('ip'::regclass, 'ep'::regclass)) || '|' || (SELECT string_agg(table_name, ',' ORDER BY table_name) FROM sql_saga.system_versioning WHERE table_name IN ('ip','ic')) || '|' || (SELECT string_agg(table_name, ',' ORDER BY table_name) FROM sql_saga.era WHERE table_name IN ('ep','ec'))")"
    out=$(sql "$db" <<SQL
BEGIN;
SET client_min_messages = warning;
UPDATE ip SET x = x || '3';
SET LOCAL row_security = off;
SELECT 'hist=' || (SELECT count(*) FROM ONLY ip_history) || ',' || (SELECT count(*) FROM ic_history);
ALTER TABLE ip ADD COLUMN z int;
ALTER TABLE ip_history ADD COLUMN z int;
ALTER TABLE ic_history ADD COLUMN z int;
SELECT sql_saga.rebuild_system_versioning_views('ip');
SELECT sql_saga.rebuild_system_versioning_views('ic');
SELECT 'snap=' || (SELECT string_agg(s, ',' ORDER BY s COLLATE "C") FROM sql_saga.system_time_era, unnest(table_columns_snapshot) AS s WHERE table_name = 'ic');
SELECT sql_saga.check_integrity();
SELECT 'integrity=ok';
UPDATE ec__for_portion_of_valid SET x = 'q', valid_from = '2025-01-01', valid_until = 'infinity' WHERE id = 1;
SELECT 'ec=' || count(*) FROM ec;
ROLLBACK;
SQL
)
    check "inheritance: parent write records child history, parent column add reaches the child family, FOR PORTION OF on the era child splits" \
        "hist=2,2,snap=id,system_valid_range,x,y,z,integrity=ok,ec=2" \
        "$(printf '%s\n' "$out" | grep -E '^(hist|snap|integrity|ec)=|ERROR' | paste -sd, -)"

    # Foreign keys created under 1.0 (temporal and regular) still enforce after the upgrade.
    out=$(sql "$db" <<SQL
BEGIN;
SET ROLE ${PREFIX}_r1;
INSERT INTO tchild (id, par_id, valid_from, valid_until) VALUES (2, 1, '2024-02-01', '2024-03-01');
INSERT INTO rchild VALUES (2, 1);
SELECT 'fk_ok';
ROLLBACK;
SQL
)
    check "valid temporal and regular FK inserts succeed" "fk_ok" "$(printf '%s\n' "$out" | grep -m1 -E 'fk_ok|ERROR')"
    r=$(as_role "$db" ${PREFIX}_r1 "INSERT INTO tchild (id, par_id, valid_from, valid_until) VALUES (3, 2, '2024-06-01', '2026-01-01')")
    case "$r" in *"violates foreign key"*) ok "temporal FK refuses a child outliving its parent" ;; *) fail "temporal FK did not refuse an uncovered child: '$r'" ;; esac
    r=$(as_role "$db" ${PREFIX}_r1 "INSERT INTO rchild VALUES (3, 99)")
    case "$r" in denied*) ok "regular FK refuses a missing parent" ;; *) fail "regular FK accepted a missing parent: '$r'" ;; esac
    r=$(as_role "$db" ${PREFIX}_r1 "DELETE FROM par WHERE id = 2")
    case "$r" in denied*) ok "regular FK protects a referenced parent from DELETE" ;; *) fail "referenced parent deleted: '$r'" ;; esac

    out=$(sql "$db" <<SQL
BEGIN;
SET ROLE ${PREFIX}_blind;
INSERT INTO rchild VALUES (20, 1);
SELECT 'blind_valid';
ROLLBACK;
SQL
)
    check "INSERT-only regular-FK role inserts a valid reference without parent SELECT" "blind_valid" "$(printf '%s\n' "$out" | grep -m1 -E 'blind_valid|ERROR')"
    r=$(as_role "$db" ${PREFIX}_blind "INSERT INTO rchild VALUES (21, 999)")
    case "$r" in *"violates check constraint"*) ok "INSERT-only regular-FK role gets the check violation for a missing key" ;; *"permission denied"*) fail "regular FK leaked a parent SELECT requirement: '$r'" ;; *) fail "regular FK missing-key result was not its check violation: '$r'" ;; esac

    check "fixtures cover every unique_key_type enum value" "" \
        "$(q "$db" "SELECT coalesce(string_agg(v::text,',' ORDER BY v::text),'') FROM unnest(enum_range(NULL::sql_saga.unique_key_type)) AS v WHERE NOT EXISTS (SELECT FROM sql_saga.unique_keys AS u WHERE u.key_type=v)")"
    check "fixtures cover every fg_type enum value" "" \
        "$(q "$db" "SELECT coalesce(string_agg(v::text,',' ORDER BY v::text),'') FROM unnest(enum_range(NULL::sql_saga.fg_type)) AS v WHERE NOT EXISTS (SELECT FROM sql_saga.foreign_keys AS f WHERE f.type=v)")"
    check "fixtures cover every updatable_view_type enum value" "" \
        "$(q "$db" "SELECT coalesce(string_agg(v::text,',' ORDER BY v::text),'') FROM unnest(enum_range(NULL::sql_saga.updatable_view_type)) AS v WHERE NOT EXISTS (SELECT FROM sql_saga.updatable_view AS u WHERE u.view_type=v)")"

    # Native library (internal to sql_saga, never a separately installable extension).
    r=$(q "$db" "SELECT sql_saga.native_library_version() = (SELECT extversion FROM pg_extension WHERE extname='sql_saga')")
    check "native library version equals sql_saga extversion" "t" "$r"
    check "sql_saga_native is not an available extension" "0" "$(q "$db" "SELECT count(*) FROM pg_available_extensions WHERE name = 'sql_saga_native'")"

    # --clean restore teardown predicate and the as-of regrant trigger exist and are wired.
    check "--clean teardown predicate exists, not executable by PUBLIC" "t|f" \
        "$(q "$db" "SELECT (to_regprocedure('sql_saga.__internal_is_clean_teardown()') IS NOT NULL) || '|' || coalesce(has_function_privilege('public', 'sql_saga.__internal_is_clean_teardown()', 'EXECUTE')::text, 'missing')" | sed 's/true/t/g; s/false/f/g')"
    check "system_versioning regrant trigger present" "1" \
        "$(q "$db" "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'sql_saga.system_versioning'::regclass AND tgname = 'system_versioning_grant_query_functions' AND tgenabled <> 'D'")"
}

assert_structure() {
    local db=$1 v n=0
    echo "  -- structure of $db"
    v=$(structural_violations "$db")
    if printf '%s\n' "$v" | grep -q '^ERROR'; then
        fail "structural check could not run on $db: $(printf '%s\n' "$v" | grep -m1 '^ERROR')"
        return
    fi
    while IFS= read -r line; do
        [ -n "$line" ] || continue
        n=$((n + 1))
        if [ $n -le 25 ]; then fail "$line"; fi
    done <<EOF
$v
EOF
    if [ $n -gt 25 ]; then fail "... and $((n - 25)) more structural violations on $db (full list: $WORK/$db.structure)"; fi
    printf '%s\n' "$v" >"$WORK/$db.structure"
    [ $n -eq 0 ] && ok "all section 5.5 invariants hold on $db"
}

assert_integrity() {
    local db=$1 result
    result=$(q "$db" "SELECT sql_saga.check_integrity()::text = ''")
    check "check_integrity passes on $db" "t" "$result"
}

run_preflight() {  # run_preflight <db>: prints output, returns psql status
    psql -X -q -v ON_ERROR_STOP=1 -d "$1" -f "$PREFLIGHT" 2>&1
}

# =============================================================================================
trap cleanup EXIT
command -v psql >/dev/null || die "psql not found"
[ "$(psql -X -At -d postgres -c 'SELECT rolsuper FROM pg_roles WHERE rolname = current_user' 2>>"$LOG")" = t ] || die "needs a superuser via the libpq environment"
echo "upgrade test: $BASELINE_TAG ($BASELINE_VERSION) -> $NEW_VERSION   server: $(psql -X -At -d postgres -c 'SHOW server_version')   work: $WORK"

# ---------------------------------------------------------------------------------------------
section "artifact"
# ---------------------------------------------------------------------------------------------
TAG_COMMIT=$(git -C "$REPO" rev-parse --verify -q "$BASELINE_TAG^{commit}") || die "tag $BASELINE_TAG not found"
case "$TAG_COMMIT" in "$BASELINE_COMMIT"*) ok "$BASELINE_TAG is commit $BASELINE_COMMIT" ;; *) fail "$BASELINE_TAG is $TAG_COMMIT, not the released $BASELINE_COMMIT" ;; esac
BASELINE_BUILD="$WORK/baseline-$BASELINE_VERSION"
mkdir -p "$BASELINE_BUILD" "$WORK/head" "$EXT" "$FOREIGN_EXT"
git -C "$REPO" archive "$BASELINE_TAG" | tar -x -C "$BASELINE_BUILD" || die "git archive $BASELINE_TAG"
if [ "$OLD_BINARIES" = 1 ]; then
    make -C "$BASELINE_BUILD" PG_CONFIG="$PG_CONFIG" >"$WORK/baseline.build.log" 2>&1 || die "building $BASELINE_TAG (see $WORK/baseline.build.log)"
    OLD_LIB="$BASELINE_BUILD"
    ls "$OLD_LIB"/sql_saga.* >/dev/null 2>&1 || die "no $BASELINE_VERSION C library in $OLD_LIB"
    if ls "$BASELINE_BUILD"/native/target/release/*sql_saga_native* >/dev/null 2>&1; then
        OLD_LIB="$OLD_LIB:$BASELINE_BUILD/native/target/release"
    fi
else
    make -C "$BASELINE_BUILD" PG_CONFIG="$PG_CONFIG" "sql_saga--$BASELINE_VERSION.sql" >"$WORK/baseline.build.log" 2>&1 || die "building $BASELINE_TAG SQL"
    OLD_LIB=""
fi
if [ "$BASELINE_VERSION" = 1.0 ] && [ "$OLD_BINARIES" = 1 ]; then
    V11_BUILD="$WORK/baseline-1.1"
    mkdir -p "$V11_BUILD"
    git -C "$REPO" archive v1.1 | tar -x -C "$V11_BUILD" || die "git archive v1.1"
    make -C "$V11_BUILD" PG_CONFIG="$PG_CONFIG" >"$WORK/baseline-1.1.build.log" 2>&1 || die "building v1.1 for the query-function anchor (see $WORK/baseline-1.1.build.log)"
    V11_LIB="$WORK/baseline-1.1-lib"
    mkdir -p "$V11_LIB"
    V11_C_LIB=$(find "$V11_BUILD" -maxdepth 1 \( -name 'sql_saga.so' -o -name 'sql_saga.dylib' \) -print | head -1)
    [ -n "$V11_C_LIB" ] || die "no v1.1 C library for the query-function anchor"
    cp "$V11_C_LIB" "$V11_LIB/"
    V11_NATIVE_LIB=$(find "$V11_BUILD/native/target/release" -maxdepth 1 \( -name 'libsql_saga_native.so' -o -name 'libsql_saga_native.dylib' \) -print | head -1)
    [ -n "$V11_NATIVE_LIB" ] || die "no v1.1 native library for the query-function anchor"
    case "$V11_NATIVE_LIB" in
        *.dylib) cp "$V11_NATIVE_LIB" "$V11_LIB/sql_saga_native.dylib" ;;
        *.so) cp "$V11_NATIVE_LIB" "$V11_LIB/sql_saga_native.so" ;;
        *) die "unrecognized v1.1 native library suffix: $V11_NATIVE_LIB" ;;
    esac
else
    V11_LIB=""
fi
[ -s "$BASELINE_BUILD/sql_saga--$BASELINE_VERSION.sql" ] || die "$BASELINE_TAG produced no sql_saga--$BASELINE_VERSION.sql"
cp "$BASELINE_BUILD/sql_saga--$BASELINE_VERSION.sql" "$EXT/" || die "copy $BASELINE_VERSION script"
echo "    $BASELINE_VERSION script from $BASELINE_TAG: md5 $(md5 -q "$EXT/sql_saga--$BASELINE_VERSION.sql" 2>/dev/null || md5sum "$EXT/sql_saga--$BASELINE_VERSION.sql" | cut -d' ' -f1)"

CANDIDATE_OK=1
if [ -f "$CANDIDATE_DIR/sql_saga.control" ]; then
    cp "$CANDIDATE_DIR/sql_saga.control" "$EXT/"
    DEFV=$(sed -n "s/^default_version[[:space:]]*=[[:space:]]*'\([^']*\)'.*/\1/p" "$EXT/sql_saga.control")
    check "candidate control default_version" "$NEW_VERSION" "$DEFV"
else
    fail "no sql_saga.control in $CANDIDATE_DIR"; CANDIDATE_OK=0
    cp "$BASELINE_BUILD/sql_saga.control" "$EXT/"
fi
for f in "sql_saga--$NEW_VERSION.sql" "sql_saga--1.0--1.1.sql" "sql_saga--1.0--$NEW_VERSION.sql" "sql_saga--1.1--$NEW_VERSION.sql"; do
    if [ -f "$CANDIDATE_DIR/$f" ]; then cp "$CANDIDATE_DIR/$f" "$EXT/"; ok "candidate ships $f"
    else fail "candidate ships $f (missing in $CANDIDATE_DIR)"; CANDIDATE_OK=0; fi
done
if [ -f "$CANDIDATE_DIR/sql_saga--$BASELINE_VERSION.sql" ]; then
    if cmp -s "$CANDIDATE_DIR/sql_saga--$BASELINE_VERSION.sql" "$BASELINE_BUILD/sql_saga--$BASELINE_VERSION.sql"; then
        ok "shipped sql_saga--$BASELINE_VERSION.sql is byte-identical to $BASELINE_TAG"
    else
        fail "shipped sql_saga--$BASELINE_VERSION.sql differs from $BASELINE_TAG (released scripts are immutable)"
    fi
else
    fail "candidate does not ship frozen sql_saga--$BASELINE_VERSION.sql"
fi
UPD="$EXT/sql_saga--1.1--$NEW_VERSION.sql"
DIRECT_UPD="$EXT/sql_saga--1.0--$NEW_VERSION.sql"
if [ -f "$UPD" ]; then
    body=$(sed -e 's/--.*$//' "$UPD")
    if printf '%s\n' "$body" | grep -qiE '^[[:space:]]*(BEGIN|COMMIT)[[:space:]]*;'; then fail "update script contains BEGIN/COMMIT"; else ok "update script has no BEGIN/COMMIT"; fi
    if printf '%s\n' "$body" | grep -qiw 'CASCADE'; then fail "update script uses CASCADE (never cascade into user objects)"; else ok "update script uses no CASCADE"; fi
    if printf '%s\n' "$body" | grep -qiE 'session_replication_role|DISABLE[[:space:]]+EVENT|ALTER[[:space:]]+EVENT[[:space:]]+TRIGGER[^;]*DISABLE'; then
        fail "update script disables event triggers or changes session_replication_role"
    else ok "update script keeps event triggers on"; fi
fi
if [ -f "$DIRECT_UPD" ] && [ -f "$EXT/sql_saga--1.0--1.1.sql" ] && [ -f "$UPD" ]; then
    FROZEN_GUARD="DO \$native_guard\$ BEGIN IF sql_saga.native_library_version() <> '1.1' THEN RAISE EXCEPTION 'sql_saga: native library version % does not match extension update version 1.1', sql_saga.native_library_version() USING ERRCODE = 'object_not_in_prerequisite_state', HINT = 'Install the sql_saga 1.1 native library and restart PostgreSQL before retrying ALTER EXTENSION.'; END IF; END; \$native_guard\$;"
    DIRECT_GUARD="DO \$native_guard\$ BEGIN IF sql_saga.native_library_version() <> '1.2' THEN RAISE EXCEPTION 'sql_saga: native library version % does not match extension update version 1.2', sql_saga.native_library_version() USING ERRCODE = 'object_not_in_prerequisite_state', HINT = 'Install the sql_saga 1.2 native library and restart PostgreSQL before retrying ALTER EXTENSION.'; END IF; END; \$native_guard\$;"
    guard_count=$(grep -Fxc "$FROZEN_GUARD" "$EXT/sql_saga--1.0--1.1.sql" || true)
    check "frozen 1.0--1.1 script has exactly one retargetable native guard" "1" "$guard_count"
    awk -v old="$FROZEN_GUARD" -v new="$DIRECT_GUARD" '{ if ($0 == old) print new; else print }' "$EXT/sql_saga--1.0--1.1.sql" >"$WORK/direct.retargeted"
    diff -u "$EXT/sql_saga--1.0--1.1.sql" "$WORK/direct.retargeted" >"$WORK/direct.guard.diff" || true
    guard_diff_lines=$(grep -c '^[-+]DO \$native_guard\$' "$WORK/direct.guard.diff" || true)
    other_diff_lines=$(grep '^[-+]' "$WORK/direct.guard.diff" | grep -vE '^(---|\+\+\+|[-+]DO \$native_guard\$)' | wc -l | tr -d ' ')
    check "retargeting changes exactly the old and new native-guard lines" "2|0" "$guard_diff_lines|$other_diff_lines"
    # The frozen validate_fk_helpers aggregates key columns once per FK; the direct script reads one row
    # per helper and key instead (exactly one line, right after its unique stable_columns line).
    FK_ANCHOR='               pg_catalog.array_agg(c.name ORDER BY c.ordinality) AS stable_columns'
    FK_OLD='        FROM sql_saga.foreign_keys AS fk'
    FK_NEW='        FROM (SELECT DISTINCT f.fk_helper_function, f.unique_key_name, f.type FROM sql_saga.foreign_keys AS f) AS fk'
    check "frozen 1.0--1.1 script has exactly one shared-helper validator anchor" "1" \
        "$(grep -Fxc "$FK_ANCHOR" "$EXT/sql_saga--1.0--1.1.sql" || true)"
    awk -v anchor="$FK_ANCHOR" -v old="$FK_OLD" -v new="$FK_NEW" '{ if (prev == anchor && $0 == old) print new; else print; prev = $0 }' \
        "$WORK/direct.retargeted" >"$WORK/direct.fkfixed"
    diff -u "$WORK/direct.retargeted" "$WORK/direct.fkfixed" >"$WORK/direct.fk.diff" || true
    check "the shared-helper validator fix changes exactly one frozen line" "-$FK_OLD|+$FK_NEW|2" \
        "$(grep -E '^[-+] ' "$WORK/direct.fk.diff" | paste -sd'|' -)|$(grep -cE '^[-+][^-+]' "$WORK/direct.fk.diff" || true)"
    cp "$WORK/direct.fkfixed" "$WORK/direct.expected"
    sed '1d' "$UPD" >>"$WORK/direct.expected"
    if cmp -s "$WORK/direct.expected" "$DIRECT_UPD"; then
        ok "direct 1.0--1.2 script is the frozen hop with only its native guard retargeted and its shared-helper validator line fixed, followed by the headerless 1.1--1.2 body"
    else
        fail "direct 1.0--1.2 script differs from the exact approved composition"
    fi
    body=$(sed -e 's/--.*$//' "$DIRECT_UPD")
    if printf '%s\n' "$body" | grep -qiE '^[[:space:]]*(BEGIN|COMMIT)[[:space:]]*;'; then fail "direct update script contains BEGIN/COMMIT"; else ok "direct update script has no BEGIN/COMMIT"; fi
    if printf '%s\n' "$body" | grep -qiw 'CASCADE'; then fail "direct update script uses CASCADE (never cascade into user objects)"; else ok "direct update script uses no CASCADE"; fi
    if printf '%s\n' "$body" | grep -qiE 'session_replication_role|DISABLE[[:space:]]+EVENT|ALTER[[:space:]]+EVENT[[:space:]]+TRIGGER[^;]*DISABLE'; then
        fail "direct update script disables event triggers or changes session_replication_role"
    else ok "direct update script keeps event triggers on"; fi
fi
if [ "$BASELINE_VERSION" = 1.0 ]; then
    EXPECTED_PATH='1.0--1.2'
else
    EXPECTED_PATH='1.1--1.2'
fi
ACTUAL_PATH=$(q postgres "SET extension_control_path = '$EXT_ROOT:\$system'; SELECT path FROM pg_extension_update_paths('sql_saga') WHERE source = '$BASELINE_VERSION' AND target = '$NEW_VERSION';")
check "PostgreSQL chooses the approved $BASELINE_VERSION to $NEW_VERSION update path" "$EXPECTED_PATH" "$ACTUAL_PATH"
if [ -f "$PREFLIGHT" ]; then ok "preflight script present ($PREFLIGHT)"; else fail "preflight script missing ($PREFLIGHT)"; fi

# Construct a baseline-specific non-release artifact. It installs normally but deliberately lacks a
# release-shape sentinel, so preflight must reject it. Setup failure is fatal rather than a skipped test.
cp "$BASELINE_BUILD/sql_saga--$BASELINE_VERSION.sql" "$FOREIGN_EXT/sql_saga--$BASELINE_VERSION.sql" || die "copy foreign baseline SQL"
cp "$BASELINE_BUILD/sql_saga.control" "$FOREIGN_EXT/" || die "copy foreign baseline control"
if [ "$BASELINE_VERSION" = 1.0 ]; then
    printf '\nDROP TABLE sql_saga.temporal_merge_cache CASCADE;\n' >>"$FOREIGN_EXT/sql_saga--$BASELINE_VERSION.sql"
else
    printf '\nDROP TABLE sql_saga.installation CASCADE;\n' >>"$FOREIGN_EXT/sql_saga--$BASELINE_VERSION.sql"
fi
FOREIGN_SQL="$FOREIGN_EXT/sql_saga--$BASELINE_VERSION.sql"
[ -s "$FOREIGN_SQL" ] || die "building non-release $BASELINE_VERSION artifact"

# ---------------------------------------------------------------------------------------------
section "roles and fixtures"
# ---------------------------------------------------------------------------------------------
new_role "${PREFIX}_owner"
new_role "${PREFIX}_r1"
new_role "${PREFIX}_regular"
new_role "${PREFIX}_admin" "IN ROLE \"${PREFIX}_regular\""
new_role "${PREFIX}_none"
new_role "${PREFIX}_blind"

OLD=${PREFIX}_old
new_db "$OLD" "$EXT_ROOT" "$OLD_LIB"
if [ "$BASELINE_VERSION" = 1.0 ]; then
    { fixture_core "$BASELINE_VERSION"; fixture_old_only; } | sql "$OLD" >"$WORK/old.fixture.log"
else
    fixture_core "$BASELINE_VERSION" | sql "$OLD" >"$WORK/old.fixture.log"
fi
grep -q '^ERROR' "$WORK/old.fixture.log" && die "$BASELINE_VERSION fixture failed: $(grep -m1 '^ERROR' "$WORK/old.fixture.log") (see $WORK/old.fixture.log)"
check "fixture database runs extension version $BASELINE_VERSION" "$BASELINE_VERSION" "$(q "$OLD" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
check "fixture database installed the $BASELINE_TAG script (not the server's)" "$EXT_ROOT:\$system" "$(q "$OLD" "SHOW extension_control_path")"
# The 1.0 C library maps history positionally: a history-only column makes it refuse the write.
# The 1.1 library maps by name. This tells which library the fixture sessions loaded.
# Probed in a sibling database with the same settings (separate committed transactions: the writer
# skips rows inserted in the same transaction), so the fixture database stays untouched.
writer_probe() {  # writer_probe <libdir or empty>
    local p=${PREFIX}_libprobe r
    new_db "$p" "$EXT_ROOT" "$1"
    r=$(sql "$p" <<SQL | grep -m1 -E 'ERROR|by-name'
SET client_min_messages = warning;
CREATE EXTENSION btree_gist;
CREATE EXTENSION sql_saga VERSION '$BASELINE_VERSION';
CREATE TABLE lib_probe (id int PRIMARY KEY, x text);
SELECT sql_saga.add_system_versioning('lib_probe');
INSERT INTO lib_probe VALUES (1, 'a');
ALTER TABLE lib_probe_history ADD COLUMN extra int;
UPDATE lib_probe SET x = 'b';
SELECT 'by-name writer';
SQL
)
    dropdb --if-exists "$p" >>"$LOG" 2>&1
    echo "$r"
}
if [ "$BASELINE_VERSION" = 1.0 ]; then
    if [ -n "$OLD_LIB" ]; then
        check "fixture sessions run the $BASELINE_TAG C library (positional writer)" "ERROR:  could not convert row type" "$(writer_probe "$OLD_LIB")"
    fi
    # Prove this is a genuine 1.0 install before anything is upgraded.
    check "1.0 baseline: shared cache holds rows" "t" "$(q "$OLD" "SELECT count(*) > 0 FROM sql_saga.temporal_merge_cache")"
    check "1.0 baseline: 8 cache routines and the cache event trigger exist" "8|1" \
        "$(q "$OLD" "SELECT (SELECT count(*) FROM pg_proc WHERE pronamespace = 'sql_saga'::regnamespace AND proname ~ '^temporal_merge_(cache|source_columns_hash|invalidate_cache)') || '|' || (SELECT count(*) FROM pg_event_trigger WHERE evtname = 'sql_saga_cache_invalidation')")"
    check "1.0 baseline: no installation token, no gated views" "true|0" \
        "$(q "$OLD" "SELECT (to_regclass('sql_saga.installation') IS NULL) || '|' || (SELECT count(*) FROM pg_class WHERE relname LIKE '%\_gated')")"
    check "1.0 baseline: base grants mirrored onto with-history, FPO view and as-of function" "true|true|true" \
        "$(q "$OLD" "SELECT (SELECT relacl::text LIKE '%${PREFIX}_r1=%' FROM pg_class WHERE oid = 'sv_with_history'::regclass)::text
                     || '|' || (SELECT relacl::text LIKE '%${PREFIX}_r1=%' FROM pg_class WHERE oid = 'fp__for_portion_of_valid'::regclass)
                     || '|' || (SELECT proacl::text LIKE '%${PREFIX}_r1=%' FROM pg_proc WHERE oid = 'sv__as_of(timestamptz)'::regprocedure)")"
    check "1.0 baseline: owner-rights with-history leaks other tenants' history to r1" "1,1,2,2" "$(ids_as "$OLD" ${PREFIX}_r1 rdoc_with_history)"
else
    if [ -n "$OLD_LIB" ]; then
        check "fixture sessions run the $BASELINE_TAG C library (by-name writer)" "by-name writer" "$(writer_probe "$OLD_LIB")"
    fi
    check "1.1 baseline: installation token and gated views exist" "false|6" \
        "$(q "$OLD" "SELECT (to_regclass('sql_saga.installation') IS NULL) || '|' || (SELECT count(*) FROM pg_class WHERE relname LIKE '%\_gated')")"
    check "1.1 baseline: shared cache is absent" "t" "$(q "$OLD" "SELECT to_regclass('sql_saga.temporal_merge_cache') IS NULL")"
fi
# The "restart": every later session loads the new binaries.
psql -X -q -d postgres -c "ALTER DATABASE $OLD RESET dynamic_library_path" >>"$LOG" 2>&1
if [ -n "$OLD_LIB" ]; then
    check "after the restart the installed candidate C library serves the $BASELINE_VERSION SQL" "by-name writer" "$(writer_probe "")"
fi
OLD_DIGEST=$(acl_digest "$OLD")

# Anchor generated query functions to the released 1.1 state. The direct v1.1 path captures the
# pristine fixture. The v1.0 path uses a sibling clone upgraded only through immutable 1.0--1.1.
if [ "$BASELINE_VERSION" = 1.1 ]; then
    QUERY_ANCHOR="$OLD"
else
    QUERY_ANCHOR=${PREFIX}_query_anchor_11
    createdb -T "$OLD" "$QUERY_ANCHOR" >>"$LOG" 2>&1 || die "createdb $QUERY_ANCHOR from $OLD"
    CREATED_DBS+=("$QUERY_ANCHOR")
    psql -X -q -d postgres -c "ALTER DATABASE $QUERY_ANCHOR SET extension_control_path = '$EXT_ROOT:\$system'" >>"$LOG" 2>&1 || die "extension_control_path on $QUERY_ANCHOR"
    [ -n "$V11_LIB" ] || die "released v1.1 libraries were not built for the query-function anchor"
    psql -X -q -d postgres -c "ALTER DATABASE $QUERY_ANCHOR SET dynamic_library_path = '$V11_LIB:\$libdir'" >>"$LOG" 2>&1 || die "v1.1 dynamic_library_path on $QUERY_ANCHOR"
    out=$(q "$QUERY_ANCHOR" "ALTER EXTENSION sql_saga UPDATE TO '1.1'")
    printf '%s\n' "$out" | grep -q '^ERROR' && die "creating released 1.1 query-function anchor: $out"
    psql -X -q -d postgres -c "ALTER DATABASE $QUERY_ANCHOR RESET dynamic_library_path" >>"$LOG" 2>&1 || die "reset dynamic_library_path on $QUERY_ANCHOR"

    HOP_REFUSAL=${PREFIX}_explicit_11_with_12
    createdb -T "$OLD" "$HOP_REFUSAL" >>"$LOG" 2>&1 || die "createdb $HOP_REFUSAL from $OLD"
    CREATED_DBS+=("$HOP_REFUSAL")
    psql -X -q -d postgres -c "ALTER DATABASE $HOP_REFUSAL SET extension_control_path = '$EXT_ROOT:\$system'" >>"$LOG" 2>&1 || die "extension_control_path on $HOP_REFUSAL"
    out=$(q "$HOP_REFUSAL" "ALTER EXTENSION sql_saga UPDATE TO '1.1'")
    check "explicit 1.0 to 1.1 hop with the 1.2 libraries is refused by the frozen native guard" \
        "ERROR:  sql_saga: native library version 1.2 does not match extension update version 1.1" "$out"
    check "refused explicit intermediate hop leaves extversion 1.0" "1.0" \
        "$(q "$HOP_REFUSAL" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
fi
query_function_snapshot "$QUERY_ANCHOR" >"$WORK/query_functions.before"
if grep -q '^ERROR' "$WORK/query_functions.before"; then
    die "capturing released 1.1 query functions: $(grep -m1 '^ERROR' "$WORK/query_functions.before")"
fi
EXPECTED_QUERY_LINES=$(q "$QUERY_ANCHOR" "SELECT count(*) * 8 FROM sql_saga.system_versioning")
check "released 1.1 query snapshot covers four functions and plans for every family" "$EXPECTED_QUERY_LINES" "$(wc -l <"$WORK/query_functions.before" | tr -d ' ')"

# Clone the genuine 1.0 fixture, then replace its generated helper with a body that matched the old
# permissive validator. Exact validation must reject it before any update mutation.
TAMPER=${PREFIX}_tampered_helper
createdb -T "$OLD" "$TAMPER" >>"$LOG" 2>&1 || die "createdb $TAMPER from $OLD"
CREATED_DBS+=("$TAMPER")
psql -X -q -d postgres -c "ALTER DATABASE $TAMPER SET extension_control_path = '$EXT_ROOT:\$system'" >>"$LOG" 2>&1 || die "extension_control_path on $TAMPER"
q "$TAMPER" "CREATE OR REPLACE FUNCTION public.par_id_exists(integer) RETURNS boolean LANGUAGE sql STABLE STRICT AS 'SELECT EXISTS (SELECT 1 FROM public.par AS uk WHERE uk.id = \$1) OR true;'" >/dev/null

# A released 1.1 database must not carry the old 1.0 helper shape. Recreate that exact body and
# properties on a clone while preserving the helper signature and owner. The 1.1--1.2 update must
# require the canonical released-1.1 SECURITY DEFINER helper instead of accepting this downgrade.
DOWNLEVEL=""
if [ "$BASELINE_VERSION" = 1.1 ]; then
    DOWNLEVEL=${PREFIX}_downlevel_helper
    createdb -T "$OLD" "$DOWNLEVEL" >>"$LOG" 2>&1 || die "createdb $DOWNLEVEL from $OLD"
    CREATED_DBS+=("$DOWNLEVEL")
    psql -X -q -d postgres -c "ALTER DATABASE $DOWNLEVEL SET extension_control_path = '$EXT_ROOT:\$system'" >>"$LOG" 2>&1 || die "extension_control_path on $DOWNLEVEL"
    out=$(sql "$DOWNLEVEL" <<'SQL'
CREATE OR REPLACE FUNCTION public.par_id_exists(integer) RETURNS boolean
LANGUAGE sql STABLE STRICT
AS $v1_body$
                SELECT EXISTS (SELECT 1 FROM public.par AS uk WHERE uk.id = $1);
            $v1_body$;
ALTER FUNCTION public.par_id_exists(integer) SECURITY INVOKER;
ALTER FUNCTION public.par_id_exists(integer) RESET ALL;
SELECT (prosrc = $expected_v1_body$
                SELECT EXISTS (SELECT 1 FROM public.par AS uk WHERE uk.id = $1);
            $expected_v1_body$)::text || '|' || prosecdef || '|' || coalesce(array_to_string(proconfig, ','), 'NULL')
FROM pg_proc WHERE oid = 'public.par_id_exists(integer)'::regprocedure;
SQL
)
    check "damaged 1.1 fixture has the exact released-v1.0 helper body and properties" \
        "true|false|NULL" "$out"
fi

# ---------------------------------------------------------------------------------------------
section "preflight"
# ---------------------------------------------------------------------------------------------
HP=${PREFIX}_histpol
new_db "$HP" "$EXT_ROOT"
fixture_histpol | sql "$HP" >"$WORK/histpol.fixture.log"
grep -q '^ERROR' "$WORK/histpol.fixture.log" && die "history-policy fixture failed: $(grep -m1 '^ERROR' "$WORK/histpol.fixture.log")"
FOREIGN=${PREFIX}_foreign
new_db "$FOREIGN" "$FOREIGN_ROOT"
printf "SET client_min_messages = warning;\nCREATE EXTENSION btree_gist;\nCREATE EXTENSION sql_saga VERSION '%s';\n" "$BASELINE_VERSION" | sql "$FOREIGN" >"$WORK/foreign.fixture.log"
grep -q '^ERROR' "$WORK/foreign.fixture.log" && die "foreign $BASELINE_VERSION fixture failed: $(grep -m1 '^ERROR' "$WORK/foreign.fixture.log")"
if [ -f "$PREFLIGHT" ]; then
    before="$(ext_snapshot "$OLD" | md5)$(acl_digest "$OLD")"
    if out=$(run_preflight "$OLD"); then ok "preflight passes on the $BASELINE_VERSION fixture database"; else fail "preflight refuses the $BASELINE_VERSION fixture database: $(printf '%s\n' "$out" | grep -m1 ERROR)"; fi
    printf '%s\n' "$out" >"$WORK/preflight.old.out"
    if [ "$BASELINE_VERSION" = 1.0 ]; then
        if printf '%s\n' "$out" | grep -q "${PREFIX}_none"; then ok "preflight reports the derived grants that normalization removes"
        else fail "preflight does not report the direct derived grants to ${PREFIX}_none"; fi
    elif printf '%s\n' "$out" | grep -q 'derived grant'; then
        fail "preflight reports unexpected derived grants on the pristine v1.1 fixture"
    else
        ok "preflight accepts pristine v1.1 without derived-grant warnings"
    fi
    check "preflight is read-only" "$before" "$(ext_snapshot "$OLD" | md5)$(acl_digest "$OLD")"
    if out=$(run_preflight "$HP"); then fail "preflight passes despite a user policy on hp_history"
    elif printf '%s\n' "$out" | grep -q user_history_policy; then ok "preflight stops on the user policy on hp_history and names it"
    else fail "preflight stops on hp, but does not name user_history_policy"; fi
    if out=$(run_preflight "$FOREIGN"); then fail "preflight accepts a non-release $BASELINE_VERSION artifact"
    else ok "preflight refuses a non-release $BASELINE_VERSION artifact"; fi
else
    fail "preflight not run: script missing"
fi

# ---------------------------------------------------------------------------------------------
section "atomic failure (history user policy)" "not meaningful (dependent)"
# ---------------------------------------------------------------------------------------------
hp_before="$(ext_snapshot "$HP" | md5)$(dump_schema "$HP" | md5)"
out=$(q "$HP" "ALTER EXTENSION sql_saga UPDATE TO '$NEW_VERSION'")
case "$out" in
    *user_history_policy*) ok "update refuses the user policy on hp_history and names it" ;;
    ERROR*) fail "update fails, but not because of the user policy: $out" ;;
    *) fail "update succeeded despite a user policy on hp_history" ;;
esac
check "failed update leaves extversion $BASELINE_VERSION" "$BASELINE_VERSION" "$(q "$HP" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
check "failed update leaves catalog and family untouched" "$hp_before" "$(ext_snapshot "$HP" | md5)$(dump_schema "$HP" | md5)"
q "$HP" "DROP POLICY user_history_policy ON hp_history" >/dev/null
out=$(q "$HP" "ALTER EXTENSION sql_saga UPDATE TO '$NEW_VERSION'")
check "update succeeds once the user policy is removed" "$NEW_VERSION" "$(q "$HP" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"

# ---------------------------------------------------------------------------------------------
section "atomic failure (tampered regular-FK helper)" "not meaningful (dependent)"
# ---------------------------------------------------------------------------------------------
tamper_before="$(ext_snapshot "$TAMPER" | md5)$(dump_schema "$TAMPER" | md5)"
out=$(q "$TAMPER" "ALTER EXTENSION sql_saga UPDATE TO '$NEW_VERSION'")
case "$out" in
    *par_id_exists*exact*helper*) ok "update rejects the regex-matching helper body and names the function" ;;
    ERROR*) fail "update rejects the tampered helper, but without the actionable exact-helper error: $out" ;;
    *) fail "update succeeded despite the tampered regular-FK helper body" ;;
esac
check "tampered-helper failure leaves extversion $BASELINE_VERSION" "$BASELINE_VERSION" "$(q "$TAMPER" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
check "tampered-helper failure is atomic" "$tamper_before" "$(ext_snapshot "$TAMPER" | md5)$(dump_schema "$TAMPER" | md5)"
check "tampered helper body remains untouched after refusal" "t" "$(q "$TAMPER" "SELECT prosrc LIKE '%OR true%' FROM pg_proc WHERE oid='public.par_id_exists(integer)'::regprocedure")"

if [ "$BASELINE_VERSION" = 1.1 ]; then
    section "atomic failure (released-v1.0 helper shape on v1.1)" "expected RED"
    downlevel_before="$(ext_snapshot "$DOWNLEVEL" | md5)$(dump_schema "$DOWNLEVEL" | md5)"
    out=$(q "$DOWNLEVEL" "ALTER EXTENSION sql_saga UPDATE TO '$NEW_VERSION'")
    case "$out" in
        *par_id_exists*canonical*1.1*helper*) ok "v1.1 update rejects the exact released-v1.0 helper shape with the canonical-1.1 message" ;;
        ERROR*) fail "v1.1 update rejects the released-v1.0 helper shape, but without the canonical-1.1 message: $out" ;;
        *) fail "v1.1 update accepted the exact released-v1.0 helper shape" ;;
    esac
    check "downlevel-helper refusal leaves extversion 1.1" "1.1" \
        "$(q "$DOWNLEVEL" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
    check "downlevel-helper refusal is atomic" "$downlevel_before" \
        "$(ext_snapshot "$DOWNLEVEL" | md5)$(dump_schema "$DOWNLEVEL" | md5)"
fi

# ---------------------------------------------------------------------------------------------
section "atomic failure (damaged family, complete final integrity gate)" "expected RED"
# ---------------------------------------------------------------------------------------------
# Design 6.2 item 7: the update ends with the complete check_integrity() gate, after every definition
# and privilege change, inside the update transaction. Each case clones the fixture, applies one
# damage that the legacy (pre-1.2) drop-protection checks accept, and requires that the update fails
# with exactly the message check_integrity() gives for it, leaves extversion and every prior object
# untouched, and then succeeds once the damage is repaired the way that message says.
# 1.0 path: query functions, history RLS and policy copies are rebuilt by the frozen 1.0--1.1 hop
# (1.0 has no copies), so that damage is reconciled, not refused. Era synchronization objects are
# carried over as they are on both paths.
# Contents of every sql_saga configuration/catalog table that is an extension member, whatever the
# baseline has (1.0: temporal_merge_cache, no installation; 1.1: installation). Rows are rendered as
# jsonb, sorted; reg* columns render by name. Nothing is masked, the installation token included: the
# fingerprint compares one database before and after a refused update, which must not change it.
catalog_rows_sql() {
    cat <<'SQL'
DO $catalog_rows$
DECLARE
    r record;
    line text;
BEGIN
    CREATE TEMP TABLE pg_temp.catalog_rows (line text) ON COMMIT PRESERVE ROWS;
    FOR r IN
        SELECT c.oid::regclass AS rel, c.relname
        FROM pg_class AS c
        JOIN pg_depend AS d ON (d.classid, d.objid, d.deptype) = ('pg_class'::regclass, c.oid, 'e')
        JOIN pg_extension AS x ON x.oid = d.refobjid AND x.extname = 'sql_saga'
        WHERE c.relnamespace = 'sql_saga'::regnamespace AND c.relkind IN ('r', 'p')
        ORDER BY c.relname COLLATE "C"
    LOOP
        EXECUTE format($q$
            INSERT INTO pg_temp.catalog_rows (line)
            SELECT %L || '|' || to_jsonb(t)::text
            FROM %s AS t
            UNION ALL SELECT %L || '|rows=' || count(*) FROM %s
        $q$, r.relname, r.rel, r.relname, r.rel);
    END LOOP;
END;
$catalog_rows$;
SELECT line FROM pg_temp.catalog_rows ORDER BY line COLLATE "C";
SQL
}
# fingerprint_part <label> <cmd...>: run a capture, print its md5. Any nonzero status, stderr output,
# ERROR line or empty stream is a capture failure: print a token that never equals a hash, return 1.
fingerprint_part() {
    local label=$1 out err status
    shift
    err=$(mktemp "$WORK/fp.XXXXXX")
    # In this subshell only, sql() keeps psql's own exit status and stderr instead of folding errors
    # into stdout, so ext_snapshot/query_function_snapshot failures are visible as failures.
    out=$(sql() { local db=$1; shift; psql -X -At -q -v ON_ERROR_STOP=1 -d "$db" "$@"; }
          "$@" 2>"$err"; echo "status=$?")
    status=${out##*status=}
    out=${out%status=*}
    if [ "$status" != 0 ] || [ -s "$err" ] || [ -z "$out" ] || printf '%s\n' "$out" | grep -q -E '^(ERROR|FATAL)'; then
        echo "CAPTURE-FAILED:$label:$status:$(head -c 200 "$err")$(printf '%s\n' "$out" | grep -m1 -E '^(ERROR|FATAL)')"
        rm -f "$err"
        return 1
    fi
    rm -f "$err"
    printf '%s' "$out" | md5
}
raw_dump_schema() { pg_dump -s -d "$1" | grep -v -E '^-- Dumped (from|by)|^\\(un)?restrict '; return "${PIPESTATUS[0]}"; }
raw_catalog_rows() { catalog_rows_sql | psql -X -At -q -v ON_ERROR_STOP=1 -d "$1"; }
damage_state() {  # damage_state <db>: extension objects + user dump + query functions + catalog rows
    local parts="" p rc=0
    p=$(fingerprint_part ext ext_snapshot "$1") || rc=1; parts="$p"
    p=$(fingerprint_part dump raw_dump_schema "$1") || rc=1; parts="$parts|$p"
    p=$(fingerprint_part query_functions query_function_snapshot "$1") || rc=1; parts="$parts|$p"
    p=$(fingerprint_part catalog_rows raw_catalog_rows "$1") || rc=1; parts="$parts|$p"
    echo "$parts"
    return $rc
}
damage_case() {  # damage_case <label> <damage sql> <exact checker message> <repair sql>
    local label=$1 damage=$2 want=$3 repair=$4 db=${PREFIX}_damage_$1 out before
    createdb -T "$OLD" "$db" >>"$LOG" 2>&1 || die "createdb $db from $OLD"
    CREATED_DBS+=("$db")
    psql -X -q -d postgres -c "ALTER DATABASE $db SET extension_control_path = '$EXT_ROOT:\$system'" >>"$LOG" 2>&1 || die "extension_control_path on $db"
    out=$(printf '%s\n' "$damage" | sql "$db")
    printf '%s\n' "$out" | grep -q '^ERROR' && die "damage fixture $label: $(printf '%s\n' "$out" | grep -m1 '^ERROR')"
    before=$(damage_state "$db") || die "damage fixture $label: prior-state capture failed: $before"
    out=$(q "$db" "ALTER EXTENSION sql_saga UPDATE TO '$NEW_VERSION'")
    check "$label: update refused with check_integrity's exact message" "ERROR:  $want" "$out"
    check "$label: refusal leaves extversion $BASELINE_VERSION" "$BASELINE_VERSION" "$(q "$db" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
    check "$label: refusal leaves extension objects, user schema, query functions and catalog rows unchanged" "$before" "$(damage_state "$db")"
    out=$(printf 'SET client_min_messages = warning;\n%s\n' "$repair" | sql "$db")
    check "$label: the repair the message names succeeds on $BASELINE_VERSION" "" "$(printf '%s\n' "$out" | grep -m1 '^ERROR')"
    out=$(q "$db" "ALTER EXTENSION sql_saga UPDATE TO '$NEW_VERSION'")
    check "$label: update succeeds after the repair" "" "$(printf '%s\n' "$out" | grep -m1 '^ERROR')"
    check "$label: repaired installation reaches $NEW_VERSION" "$NEW_VERSION" "$(q "$db" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
    assert_integrity "$db"
}
# Era with synchronized columns and no dependents, so drop_era/add_era is the plain repair.
DMG_ERA="SET client_min_messages = warning;
CREATE TABLE dmg (id int, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('dmg', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');"
damage_case sync_trigger \
    "$DMG_ERA
SET session_replication_role = replica;
SELECT format('DROP TRIGGER %I ON dmg', sync_temporal_trg_name) FROM sql_saga.era WHERE table_name = 'dmg' \\gexec
RESET session_replication_role;" \
    'sql_saga: integrity violation for era "valid" on table "public.dmg": named check or synchronization object is noncanonical; repair with drop_era and add_era' \
    "SELECT sql_saga.drop_era('dmg', 'valid');
SELECT sql_saga.add_era('dmg', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');"
if [ "$BASELINE_VERSION" = 1.1 ]; then
    damage_case volatile_as_of \
        "ALTER FUNCTION sv__as_of(timestamptz) VOLATILE;" \
        'sql_saga: integrity violation for system-versioned table "public.sv": generated query function role "as_of" definition is noncanonical; repair with rebuild_system_versioning_views' \
        "SELECT sql_saga.rebuild_system_versioning_views('sv');"
    damage_case missing_policy_copy \
        "SET session_replication_role = replica;
SELECT format('DROP POLICY %I ON rdoc_history', 'sql_saga_' || left(md5('tenant_a' || token), 16)) FROM sql_saga.installation \\gexec
RESET session_replication_role;" \
        'sql_saga: integrity violation for system-versioned table "public.rdoc": managed history policy set is inconsistent; repair with sync_history_policies' \
        "SELECT sql_saga.sync_history_policies('rdoc');"
fi

# ---------------------------------------------------------------------------------------------
section "custom privileges on add_system_versioning" "expected RED"
# ---------------------------------------------------------------------------------------------
# 1.2 adds a trailing parameter to add_system_versioning, so the update replaces the routine. Privileges
# changed on the 1.1 routine cannot be carried into an extension script without becoming extension
# initial privileges, which pg_dump omits. The update refuses instead, before any change, and its hint
# gives the exact statements: reset, update, re-apply on the new signature (as user ACL deltas).
ASV7="sql_saga.add_system_versioning(regclass, name, name, name, name, name, name)"
ASV8="sql_saga.add_system_versioning(regclass, name, name, name, name, name, name, boolean)"
ASV7_DUMP="sql_saga.add_system_versioning(table_oid regclass, history_table_name name, view_name name, function_as_of_name name, function_between_name name, function_between_symmetric_name name, function_from_to_name name)"
ASV8_DUMP="sql_saga.add_system_versioning(table_oid regclass, history_table_name name, view_name name, function_as_of_name name, function_between_name name, function_between_symmetric_name name, function_from_to_name name, history_table_must_exist boolean)"
ASV_OWNER=$(psql -X -At -d postgres -c 'SELECT current_user::regrole::text' 2>>"$LOG")
asv_acl() {  # asv_acl <db> <regprocedure>: effective EXECUTE grantees, sorted, grantor ignored
    q "$1" "SELECT coalesce(string_agg(g, ',' ORDER BY g COLLATE \"C\"), '') FROM (SELECT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END || CASE WHEN a.is_grantable THEN '*' ELSE '' END AS g FROM pg_proc AS p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a WHERE p.oid = '$2'::regprocedure AND a.privilege_type = 'EXECUTE') AS e"
}
asv_case() {  # asv_case <label> <setup sql> <expected error> <expected hint> <expected acl after> <expected dump lines>
    local label=$1 setup=$2 want_err=$3 want_hint=$4 want_acl=$5 want_dump=$6 db=${PREFIX}_acl_$1 out before err hint
    new_db "$db" "$EXT_ROOT"
    out=$(printf "SET client_min_messages = warning;\nCREATE EXTENSION btree_gist;\nCREATE EXTENSION sql_saga VERSION '%s';\n%s\n" "$BASELINE_VERSION" "$setup" | sql "$db")
    printf '%s\n' "$out" | grep -q '^ERROR' && die "acl fixture $label: $(printf '%s\n' "$out" | grep -m1 '^ERROR')"
    before="$(ext_snapshot "$db" | md5)$(dump_schema "$db" | md5)"
    out=$(printf "\\\\set VERBOSITY default\nALTER EXTENSION sql_saga UPDATE TO '%s';\n" "$NEW_VERSION" | psql -X -q -d "$db" 2>&1 | sed -e 's/^psql:[^ ]*:[0-9]*: //')
    err=$(printf '%s\n' "$out" | grep -m1 '^ERROR:' | sed 's/^ERROR:  *//')
    hint=$(printf '%s\n' "$out" | grep -m1 '^HINT:' | sed 's/^HINT:  *//')
    check "$label: update refused with the exact error" "$want_err" "$err"
    check "$label: hint gives the exact way out" "$want_hint" "$hint"
    check "$label: refusal leaves extversion $BASELINE_VERSION" "$BASELINE_VERSION" "$(q "$db" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
    check "$label: refusal is atomic" "$before" "$(ext_snapshot "$db" | md5)$(dump_schema "$db" | md5)"
    # Follow the hint the update actually returned, literally, as one transaction.
    out=$(printf 'BEGIN;\n%s\nCOMMIT;\n' "$(printf '%s\n' "$hint" | sed -e 's/^.*Run: //')" | sql "$db")
    check "$label: following the hint succeeds" "" "$(printf '%s\n' "$out" | grep -m1 '^ERROR')"
    check "$label: following the hint reaches $NEW_VERSION" "$NEW_VERSION" "$(q "$db" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
    check "$label: the customisation is re-applied to the new signature" "$want_acl" "$(asv_acl "$db" "$ASV8")"
    check "$label: the re-applied customisation is a user delta, not an initial privilege" "0" \
        "$(q "$db" "SELECT count(*) FROM pg_init_privs WHERE objoid = '$ASV8'::regprocedure AND classoid = 'pg_proc'::regclass")"
    check "$label: pg_dump repeats the re-applied customisation" "$want_dump" \
        "$(dump_schema "$db" | grep -E "^(GRANT|REVOKE) .* ON FUNCTION sql_saga\.add_system_versioning\(" | paste -sd'|' -)"
}
if [ "$BASELINE_VERSION" = 1.1 ]; then
    new_role "${PREFIX}_asv"
    # The hint resets the whole set, not only the grantees that differ: a REVOKE ... CASCADE also removes
    # grants made through a grant option, so every non-owner grantee is revoked and the complete target set
    # granted again, before and after the update.
    ASV_ERR="sql_saga: the privileges on $ASV7 differ from those its 1.2 replacement gets; the update replaces this function, and cannot carry changed privileges over"
    ASV_HINT="Reset them, update, and apply them to the new signature after the update. Run:"
    asv_case revoke_public \
        "REVOKE EXECUTE ON FUNCTION $ASV7 FROM PUBLIC;" \
        "$ASV_ERR" \
        "$ASV_HINT GRANT EXECUTE ON FUNCTION $ASV7 TO PUBLIC; ALTER EXTENSION sql_saga UPDATE TO '1.2'; REVOKE ALL ON FUNCTION $ASV8 FROM PUBLIC CASCADE;" \
        "$ASV_OWNER" \
        "REVOKE ALL ON FUNCTION $ASV8_DUMP FROM PUBLIC;"
    asv_case grant_option \
        "GRANT EXECUTE ON FUNCTION $ASV7 TO ${PREFIX}_asv WITH GRANT OPTION;" \
        "$ASV_ERR" \
        "$ASV_HINT REVOKE ALL ON FUNCTION $ASV7 FROM PUBLIC, ${PREFIX}_asv CASCADE; GRANT EXECUTE ON FUNCTION $ASV7 TO PUBLIC; ALTER EXTENSION sql_saga UPDATE TO '1.2'; REVOKE ALL ON FUNCTION $ASV8 FROM PUBLIC CASCADE; GRANT EXECUTE ON FUNCTION $ASV8 TO PUBLIC; GRANT EXECUTE ON FUNCTION $ASV8 TO ${PREFIX}_asv WITH GRANT OPTION;" \
        "PUBLIC,$ASV_OWNER,${PREFIX}_asv*" \
        "GRANT ALL ON FUNCTION $ASV8_DUMP TO ${PREFIX}_asv WITH GRANT OPTION;"
    # Default privileges changed since 1.1: the replacement would gain ${PREFIX}_asv and lose nothing the
    # site removed. Refused as well, so the new routine never silently gets broader access.
    asv_case new_default \
        "REVOKE EXECUTE ON FUNCTION $ASV7 FROM PUBLIC;
ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO ${PREFIX}_asv;" \
        "$ASV_ERR" \
        "$ASV_HINT GRANT EXECUTE ON FUNCTION $ASV7 TO PUBLIC; GRANT EXECUTE ON FUNCTION $ASV7 TO ${PREFIX}_asv; ALTER EXTENSION sql_saga UPDATE TO '1.2'; REVOKE ALL ON FUNCTION $ASV8 FROM PUBLIC, ${PREFIX}_asv CASCADE;" \
        "$ASV_OWNER" \
        "REVOKE ALL ON FUNCTION $ASV8_DUMP FROM PUBLIC;"
    # Delegation plus changed defaults (G4 R2.1): A holds EXECUTE WITH GRANT OPTION and granted it to B;
    # default privileges now give A and B plain EXECUTE. Only A differs, but revoking A's grant option
    # cascades to B's delegated grant, so the hint must restore B as well. A's name needs quoting.
    ASV_A_RAW="${PREFIX}_dele gator's"
    ASV_A="\"${PREFIX}_dele gator's\""
    ASV_B=${PREFIX}_delegatee
    new_role "$ASV_A_RAW"
    new_role "$ASV_B"
    asv_case delegated \
        "GRANT EXECUTE ON FUNCTION $ASV7 TO $ASV_A WITH GRANT OPTION;
SET ROLE $ASV_A;
GRANT EXECUTE ON FUNCTION $ASV7 TO $ASV_B;
RESET ROLE;
ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO $ASV_A, $ASV_B;" \
        "$ASV_ERR" \
        "$ASV_HINT REVOKE ALL ON FUNCTION $ASV7 FROM PUBLIC, $ASV_A, $ASV_B CASCADE; GRANT EXECUTE ON FUNCTION $ASV7 TO PUBLIC; GRANT EXECUTE ON FUNCTION $ASV7 TO $ASV_A; GRANT EXECUTE ON FUNCTION $ASV7 TO $ASV_B; ALTER EXTENSION sql_saga UPDATE TO '1.2'; REVOKE ALL ON FUNCTION $ASV8 FROM PUBLIC, $ASV_A, $ASV_B CASCADE; GRANT EXECUTE ON FUNCTION $ASV8 TO PUBLIC; GRANT EXECUTE ON FUNCTION $ASV8 TO $ASV_A WITH GRANT OPTION; GRANT EXECUTE ON FUNCTION $ASV8 TO $ASV_B;" \
        "$ASV_A*,PUBLIC,$ASV_OWNER,$ASV_B" \
        "GRANT ALL ON FUNCTION $ASV8_DUMP TO $ASV_A WITH GRANT OPTION;|GRANT ALL ON FUNCTION $ASV8_DUMP TO $ASV_B;"
    # Unmodified hardened installation ({owner=X} from default privileges): no refusal, and the new
    # routine gets what a fresh 1.2 install gets there.
    HARD=${PREFIX}_acl_hardened
    new_db "$HARD" "$EXT_ROOT"
    out=$(printf "SET client_min_messages = warning;\nALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;\nCREATE EXTENSION btree_gist;\nCREATE EXTENSION sql_saga VERSION '%s';\n" "$BASELINE_VERSION" | sql "$HARD")
    printf '%s\n' "$out" | grep -q '^ERROR' && die "hardened acl fixture: $(printf '%s\n' "$out" | grep -m1 '^ERROR')"
    check "hardened: 1.1 routine is owner-only" "$ASV_OWNER" "$(asv_acl "$HARD" "$ASV7")"
    check "hardened: unmodified installation updates without refusal" "" "$(q "$HARD" "ALTER EXTENSION sql_saga UPDATE TO '$NEW_VERSION'" | grep '^ERROR')"
    check "hardened: reaches $NEW_VERSION" "$NEW_VERSION" "$(q "$HARD" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
    check "hardened: new routine is owner-only, as in a fresh $NEW_VERSION install" "$ASV_OWNER" "$(asv_acl "$HARD" "$ASV8")"
    check "hardened: no leftover probe routine" "0" \
        "$(q "$HARD" "SELECT count(*) FROM pg_proc WHERE pronamespace = 'sql_saga'::regnamespace AND proname LIKE '%acl_probe%'")"
fi

# ---------------------------------------------------------------------------------------------
section "system time era NOTICE patch is fail-closed" "expected RED"
# ---------------------------------------------------------------------------------------------
# The 1.1--1.2 script patches the released __internal_add_system_time_era body textually
# (DO $upgrade_add_system_time_era_notices$). It must apply only to the exact released shape: every old
# anchor exactly once and no new NOTICE text yet, and afterwards each new NOTICE exactly once. Each case
# runs inside a transaction on the released 1.1 state ($QUERY_ANCHOR, before any update) and rolls back.
ERA_NOTICE_BLOCK="$WORK/era_notice_block.sql"
awk '/^DO \$upgrade_add_system_time_era_notices\$/ { on = 1 } on { print } on && /^\$upgrade_add_system_time_era_notices\$;/ { exit }' \
    "$EXT/sql_saga--1.1--1.2.sql" >"$ERA_NOTICE_BLOCK"
check "1.1--1.2 script has one system time era NOTICE patch block" "1" \
    "$(grep -c '^DO \$upgrade_add_system_time_era_notices\$' "$EXT/sql_saga--1.1--1.2.sql")"
ERA_FN="'sql_saga.__internal_add_system_time_era(regclass,name,name,name,name,name,name[])'::regprocedure"
ERA_NOTICE_COUNTS="SELECT (SELECT count(*) FROM regexp_matches(prosrc, 'RAISE NOTICE ''sql_saga: Added column', 'g'))
    || '|' || (SELECT count(*) FROM regexp_matches(prosrc, 'RAISE NOTICE ''sql_saga: Created constraint', 'g'))
    || '|' || (SELECT count(*) FROM regexp_matches(prosrc, 'RAISE NOTICE ''sql_saga: Created trigger', 'g'))
    FROM pg_catalog.pg_proc WHERE oid = $ERA_FN;"
# era_notice_case <label> <mutation sql run before the block, or empty>: prints the block outcome
era_notice_case() {
    { echo "BEGIN;"; printf '%s\n' "$2"; cat "$ERA_NOTICE_BLOCK"; echo "$ERA_NOTICE_COUNTS"; echo "ROLLBACK;"; } \
        | sql "$QUERY_ANCHOR" >"$WORK/era_notice.$1.log"
    if grep -q '^ERROR' "$WORK/era_notice.$1.log"; then grep -m1 '^ERROR' "$WORK/era_notice.$1.log"; else tail -n 1 "$WORK/era_notice.$1.log"; fi
}
# era_mutation <old> <new>: replace text in the live body exactly once (fails if <old> is not there once)
era_mutation() {
    printf '%s\n' "DO \$m\$ DECLARE d text := pg_catalog.pg_get_functiondef($ERA_FN); BEGIN
    IF (length(d) - length(replace(d, \$o\$$1\$o\$, ''))) / length(\$o\$$1\$o\$) <> 1 THEN RAISE EXCEPTION 'mutation anchor not found once'; END IF;
    EXECUTE replace(d, \$o\$$1\$o\$, \$n\$$2\$n\$); END \$m\$;"
}
era_before="$(q "$QUERY_ANCHOR" "SELECT md5(pg_catalog.pg_get_functiondef($ERA_FN))")"
ERA_TRUNC_LINE="    EXECUTE format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE PROCEDURE sql_saga.truncate_system_versioning()', truncate_trigger /* %I */, table_oid /* %s */);"
ERA_GEN_LINE="    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.generated_always_as_row_start_end(%L)', generated_always_trigger, table_oid, range_column_name);"
check "pristine released body: the patch adds each new NOTICE exactly once" "1|1|3" "$(era_notice_case pristine "")"
out=$(era_notice_case rerun "$(cat "$ERA_NOTICE_BLOCK")")
case "$out" in
    ERROR*__internal_add_system_time_era*) ok "already patched body: the patch refuses to apply twice" ;;
    ERROR*) fail "already patched body: refused, but not by the NOTICE patch guard: $out" ;;
    *) fail "already patched body: the patch applied again (NOTICE counts added|constraint|trigger: $out)" ;;
esac
for c in "truncate:$ERA_TRUNC_LINE" "generated:$ERA_GEN_LINE"; do
    label=${c%%:*}; line=${c#*:}
    # A comment line between the copies keeps each copy a complete anchor (anchors include their newlines).
    out=$(era_notice_case "dup_$label" "$(era_mutation "$line" "$line
    -- duplicated anchor
$line")")
    case "$out" in
        ERROR*mutation*) fail "duplicate $label anchor: harness could not build the mutation: $out" ;;
        ERROR*__internal_add_system_time_era*) ok "duplicate $label trigger anchor: the patch refuses" ;;
        ERROR*) fail "duplicate $label trigger anchor: refused, but not by the NOTICE patch guard: $out" ;;
        *) fail "duplicate $label trigger anchor: the patch applied (NOTICE counts added|constraint|trigger: $out)" ;;
    esac
done
out=$(era_notice_case missing_truncate "$(era_mutation "$ERA_TRUNC_LINE" "    EXECUTE format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE PROCEDURE sql_saga.truncate_system_versioning()', truncate_trigger, table_oid);")")
case "$out" in
    ERROR*mutation*) fail "missing truncate anchor: harness could not build the mutation: $out" ;;
    ERROR*__internal_add_system_time_era*) ok "missing truncate trigger anchor: the patch refuses" ;;
    *) fail "missing truncate trigger anchor: the patch did not refuse: $out" ;;
esac
out=$(era_notice_case new_text_present "$(era_mutation "$ERA_GEN_LINE" "$ERA_GEN_LINE
    RAISE NOTICE 'sql_saga: Created trigger \"%\" on table', generated_always_trigger;")")
case "$out" in
    ERROR*mutation*) fail "body already carrying a creation NOTICE: harness could not build the mutation: $out" ;;
    ERROR*__internal_add_system_time_era*) ok "body already carrying a creation NOTICE: the patch refuses" ;;
    *) fail "body already carrying a creation NOTICE: the patch applied (NOTICE counts added|constraint|trigger: $out)" ;;
esac
check "NOTICE patch probes roll back: released body untouched" "$era_before" \
    "$(q "$QUERY_ANCHOR" "SELECT md5(pg_catalog.pg_get_functiondef($ERA_FN))")"

# ---------------------------------------------------------------------------------------------
section "shared regular-FK helper (two regular FKs on one key)" "expected RED"
# ---------------------------------------------------------------------------------------------
# add_regular_foreign_key shares one helper function between all regular FKs referencing the same
# unique key. A healthy baseline database with two such FKs (single- and two-column keys) must update,
# keep the helper and both FKs enforcing, and pass the final integrity gate.
SHARED=${PREFIX}_shared_helper
new_db "$SHARED" "$EXT_ROOT" "$OLD_LIB"
out=$(sql "$SHARED" <<SQL
SET client_min_messages = warning;
CREATE EXTENSION btree_gist;
CREATE EXTENSION sql_saga VERSION '$BASELINE_VERSION';
CREATE TABLE par (id int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('par', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('par', ARRAY['id']::name[], 'valid', key_type => 'natural');
INSERT INTO par VALUES (1, '[2020-01-01,2030-01-01)'), (2, '[2020-01-01,2030-01-01)'), (3, '[2020-01-01,2030-01-01)');
CREATE TABLE c1 (id int PRIMARY KEY, par_id int);
CREATE TABLE c2 (id int PRIMARY KEY, owner_id int);
SELECT sql_saga.add_foreign_key('c1', ARRAY['par_id']::name[], 'par', ARRAY['id']::name[]);
SELECT sql_saga.add_foreign_key('c2', ARRAY['owner_id']::name[], 'par', ARRAY['id']::name[]);
CREATE TABLE par2 (a int, b text, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('par2', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('par2', ARRAY['a', 'b']::name[], 'valid', key_type => 'natural');
INSERT INTO par2 VALUES (1, 'x', '[2020-01-01,2030-01-01)'), (2, 'y', '[2020-01-01,2030-01-01)'), (3, 'z', '[2020-01-01,2030-01-01)');
CREATE TABLE d1 (id int PRIMARY KEY, a int, b text);
CREATE TABLE d2 (id int PRIMARY KEY, a int, b text);
SELECT sql_saga.add_foreign_key('d1', ARRAY['a', 'b']::name[], 'par2', ARRAY['a', 'b']::name[]);
SELECT sql_saga.add_foreign_key('d2', ARRAY['a', 'b']::name[], 'par2', ARRAY['a', 'b']::name[]);
-- Each child references its own parent key, so a surviving sibling FK cannot mask a broken one:
-- par 1 <- c1, par 2 <- c2, par2 (1,x) <- d1, par2 (2,y) <- d2; par 3 and par2 (3,z) stay unreferenced.
INSERT INTO c1 VALUES (1, 1);
INSERT INTO c2 VALUES (1, 2);
INSERT INTO d1 VALUES (1, 1, 'x');
INSERT INTO d2 VALUES (1, 2, 'y');
SQL
)
printf '%s\n' "$out" | grep -q '^ERROR' && die "shared helper fixture: $(printf '%s\n' "$out" | grep -m1 '^ERROR')"
check "shared helper fixture: two helpers serve four regular FKs" "2|4" \
    "$(q "$SHARED" "SELECT count(DISTINCT fk_helper_function) || '|' || count(*) FROM sql_saga.foreign_keys WHERE type = 'regular_to_temporal'")"
# The "restart": the update and every later session load the candidate binaries.
psql -X -q -d postgres -c "ALTER DATABASE $SHARED RESET dynamic_library_path" >>"$LOG" 2>&1 || die "reset dynamic_library_path on $SHARED"
check "shared helper: healthy $BASELINE_VERSION database updates without refusal" "" \
    "$(q "$SHARED" "ALTER EXTENSION sql_saga UPDATE TO '$NEW_VERSION'" | grep '^ERROR')"
check "shared helper: reaches $NEW_VERSION" "$NEW_VERSION" "$(q "$SHARED" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
check "shared helper: check_integrity passes after the update" "" "$(q "$SHARED" "SELECT sql_saga.check_integrity()" | grep '^ERROR')"
check "shared helper: after the update two helpers still serve four regular FKs" "2|4" \
    "$(q "$SHARED" "SELECT count(DISTINCT fk_helper_function) || '|' || count(*) FROM sql_saga.foreign_keys WHERE type = 'regular_to_temporal'")"
# Valid references are still accepted by every FK (rolled back).
check "shared helper: valid inserts into c1, c2, d1, d2 succeed after the update" "" \
    "$(q "$SHARED" "BEGIN; INSERT INTO c1 VALUES (2, 3); INSERT INTO c2 VALUES (2, 3); INSERT INTO d1 VALUES (2, 3, 'z'); INSERT INTO d2 VALUES (2, 3, 'z'); ROLLBACK" | grep '^ERROR')"
# Referencing side: one invalid insert per FK, each in its own session.
for probe in "c1|INSERT INTO c1 VALUES (9, 99)|c1_par_id_check" \
             "c2|INSERT INTO c2 VALUES (9, 99)|c2_owner_id_check" \
             "d1|INSERT INTO d1 VALUES (9, 9, 'q')|d1_a_b_check" \
             "d2|INSERT INTO d2 VALUES (9, 9, 'q')|d2_a_b_check"; do
    IFS='|' read -r child stmt con <<<"$probe"
    check "shared helper: $child rejects a missing parent after the update" \
        "ERROR:  new row for relation \"$child\" violates check constraint \"$con\"" \
        "$(q "$SHARED" "BEGIN; $stmt; ROLLBACK")"
done
# Referenced side: delete the parent row referenced only by that FK, each in its own session.
for probe in "c1|par|DELETE FROM par WHERE id = 1|c1_par_id_fkey" \
             "c2|par|DELETE FROM par WHERE id = 2|c2_owner_id_fkey" \
             "d1|par2|DELETE FROM par2 WHERE a = 1 AND b = 'x'|d1_a_b_fkey" \
             "d2|par2|DELETE FROM par2 WHERE a = 2 AND b = 'y'|d2_a_b_fkey"; do
    IFS='|' read -r child parent stmt con <<<"$probe"
    check "shared helper: deleting the parent referenced only by $child is refused after the update" \
        "ERROR:  update or delete on table \"public.$parent\" violates foreign key constraint \"$con\" on table \"public.$child\"" \
        "$(q "$SHARED" "BEGIN; $stmt; ROLLBACK")"
done
check "shared helper: unreferenced parents can be deleted after the update" "" \
    "$(q "$SHARED" "BEGIN; DELETE FROM par WHERE id = 3; DELETE FROM par2 WHERE a = 3; ROLLBACK" | grep '^ERROR')"

# ---------------------------------------------------------------------------------------------
section "StatBus shapes: XOR keys sorted by unrelated DDL, regular FK child sorting before its parent" "expected RED"
# ---------------------------------------------------------------------------------------------
# tmp/STATBUS_REPORT_xor_key_order.md and tmp/fable_audit_v13_brief.md (defects A and B). A healthy
# baseline database shaped like StatBus: XOR unique keys (add_unique_key with mutually_exclusive_columns)
# whose given column order is not alphabetical, then unrelated DDL, after which the baseline's
# rename_following has rewritten unique_keys.column_names sorted by name (the constraints are unchanged);
# and regular foreign keys whose child table sorts before its parent ("activity_note" before
# "legal_unit"), which the baseline enforces with a CHECK constraint reading the parent. The update must
# succeed, repair the catalog to the constraint order, pass check_integrity, keep every foreign key
# enforcing, and leave a database that serial and --single-transaction restores accept.
STATBUS=${PREFIX}_statbus_shapes
new_db "$STATBUS" "$EXT_ROOT" "$OLD_LIB"
out=$(sql "$STATBUS" <<SQL
SET client_min_messages = warning;
CREATE EXTENSION btree_gist;
CREATE EXTENSION sql_saga VERSION '$BASELINE_VERSION';
CREATE TABLE legal_unit (id int NOT NULL, name text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('legal_unit', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('legal_unit', ARRAY['id']::name[], 'valid', key_type => 'primary', unique_key_name => 'legal_unit_id_valid');
CREATE TABLE establishment (id int NOT NULL, name text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('establishment', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('establishment', ARRAY['id']::name[], 'valid', key_type => 'primary', unique_key_name => 'establishment_id_valid');
-- XOR keys in the five StatBus shapes (column order as StatBus gives it).
CREATE TABLE activity (id int NOT NULL, type text NOT NULL, legal_unit_id int, establishment_id int,
                       valid_range daterange NOT NULL, valid_from date, valid_until date,
                       CONSTRAINT activity_one_unit CHECK (num_nonnulls(establishment_id, legal_unit_id) = 1));
SELECT sql_saga.add_era('activity', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('activity', ARRAY['id']::name[], 'valid', key_type => 'primary', unique_key_name => 'activity_id_valid');
SELECT sql_saga.add_unique_key(table_oid => 'activity', key_type => 'natural', column_names => ARRAY['type', 'legal_unit_id', 'establishment_id'],
    mutually_exclusive_columns => ARRAY['legal_unit_id', 'establishment_id'], unique_key_name => 'activity_type_establishment_id_valid');
SELECT sql_saga.add_foreign_key('activity', ARRAY['establishment_id']::name[], 'establishment', ARRAY['id']::name[]);
SELECT sql_saga.add_foreign_key('activity', ARRAY['legal_unit_id']::name[], 'legal_unit', ARRAY['id']::name[]);
CREATE TABLE location (id int NOT NULL, type text NOT NULL, establishment_id int, legal_unit_id int,
                       valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('location', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'location', key_type => 'natural', column_names => ARRAY['type', 'establishment_id', 'legal_unit_id'],
    mutually_exclusive_columns => ARRAY['establishment_id', 'legal_unit_id'], unique_key_name => 'location_type_establishment_id_valid');
SELECT sql_saga.add_foreign_key('location', ARRAY['establishment_id']::name[], 'establishment', ARRAY['id']::name[]);
SELECT sql_saga.add_foreign_key('location', ARRAY['legal_unit_id']::name[], 'legal_unit', ARRAY['id']::name[]);
CREATE TABLE contact (id int NOT NULL, legal_unit_id int, establishment_id int, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('contact', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'contact', key_type => 'natural', column_names => ARRAY['legal_unit_id', 'establishment_id'],
    mutually_exclusive_columns => ARRAY['legal_unit_id', 'establishment_id'], unique_key_name => 'contact_natural_key_valid');
CREATE TABLE person_for_unit (id int NOT NULL, person_id int NOT NULL, person_role_id int, legal_unit_id int, establishment_id int,
                              valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('person_for_unit', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'person_for_unit', key_type => 'natural',
    column_names => ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id'],
    mutually_exclusive_columns => ARRAY['legal_unit_id', 'establishment_id'], unique_key_name => 'person_for_unit_natural_key_valid');
CREATE TABLE stat_for_unit (id int NOT NULL, stat_definition_id int NOT NULL, legal_unit_id int, establishment_id int,
                            valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('stat_for_unit', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'stat_for_unit', key_type => 'natural',
    column_names => ARRAY['stat_definition_id', 'legal_unit_id', 'establishment_id'],
    mutually_exclusive_columns => ARRAY['establishment_id', 'legal_unit_id'], unique_key_name => 'stat_for_unit_natural_key_valid');
-- Regular foreign keys: activity_note sorts before legal_unit, unit_note after it.
CREATE TABLE activity_note (id int PRIMARY KEY, legal_unit_id int);
CREATE TABLE unit_note (id int PRIMARY KEY, legal_unit_id int);
SELECT sql_saga.add_foreign_key('activity_note', ARRAY['legal_unit_id']::name[], 'legal_unit', ARRAY['id']::name[]);
SELECT sql_saga.add_foreign_key('unit_note', ARRAY['legal_unit_id']::name[], 'legal_unit', ARRAY['id']::name[]);
INSERT INTO legal_unit (id, name, valid_from, valid_until) SELECT g, 'LU ' || g, '2020-01-01', 'infinity' FROM generate_series(1, 20) AS g;
INSERT INTO establishment (id, name, valid_from, valid_until) SELECT g, 'EST ' || g, '2020-01-01', 'infinity' FROM generate_series(1, 20) AS g;
INSERT INTO activity (id, type, legal_unit_id, valid_from, valid_until) SELECT g, 'primary', g, '2021-01-01', 'infinity' FROM generate_series(1, 10) AS g;
INSERT INTO activity (id, type, establishment_id, valid_from, valid_until) SELECT g, 'primary', g, '2021-01-01', 'infinity' FROM generate_series(11, 20) AS g;
INSERT INTO location (id, type, legal_unit_id, valid_from, valid_until) SELECT g, 'physical', g, '2021-01-01', 'infinity' FROM generate_series(1, 10) AS g;
INSERT INTO person_for_unit (id, person_id, person_role_id, legal_unit_id, valid_from, valid_until) SELECT g, g, 1, g, '2021-01-01', 'infinity' FROM generate_series(1, 10) AS g;
INSERT INTO activity_note SELECT g, 1 + g % 20 FROM generate_series(1, 40) AS g;
INSERT INTO activity_note VALUES (41, NULL);
INSERT INTO unit_note SELECT g, 1 + g % 20 FROM generate_series(1, 40) AS g;
-- Unrelated DDL after setup, as StatBus migrations run it.
ALTER TABLE activity ADD COLUMN note text;
CREATE INDEX activity_lookup_idx ON activity (establishment_id, legal_unit_id, type);
COMMENT ON TABLE location IS 'where a unit is';
ALTER TABLE contact ADD CONSTRAINT contact_one_unit CHECK (num_nonnulls(legal_unit_id, establishment_id) = 1);
SQL
)
printf '%s\n' "$out" | grep -q '^ERROR' && die "StatBus shapes fixture: $(printf '%s\n' "$out" | grep -m1 '^ERROR')"
# The catalog order of every XOR key against the order of its native constraint (range column excluded).
XOR_ORDER_SQL="SELECT string_agg(format('%s=%s', uk.unique_key_name, CASE WHEN uk.column_names = live.cols THEN 'constraint order' WHEN uk.column_names::text[] = (SELECT array_agg(c ORDER BY c COLLATE \"C\") FROM unnest(live.cols::text[]) AS c) THEN 'sorted' ELSE 'other' END), ' ' ORDER BY uk.unique_key_name)
FROM sql_saga.unique_keys AS uk
JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
JOIN pg_catalog.pg_constraint AS c ON c.conrelid = format('%I.%I', uk.table_schema, uk.table_name)::regclass AND c.conname = uk.unique_constraint
CROSS JOIN LATERAL (SELECT array_agg(a.attname ORDER BY u.ord) AS cols FROM unnest(c.conkey) WITH ORDINALITY AS u (attnum, ord)
                    JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (c.conrelid, u.attnum) WHERE a.attname <> e.range_column_name) AS live
WHERE uk.mutually_exclusive_columns IS NOT NULL"
XOR_SORTED="activity_type_establishment_id_valid=sorted contact_natural_key_valid=sorted location_type_establishment_id_valid=sorted person_for_unit_natural_key_valid=sorted stat_for_unit_natural_key_valid=sorted"
XOR_CANONICAL="activity_type_establishment_id_valid=constraint order contact_natural_key_valid=constraint order location_type_establishment_id_valid=constraint order person_for_unit_natural_key_valid=constraint order stat_for_unit_natural_key_valid=constraint order"
# Child-side CHECK constraints of regular foreign keys that call a function reading the parent table.
CHILD_CHECKS_SQL="SELECT count(*) FROM sql_saga.foreign_keys AS fk
JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
JOIN pg_catalog.pg_constraint AS c ON c.conrelid = format('%I.%I', fk.table_schema, fk.table_name)::regclass AND c.contype = 'c'
JOIN pg_catalog.pg_depend AS d ON (d.classid, d.objid, d.refclassid) = ('pg_catalog.pg_constraint'::regclass, c.oid, 'pg_catalog.pg_proc'::regclass)
JOIN pg_catalog.pg_proc AS p ON p.oid = d.refobjid
WHERE fk.type = 'regular_to_temporal' AND p.pronamespace <> 'pg_catalog'::regnamespace AND p.prosrc ~ format('\\m%s\\M', uk.table_name)"
# The shape this section is about: the baseline sorted the XOR keys and enforces the regular FKs with
# CHECKs reading the parent. If not, the fixture does not model StatBus and the section proves nothing.
FAILURE_CLASS=prerequisite
check "StatBus shapes: the $BASELINE_VERSION baseline sorted every XOR key catalog after unrelated DDL" "$XOR_SORTED" "$(q "$STATBUS" "$XOR_ORDER_SQL")"
check "StatBus shapes: the $BASELINE_VERSION baseline enforces both regular FK children with a CHECK reading the parent" "2" "$(q "$STATBUS" "$CHILD_CHECKS_SQL")"
FAILURE_CLASS="expected RED"
psql -X -q -d postgres -c "ALTER DATABASE $STATBUS RESET dynamic_library_path" >>"$LOG" 2>&1 || die "reset dynamic_library_path on $STATBUS"
STATBUS_UPDATE=$(q "$STATBUS" "ALTER EXTENSION sql_saga UPDATE TO '$NEW_VERSION'")
check "StatBus shapes: healthy $BASELINE_VERSION database updates without refusal" "" "$(printf '%s\n' "$STATBUS_UPDATE" | grep '^ERROR')"
check "StatBus shapes: reaches $NEW_VERSION" "$NEW_VERSION" "$(q "$STATBUS" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
check "StatBus shapes: check_integrity passes after the update" "" "$(q "$STATBUS" "SELECT sql_saga.check_integrity()" | grep '^ERROR')"
check "StatBus shapes: every XOR key catalog is in constraint order after the update" "$XOR_CANONICAL" "$(q "$STATBUS" "$XOR_ORDER_SQL")"
check "StatBus shapes: no regular FK child is a CHECK reading the parent after the update" "0" "$(q "$STATBUS" "$CHILD_CHECKS_SQL")"
check "StatBus shapes: unrelated DDL after the update keeps the XOR keys in constraint order" "$XOR_CANONICAL" \
    "$(q "$STATBUS" "ALTER TABLE person_for_unit ADD COLUMN note text" | grep '^ERROR'; q "$STATBUS" "$XOR_ORDER_SQL")"
check "StatBus shapes: check_integrity passes after unrelated DDL on the updated database" "" "$(q "$STATBUS" "SELECT sql_saga.check_integrity()" | grep '^ERROR')"
# Enforcement after the update (each probe rolled back, all constraints checked immediately).
statbus_probe() { if q "$STATBUS" "BEGIN; SET CONSTRAINTS ALL IMMEDIATE; $1; SET CONSTRAINTS ALL IMMEDIATE; ROLLBACK" | grep -q '^ERROR'; then echo refused; else echo accepted; fi; }
check "StatBus shapes: activity_note (sorts before parent) refuses a missing legal unit" "refused" "$(statbus_probe "INSERT INTO activity_note VALUES (99, 999)")"
check "StatBus shapes: activity_note accepts an existing legal unit and NULL" "accepted" "$(statbus_probe "INSERT INTO activity_note VALUES (99, 3); INSERT INTO activity_note VALUES (100, NULL)")"
check "StatBus shapes: unit_note (sorts after parent) refuses a missing legal unit" "refused" "$(statbus_probe "INSERT INTO unit_note VALUES (99, 999)")"
check "StatBus shapes: deleting a legal unit referenced by activity_note is refused" "refused" "$(statbus_probe "DELETE FROM legal_unit WHERE id = 2")"
check "StatBus shapes: activity refuses a missing legal unit (temporal FK)" "refused" \
    "$(statbus_probe "INSERT INTO activity (id, type, legal_unit_id, valid_from, valid_until) VALUES (99, 'primary', 999, '2021-01-01', 'infinity')")"
check "StatBus shapes: activity XOR key refuses a duplicate (type, legal unit)" "refused" \
    "$(statbus_probe "INSERT INTO activity (id, type, legal_unit_id, valid_from, valid_until) VALUES (99, 'primary', 1, '2022-01-01', 'infinity')")"
# The updated database survives serial and --single-transaction restores.
if pg_dump -Fc -d "$STATBUS" -f "$WORK/statbus.dump" 2>>"$LOG"; then
    for variant in serial single_txn; do
        dst=${STATBUS}_${variant}
        new_db "$dst" "$EXT_ROOT"
        opts="--exit-on-error"
        [ $variant = single_txn ] && opts="$opts --single-transaction"
        if pg_restore $opts -d "$dst" "$WORK/statbus.dump" >"$WORK/statbus.$variant.log" 2>&1; then
            ok "StatBus shapes: $variant restore of the updated database"
            check "StatBus shapes: $variant restore: check_integrity passes" "" "$(q "$dst" "SELECT sql_saga.check_integrity()" | grep '^ERROR')"
            check "StatBus shapes: $variant restore: XOR keys in constraint order" "$XOR_CANONICAL" "$(q "$dst" "$XOR_ORDER_SQL")"
            check "StatBus shapes: $variant restore: all rows present" "20|20|20|41|40" \
                "$(q "$dst" "SELECT (SELECT count(*) FROM legal_unit) || '|' || (SELECT count(*) FROM establishment) || '|' || (SELECT count(*) FROM activity) || '|' || (SELECT count(*) FROM activity_note) || '|' || (SELECT count(*) FROM unit_note)")"
        else
            fail "StatBus shapes: $variant restore of the updated database failed: $(grep -m1 -E 'ERROR|error:' "$WORK/statbus.$variant.log")"
        fi
    done
else
    fail "StatBus shapes: pg_dump of the updated database"
fi

# ---------------------------------------------------------------------------------------------
# tmp/fable_audit_v13.md finding 3 (A3). The updatable views are created as "AS TABLE t" / "SELECT * FROM t",
# so their column list is frozen; StatBus then ran ALTER TABLE ... ADD COLUMN on era tables that already had
# views (stat_for_unit.stat GENERATED ALWAYS ... STORED; location.region_version_id plain + FK + CHECK). On
# v1.0 the embedded 1.0 -> 1.1 migration recreates every view, so that path passes today and is kept as a
# guard; on v1.1 the views keep lagging their base and the final check_integrity gate refuses the update.
if [ "$BASELINE_VERSION" = 1.0 ]; then
    section "view column drift: views lag a later ADD COLUMN (guard: the 1.0 path rebuilds views)" "regression"
else
    section "view column drift: views lag a later ADD COLUMN" "expected RED"
fi
VIEW_CLASS=$FAILURE_CLASS
# ---------------------------------------------------------------------------------------------
DRIFT=${PREFIX}_view_drift
new_db "$DRIFT" "$EXT_ROOT" "$OLD_LIB"
out=$(sql "$DRIFT" <<SQL
SET client_min_messages = warning;
CREATE EXTENSION btree_gist;
CREATE EXTENSION sql_saga VERSION '$BASELINE_VERSION';
CREATE TABLE region (id int PRIMARY KEY);
INSERT INTO region VALUES (1), (2);
CREATE FUNCTION drift_today() RETURNS date LANGUAGE sql STABLE AS \$\$ SELECT '2024-06-01'::date \$\$;
CREATE TABLE stat_for_unit (id int NOT NULL, value_int int, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('stat_for_unit', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('stat_for_unit', ARRAY['id']::name[], 'valid', key_type => 'primary', unique_key_name => 'stat_for_unit_id_valid');
SELECT sql_saga.add_for_portion_of_view('stat_for_unit');
CREATE TABLE location (id int NOT NULL, value_int int, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('location', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('location', ARRAY['id']::name[], 'valid', key_type => 'primary', unique_key_name => 'location_id_valid');
SELECT sql_saga.add_for_portion_of_view('location');
SELECT sql_saga.add_current_view('location', current_func_name => 'drift_today()');
INSERT INTO stat_for_unit (id, value_int, valid_from, valid_until) VALUES (1, 1, '2024-01-01', 'infinity');
INSERT INTO location (id, value_int, valid_from, valid_until) VALUES (1, 1, '2024-01-01', 'infinity');
-- After the views exist: the two StatBus column additions (plain + FK + CHECK, and GENERATED STORED).
ALTER TABLE stat_for_unit ADD COLUMN stat int GENERATED ALWAYS AS (value_int * 2) STORED;
ALTER TABLE location ADD COLUMN region_id int REFERENCES region (id) CHECK (region_id > 0);
ALTER TABLE location ADD COLUMN note text;
SQL
)
printf '%s\n' "$out" | grep -q '^ERROR' && die "view drift fixture: $(printf '%s\n' "$out" | grep -m1 '^ERROR')"
# Base columns missing from (or retyped in) each managed view, and generated_columns versus the base.
VIEW_DRIFT_SQL="SELECT string_agg(format('%s:%s', uv.view_name, COALESCE((SELECT string_agg(a.attname, ',' ORDER BY a.attnum)
    FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = format('%I.%I', uv.table_schema, uv.table_name)::regclass AND a.attnum > 0 AND NOT a.attisdropped
      AND NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS va WHERE va.attrelid = format('%I.%I', uv.view_schema, uv.view_name)::regclass
                      AND va.attname = a.attname AND va.atttypid = a.atttypid AND va.attnum > 0 AND NOT va.attisdropped)), '-')), ' ' ORDER BY uv.view_name)
FROM sql_saga.updatable_view AS uv"
GENERATED_SQL="SELECT string_agg(format('%s:%s', uv.view_name, uv.generated_columns = (SELECT COALESCE(array_agg(a.attname ORDER BY a.attnum), '{}')
    FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = format('%I.%I', uv.table_schema, uv.table_name)::regclass
      AND a.attnum > 0 AND NOT a.attisdropped AND (a.attgenerated <> '' OR a.attidentity = 'a'))), ' ' ORDER BY uv.view_name)
FROM sql_saga.updatable_view AS uv"
FAILURE_CLASS=prerequisite
check "view drift: the $BASELINE_VERSION views lag the columns added after them" \
    "location__current_valid:region_id,note location__for_portion_of_valid:region_id,note stat_for_unit__for_portion_of_valid:stat" \
    "$(q "$DRIFT" "$VIEW_DRIFT_SQL")"
FAILURE_CLASS=$VIEW_CLASS
psql -X -q -d postgres -c "ALTER DATABASE $DRIFT RESET dynamic_library_path" >>"$LOG" 2>&1 || die "reset dynamic_library_path on $DRIFT"
check "view drift: healthy $BASELINE_VERSION database updates without refusal" "" \
    "$(q "$DRIFT" "ALTER EXTENSION sql_saga UPDATE TO '$NEW_VERSION'" | grep '^ERROR')"
check "view drift: reaches $NEW_VERSION" "$NEW_VERSION" "$(q "$DRIFT" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
check "view drift: check_integrity passes after the update" "" "$(q "$DRIFT" "SELECT sql_saga.check_integrity()" | grep '^ERROR')"
check "view drift: every view exposes every base column after the update" \
    "location__current_valid:- location__for_portion_of_valid:- stat_for_unit__for_portion_of_valid:-" "$(q "$DRIFT" "$VIEW_DRIFT_SQL")"
check "view drift: generated_columns equals the base's generated columns after the update" \
    "location__current_valid:t location__for_portion_of_valid:t stat_for_unit__for_portion_of_valid:t" "$(q "$DRIFT" "$GENERATED_SQL")"
check "view drift: a portion update through the view sets the added column, the generated column is computed" \
    "2024-01-01|2024-03-01|1|2|||2024-03-01|2024-05-01|5|10|2|mid|2024-05-01|infinity|1|2||" \
    "$(q "$DRIFT" "BEGIN;
UPDATE stat_for_unit__for_portion_of_valid SET value_int = 5, valid_from = '2024-03-01', valid_until = '2024-05-01' WHERE id = 1;
UPDATE location__for_portion_of_valid SET region_id = 2, note = 'mid', valid_from = '2024-03-01', valid_until = '2024-05-01' WHERE id = 1;
SELECT string_agg(format('%s|%s|%s|%s|%s|%s', s.valid_from, s.valid_until, s.value_int, s.stat, l.region_id, l.note), '|' ORDER BY s.valid_from)
FROM stat_for_unit AS s JOIN location AS l USING (id, valid_from);
ROLLBACK" | grep -v '^BEGIN\|^ROLLBACK\|^UPDATE')"
check "view drift: an update through the current view sets the added column" "2024-06-01|infinity|1|now" \
    "$(q "$DRIFT" "BEGIN;
UPDATE location__current_valid SET region_id = 1, note = 'now' WHERE id = 1;
SELECT concat_ws('|', valid_from, valid_until, region_id, note) FROM location WHERE valid_from = '2024-06-01';
ROLLBACK" | grep -v '^BEGIN\|^ROLLBACK\|^UPDATE')"
# ADD COLUMN on the updated database: the candidate itself must keep the views in step (fresh-install
# A3, RED on both paths until rename_following rebuilds the views).
FAILURE_CLASS="expected RED"
check "view drift: ADD COLUMN after the update keeps check_integrity passing" "" \
    "$(q "$DRIFT" "ALTER TABLE location ADD COLUMN later text" | grep '^ERROR'; q "$DRIFT" "SELECT sql_saga.check_integrity()" | grep '^ERROR')"
check "view drift: ADD COLUMN after the update is exposed by every view of the table" \
    "location__current_valid:- location__for_portion_of_valid:- stat_for_unit__for_portion_of_valid:-" "$(q "$DRIFT" "$VIEW_DRIFT_SQL")"

# ---------------------------------------------------------------------------------------------
section "ALTER EXTENSION sql_saga UPDATE TO '$NEW_VERSION'" "expected RED"
# ---------------------------------------------------------------------------------------------
UPDATE_OUT=$(printf "SET client_min_messages = notice;\nALTER EXTENSION sql_saga UPDATE TO '%s';\n" "$NEW_VERSION" | sql "$OLD")
printf '%s\n' "$UPDATE_OUT" >"$WORK/update.out"
if printf '%s\n' "$UPDATE_OUT" | grep -q '^ERROR'; then
    UPDATE_ERROR=$(printf '%s\n' "$UPDATE_OUT" | grep -m1 '^ERROR' | sed 's/^ERROR:  *//')
    fail "update failed: $UPDATE_ERROR"
    UPGRADED=0
else
    ok "update ran"
    UPGRADED=1
fi
if [ "$UPDATE_ERROR" = "extension \"sql_saga\" has no update path from version \"$BASELINE_VERSION\" to version \"$NEW_VERSION\"" ]; then
    ok "RED gate: update path is the exact missing artifact"
elif [ -n "$UPDATE_ERROR" ]; then
    fail "RED gate failed: update error is not the missing update path"
fi
FAILURE_CLASS="not meaningful (dependent)"
check "extversion after update" "$NEW_VERSION" "$(q "$OLD" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"

# ---------------------------------------------------------------------------------------------
section "1.2 structure after upgrade" "not meaningful (dependent)"
# ---------------------------------------------------------------------------------------------
[ $UPGRADED = 1 ] || echo "    (update did not run: these assertions describe the 1.0 shape that the update must change)"
assert_structure "$OLD"
assert_integrity "$OLD"
check "upgrade leaves no extension initial privileges on detached managed objects" "0" "$(nonmember_managed_initprivs "$OLD")"
check "history policy copies in sync after upgrade" "0" "$(policy_copy_faults "$OLD")"
if [ "$BASELINE_VERSION" = 1.0 ]; then
    if [ "$(acl_digest "$OLD")" != "$OLD_DIGEST" ]; then ok "v1.0 derived ACLs were normalized (ACL digest changed)"
    else fail "v1.0 ACL digest unchanged: mirrored and direct derived grants survive"; fi
else
    check "pristine v1.1 ACL digest is preserved by the 1.2 update" "$OLD_DIGEST" "$(acl_digest "$OLD")"
fi
check "direct derived grants to saga_up_none were removed" "f|f|f" \
    "$(q "$OLD" "SELECT EXISTS (SELECT FROM aclexplode(coalesce((SELECT relacl FROM pg_class WHERE oid = 'sv_with_history'::regclass), acldefault('r', (SELECT relowner FROM pg_class WHERE oid = 'sv_with_history'::regclass)))) AS a WHERE a.grantee = '${PREFIX}_none'::regrole AND a.privilege_type = 'SELECT') || '|' || EXISTS (SELECT FROM aclexplode(coalesce((SELECT relacl FROM pg_class WHERE oid = 'fp__for_portion_of_valid'::regclass), acldefault('r', (SELECT relowner FROM pg_class WHERE oid = 'fp__for_portion_of_valid'::regclass)))) AS a WHERE a.grantee = '${PREFIX}_none'::regrole AND a.privilege_type = 'SELECT') || '|' || EXISTS (SELECT FROM aclexplode(coalesce((SELECT proacl FROM pg_proc WHERE oid = 'sv__as_of(timestamptz)'::regprocedure), acldefault('f', (SELECT proowner FROM pg_proc WHERE oid = 'sv__as_of(timestamptz)'::regprocedure)))) AS a WHERE a.grantee = '${PREFIX}_none'::regrole AND a.privilege_type = 'EXECUTE')" | sed 's/true/t/g; s/false/f/g')"

query_function_snapshot "$OLD" >"$WORK/query_functions.after"
if diff -u "$WORK/query_functions.before" "$WORK/query_functions.after" >"$WORK/query_functions.diff"; then
    ok "all generated query function definitions, properties and plans match released 1.1"
else
    fail "generated query functions changed from released 1.1 ($(grep -c '^[-+][^-+]' "$WORK/query_functions.diff") differing lines)"
fi

# ---------------------------------------------------------------------------------------------
section "1.2 behaviour after upgrade" "not meaningful (dependent)"
# ---------------------------------------------------------------------------------------------
behaviour "$OLD"

# ---------------------------------------------------------------------------------------------
section "fresh $NEW_VERSION versus upgraded equivalence" "not meaningful (dependent)"
# ---------------------------------------------------------------------------------------------
FRESH=${PREFIX}_fresh
new_db "$FRESH" "$EXT_ROOT"
fixture_core "$NEW_VERSION" | sql "$FRESH" >"$WORK/fresh.fixture.log"
if grep -q '^ERROR' "$WORK/fresh.fixture.log"; then
    fail "fresh install of $NEW_VERSION with the core fixtures failed: $(grep -m1 '^ERROR' "$WORK/fresh.fixture.log" | sed 's/^ERROR:  *//')"
    FRESH_OK=0
else
    ok "fresh $NEW_VERSION install with the core fixtures"
    FRESH_OK=1
fi
if [ $FRESH_OK = 1 ]; then
    assert_structure "$FRESH"
    assert_integrity "$FRESH"
    check "fresh install has no extension initial privileges on detached managed objects" "0" "$(nonmember_managed_initprivs "$FRESH")"
    behaviour "$FRESH"
fi
if [ $UPGRADED = 0 ] || [ $FRESH_OK = 0 ]; then
    fail "comparison not meaningful: needs a successful update and a fresh $NEW_VERSION install (both run above)"
else
SEMANTIC_OK=1
for db in "$OLD" "$FRESH"; do
    ext_snapshot "$db" >"$WORK/$db.ext"
    if out=$(semantic_capture "$db" "$WORK/$db.semantic"); then
        ok "semantic snapshot of $db ran cleanly and covers every section ($(wc -l <"$WORK/$db.semantic" | tr -d ' ') lines)"
    else
        fail "$out"
        SEMANTIC_OK=0
    fi
    token_sed "$db" >"$WORK/$db.sed" 2>/dev/null
    grep -q '^ERROR' "$WORK/$db.sed" && : >"$WORK/$db.sed"
    dump_schema "$db" | sed -f "$WORK/$db.sed" >"$WORK/$db.schema.unsorted"
    # pg_dump emits objects of equal sort rank (policies) in OID order, and each is preceded by a
    # "-- Name: ...; Type: ..." comment block: drop comment lines (no schema content) and compare the
    # CREATE POLICY statements as a sorted set.
    grep -v -E '^CREATE POLICY|^--' "$WORK/$db.schema.unsorted" >"$WORK/$db.schema.sql"
    grep -E '^CREATE POLICY' "$WORK/$db.schema.unsorted" | LC_ALL=C sort >>"$WORK/$db.schema.sql"
done
if diff -u "$WORK/$FRESH.ext" "$WORK/$OLD.ext" >"$WORK/ext.diff"; then ok "extension objects identical (routines, bodies, ACLs, relations, types, event triggers, members)"
else
    fail "extension objects differ between fresh and upgraded: $(grep -c '^[-+][^-+]' "$WORK/ext.diff") lines (see $WORK/ext.diff)"
    grep '^[-+][^-+]' "$WORK/ext.diff" | head -8 | sed 's/^/          /'
fi
if [ $SEMANTIC_OK = 0 ]; then fail "exhaustive semantic comparison not run: a snapshot failed or lacks coverage"
elif diff -u "$WORK/$FRESH.semantic" "$WORK/$OLD.semantic" >"$WORK/semantic.diff"; then ok "exhaustive normalized semantic catalogs identical, including aggregates and member initial privileges"
else fail "semantic catalogs differ between fresh and upgraded (see $WORK/semantic.diff)"; grep '^[-+][^-+]' "$WORK/semantic.diff" | head -8 | sed 's/^/          /'; fi
# Member initial privileges, compared on their own so a difference is named even when other sections
# also differ: every pg_init_privs row of a current sql_saga member, privtype and full ACL.
if [ $SEMANTIC_OK = 1 ]; then
    grep '^initpriv|' "$WORK/$FRESH.semantic" >"$WORK/$FRESH.initprivs"
    grep '^initpriv|' "$WORK/$OLD.semantic" >"$WORK/$OLD.initprivs"
    if diff -u "$WORK/$FRESH.initprivs" "$WORK/$OLD.initprivs" >"$WORK/initprivs.diff"; then
        ok "pg_init_privs of every sql_saga member identical fresh versus upgraded ($(wc -l <"$WORK/$OLD.initprivs" | tr -d ' ') rows)"
    else
        fail "pg_init_privs of sql_saga members differ fresh versus upgraded: $(grep -c '^[-+][^-+]' "$WORK/initprivs.diff") lines (see $WORK/initprivs.diff)"
        grep '^[-+][^-+]' "$WORK/initprivs.diff" | head -6 | sed 's/^/          /'
    fi
fi
if [ $FRESH_OK = 1 ]; then
    role_matrix "$OLD" >"$WORK/$OLD.roles"; role_matrix "$FRESH" >"$WORK/$FRESH.roles"
    if diff -u "$WORK/$FRESH.roles" "$WORK/$OLD.roles" >"$WORK/roles.diff"; then ok "per-role reads and DML rights identical on every relation"
    else fail "per-role behaviour differs between fresh and upgraded (see $WORK/roles.diff)"; grep '^[-+][^-+]' "$WORK/roles.diff" | head -6 | sed 's/^/          /'; fi
fi
if diff -u "$WORK/$FRESH.schema.sql" "$WORK/$OLD.schema.sql" >"$WORK/schema.diff"; then ok "normalized pg_dump -s identical"
else
    fail "normalized pg_dump -s differs between fresh and upgraded: $(grep -c '^[-+][^-+]' "$WORK/schema.diff") lines (see $WORK/schema.diff)"
    grep '^[-+][^-+]' "$WORK/schema.diff" | head -8 | sed 's/^/          /'
fi
fi

# ---------------------------------------------------------------------------------------------
section "idempotence" "not meaningful (dependent)"
# ---------------------------------------------------------------------------------------------
if [ $UPGRADED = 0 ]; then
    fail "not run: the first update failed, there is nothing to re-run"
else
snap() { echo "$(ext_snapshot "$1" | md5)|$(dump_schema "$1" | md5)|$(acl_digest "$1")"; }
S1=$(snap "$OLD"); X1=$(xmin_digest "$OLD")
out=$(printf "SET client_min_messages = notice;\nALTER EXTENSION sql_saga UPDATE TO '%s';\nALTER EXTENSION sql_saga UPDATE;\n" "$NEW_VERSION" | sql "$OLD")
if printf '%s\n' "$out" | grep -q '^ERROR'; then fail "re-running ALTER EXTENSION UPDATE fails: $(printf '%s\n' "$out" | grep -m1 '^ERROR')"
else ok "re-running ALTER EXTENSION UPDATE is accepted"; fi
check "re-running the update changes nothing" "$S1" "$(snap "$OLD")"
check "reconciler rerun leaves no extension initial privileges on detached managed objects" "0" "$(nonmember_managed_initprivs "$OLD")"
out=$(sql "$OLD" <<'SQL'
SET client_min_messages = notice;
SELECT format('SELECT sql_saga.__internal_reconcile_system_versioning_family(%L::regclass)', format('%I.%I', sv.table_schema, sv.table_name))
FROM sql_saga.system_versioning AS sv ORDER BY 1 \gexec
SELECT format('SELECT sql_saga.sync_history_policies(%L::regclass)', format('%I.%I', sv.table_schema, sv.table_name))
FROM sql_saga.system_versioning AS sv ORDER BY 1 \gexec
SQL
)
printf '%s\n' "$out" >"$WORK/reconcile.out"
if printf '%s\n' "$out" | grep -q '^ERROR'; then fail "reconciler re-run fails: $(printf '%s\n' "$out" | grep -m1 '^ERROR' | sed 's/^ERROR:  *//')"
else ok "reconciler re-run succeeds"; fi
if printf '%s\n' "$out" | grep -q 'NOTICE:  sql_saga:'; then fail "reconciler re-run reports changes: $(printf '%s\n' "$out" | grep -m1 'NOTICE:  sql_saga:')"
else ok "reconciler re-run emits no change NOTICEs"; fi
check "reconciler re-run changes no object definitions" "$S1" "$(snap "$OLD")"
check "reconciler re-run rewrites no catalog rows (xmin)" "$X1" "$(xmin_digest "$OLD")"
fi

# ---------------------------------------------------------------------------------------------
section "dump and restore after upgrade" "not meaningful (dependent)"
# ---------------------------------------------------------------------------------------------
if [ $UPGRADED = 0 ]; then
    fail "not run: no upgraded database to dump"
else
SRC_TOKEN=$(q "$OLD" "SELECT token FROM sql_saga.installation")
SRC_DIGEST=$(acl_digest "$OLD")
prev="$OLD"
for hop in 1 2; do
    serial_dst=${PREFIX}_old_rt${hop}_serial
    pg_dump -Fc -d "$prev" -f "$WORK/rt$hop.dump" 2>>"$LOG" || { fail "pg_dump of $prev"; break; }
    for variant in serial single_txn; do
        dst=${PREFIX}_old_rt${hop}_${variant}
        new_db "$dst" "$EXT_ROOT"
        opts="--exit-on-error"
        [ $variant = single_txn ] && opts="$opts --single-transaction"
        if ! pg_restore $opts -d "$dst" "$WORK/rt$hop.dump" >"$WORK/rt$hop.$variant.log" 2>&1; then
            fail "hop $hop $variant: pg_restore failed: $(grep -m1 -E 'ERROR|error:' "$WORK/rt$hop.$variant.log")"
            continue
        fi
        tok=$(q "$dst" "SELECT token FROM sql_saga.installation")
        if [ -n "$tok" ] && [ "$tok" != "$SRC_TOKEN" ] && ! printf '%s' "$tok" | grep -q ERROR; then ok "hop $hop $variant: installation token rotated"
        else fail "hop $hop $variant: installation token not rotated ('$tok')"; fi
        check "hop $hop $variant: version" "$NEW_VERSION" "$(q "$dst" "SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'")"
        check "hop $hop $variant: native library version equals extversion" "t" "$(q "$dst" "SELECT sql_saga.native_library_version() = (SELECT extversion FROM pg_extension WHERE extname='sql_saga')")"
        check "hop $hop $variant: history policy copies in sync" "0" "$(policy_copy_faults "$dst")"
        check "hop $hop $variant: SYSTEM_TIME snapshots equal live columns" "0" "$(snapshot_faults "$dst")"
        check "hop $hop $variant: tenant isolation" "$ISOLATION_EXPECTED" "$(tenant_isolation "$dst")"
        check "hop $hop $variant: ACL digest equals the upgraded source" "$SRC_DIGEST" "$(acl_digest "$dst")"
        assert_integrity "$dst"
    done
    prev=$serial_dst
done
# pg_restore --clean over the upgraded database itself (teardown predicate must let the drops through)
CLN=${PREFIX}_old_clean
new_db "$CLN" "$EXT_ROOT"
if pg_restore --exit-on-error -d "$CLN" "$WORK/rt1.dump" >"$WORK/clean.seed.log" 2>&1 \
   && pg_restore --clean --if-exists --exit-on-error -d "$CLN" "$WORK/rt1.dump" >"$WORK/clean.log" 2>&1; then
    ok "pg_restore --clean into a database holding the upgraded families"
    check "--clean restore: history policy copies in sync" "0" "$(policy_copy_faults "$CLN")"
    check "--clean restore: SYSTEM_TIME snapshots equal live columns" "0" "$(snapshot_faults "$CLN")"
    check "--clean restore: tenant isolation" "$ISOLATION_EXPECTED" "$(tenant_isolation "$CLN")"
    assert_integrity "$CLN"
else
    fail "pg_restore --clean failed: $(cat "$WORK/clean.seed.log" "$WORK/clean.log" 2>/dev/null | grep -m1 -E 'ERROR|error:')"
fi
fi

# ---------------------------------------------------------------------------------------------
echo
if [ ${#FAILS[@]} -gt 0 ]; then
    echo "RED. First failure: $RED_REASON"
    if [ -n "$UPDATE_ERROR" ]; then
        echo "RED. ALTER EXTENSION sql_saga UPDATE TO '$NEW_VERSION' on the $BASELINE_TAG database: $UPDATE_ERROR"
    fi
    printf 'failure classes:\n'
    printf '    %s\n' "${FAILS[@]}" | sed 's/|/: /'
    [ "$KEEP" = 1 ] && echo "logs: $WORK" || echo "work directory will be removed (rerun with UPGRADE_KEEP=1 to retain it)"
    exit 1
fi
echo "GREEN"
exit 0
