# Sourced by the upgrade harness and its mutation tests. Keep scanner failures
# distinct from an ordinary negative match: no error may be reported as PASS.
assert_no_cascading_ddl() {
    local scan_status
    if printf '%s\n' "$1" | "${SQL_SAGA_PYTHON:-python3}" "$REPO/devops/check_no_cascading_ddl.py"; then
        ok "direct update script uses no cascading DROP/TRUNCATE"
    else
        scan_status=$?
        case "$scan_status" in
            1) fail "direct update script uses cascading DROP/TRUNCATE (never cascade into user objects)" ;;
            *) die "migration CASCADE scanner failed with status $scan_status" ;;
        esac
    fi
}
