#!/usr/bin/env python3
"""Direct 1.0..1.7 to 1.8 updates from authenticated v1.7 artifacts (devops/direct_update.py builds them).

Specified by devops/test_generate_update_scripts_1_8.py. The 1.8 delta: sql_saga.era loses the never-written
audit_schema_name and audit_table_name (devops/upgrade_1_8_schema.sql, catalog_format 1.8), the health check
refuses an UNLOGGED history table of a system-versioned table, and temporal_merge generates its key-column SQL in a
deterministic order.
"""
from direct_update import Release, GeneratorError  # noqa: F401  (GeneratorError: for the tests)

RELEASE = Release(
    target='1.8',
    previous='1.7',
    sources=('1.0', '1.1', '1.2', '1.3', '1.4', '1.5', '1.6', '1.7'),
    # Produced only from git archive bc0fc53 (v1.7), using that archive's generator; two builds were identical.
    frozen_sha256={
        'sql_saga--1.7.sql': 'ebd04d86a0abb2418589718e80ad575deef6d06ad2b13607b7247979fd59946b',
        'sql_saga--1.0--1.7.sql': 'dbf8ad06664fc5b1fc1bdd7847aebc3923665e32ff0b82bbff0d32b92a3f54bd',
        'sql_saga--1.1--1.7.sql': '8cb8b95b10a0f6181161c4371f58a2abd20ebd7203a9e2b26932572285694a65',
        'sql_saga--1.2--1.7.sql': '0f27b9d5891f1a8a97501fc50b9bb2ae520cd40c68f27ca93dcdaa999711e616',
        'sql_saga--1.3--1.7.sql': '4cad98ffcee22d9f849c8c2491a8d958b74a0b695f2252ae89143e624c1a44e5',
        'sql_saga--1.4--1.7.sql': '48c9b789fa481303f4f2c7d35d737ea66c3a191791255fd4167659d2ad9892c9',
        'sql_saga--1.5--1.7.sql': 'e00c68c0d3e723312ee70f4b73d2dfc78f109d0f2d718fbc55016bfcde201351',
        'sql_saga--1.6--1.7.sql': '5b835fefd6565c4772458c1ba0e445e316a400c6c11636e7851f737edd0e0676',
    },
    routine_manifest=(
        'sql_saga.__internal_ddl_command_affects_managed_object()',
        'sql_saga.__api_add_era(regclass, name, name, boolean, name, name, name, boolean, boolean, boolean, regtype, name, name[])',
        'sql_saga.__internal_add_system_time_era(regclass, name, name, name, name, name, name[])',
        'sql_saga.__internal_health_checks()',
        'sql_saga.check_integrity()',
        # Generated SQL lists key columns in their declared order, not in the order the catalog join returned them.
        'sql_saga.temporal_merge_plan(regclass, regclass, sql_saga.temporal_merge_mode, name, text[], name, name, sql_saga.temporal_merge_delete_mode, jsonb, text[], boolean, boolean)',
        'sql_saga.temporal_merge_execute(regclass, regclass, text[], sql_saga.temporal_merge_mode, name, name, name, boolean, text[], sql_saga.temporal_merge_delete_mode, boolean, name, name, name, name, text[], boolean)',
        # Comments only: their design citations now name doc/design/ instead of the archived tmp/ notes.
        'sql_saga.__api_add_system_versioning(regclass, name, name, name, name, name, name, boolean)',
        'sql_saga.__internal_drop_protection()',
    ),
    new_routines=(),
    new_privileges=(),
    repairs='upgrade_1_8_repairs.sql',
    # The 1.4--1.7 to 1.6--1.7 bodies have no native guard after their opening prefix.
    sources_without_inner_guards=('1.4', '1.5', '1.6'),
    schema='upgrade_1_8_schema.sql',
    changed_tables=('sql_saga.era',),
)
build = RELEASE.build

if __name__ == '__main__':
    RELEASE.main('generate_update_scripts_1_8')
