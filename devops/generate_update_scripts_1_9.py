#!/usr/bin/env python3
"""Direct 1.0..1.8 to 1.9 updates from authenticated v1.8 artifacts (devops/direct_update.py builds them).

Specified by devops/test_generate_update_scripts_1_9.py. The 1.9 delta: provenance columns (doc/design/provenance.md).
sql_saga.era gains provenance_columns (devops/upgrade_1_9_schema.sql, catalog_format 1.9); add_era gains a trailing
provenance_columns argument, so its signature is replaced; set_era_provenance_columns and the managed trigger
function are new; the planners, the executor, drop protection, rename following and check_integrity know them.
"""
from direct_update import Release, GeneratorError  # noqa: F401  (GeneratorError: for the tests)

OLD_ADD_ERA_ARGS = 'regclass, name, name, boolean, name, name, name, boolean, boolean, boolean, regtype, name, name[]'
NEW_ADD_ERA_ARGS = OLD_ADD_ERA_ARGS + ', name[]'

RELEASE = Release(
    target='1.9',
    previous='1.8',
    sources=('1.0', '1.1', '1.2', '1.3', '1.4', '1.5', '1.6', '1.7', '1.8'),
    # Produced only from git archive ea6542e (v1.8), using that archive's generator; two builds were identical.
    frozen_sha256={
        'sql_saga--1.8.sql': 'e380c3aee6bfb9b2c716953cc1bf09fb66668a05378bdf01c7481e3a8f7795de',
        'sql_saga--1.0--1.8.sql': '221081d8642610d76ef735ce65b36b48c77db03a5932e1c8b2942eef863223ba',
        'sql_saga--1.1--1.8.sql': '4f0b1f50bdc3c348342da0aba65300252b180a3fc098c72cff1e6b1ff88a34a4',
        'sql_saga--1.2--1.8.sql': 'ad145f13916281312ea825c46f5b4e329057a850e492772ab3b7384f68ae3a71',
        'sql_saga--1.3--1.8.sql': 'c536e52a42890c4f1031a0beaeaeed3849399264cbf0b6b716c3a490e10e3a73',
        'sql_saga--1.4--1.8.sql': '66832f39821dd952f2c0eaf4cb78b14ec34e230e11dff084c163dcbfc46cc812',
        'sql_saga--1.5--1.8.sql': '2790503080e13c733094ed618d2165036c06cdc8e78625d0859ff3d0a2fc6b98',
        'sql_saga--1.6--1.8.sql': '0768cf550686e507e625a012d2477ab8613d10d1a9e778ba12bd9c8a0e4353dc',
        'sql_saga--1.7--1.8.sql': '952e5555b1e9244de3c10e3114b393aed1888262561b0d20a0f2f68e4ca562c1',
    },
    routine_manifest=(
        # New: the trigger and the shared worker first, so add_era and the setter can call them.
        'sql_saga.provenance_trigger()',
        'sql_saga.__internal_set_era_provenance_columns(regclass, name, name[])',
        'sql_saga.__api_set_era_provenance_columns(regclass, name, name[])',
        'sql_saga.set_era_provenance_columns(regclass, name, name[])',
        'sql_saga.__api_add_era(%s)' % NEW_ADD_ERA_ARGS,
        'sql_saga.add_era(%s)' % NEW_ADD_ERA_ARGS,
        # Changed.
        'sql_saga.__api_drop_era(regclass, name, sql_saga.drop_behavior, boolean)',
        'sql_saga.__api_set_era_ephemeral_columns(regclass, name, name[])',
        'sql_saga.__internal_add_system_time_era(regclass, name, name, name, name, name, name[])',
        'sql_saga.__internal_drop_protection()',
        'sql_saga.__internal_era_ephemeral_and_pk_consistency_rename_follow()',
        'sql_saga.check_integrity()',
        'sql_saga.temporal_merge_plan(regclass, regclass, sql_saga.temporal_merge_mode, name, text[], name, name, sql_saga.temporal_merge_delete_mode, jsonb, text[], boolean, boolean)',
        'sql_saga.temporal_merge_execute(regclass, regclass, text[], sql_saga.temporal_merge_mode, name, name, name, boolean, text[], sql_saga.temporal_merge_delete_mode, boolean, name, name, name, name, text[], boolean)',
    ),
    new_routines=(
        'sql_saga.provenance_trigger()',
        'sql_saga.__internal_set_era_provenance_columns(regclass, name, name[])',
        'sql_saga.__api_set_era_provenance_columns(regclass, name, name[])',
        'sql_saga.set_era_provenance_columns(regclass, name, name[])',
        'sql_saga.__api_add_era(%s)' % NEW_ADD_ERA_ARGS,
        'sql_saga.add_era(%s)' % NEW_ADD_ERA_ARGS,
    ),
    new_privileges=(
        'GRANT EXECUTE ON FUNCTION sql_saga.provenance_trigger() TO PUBLIC;',
        'REVOKE ALL ON FUNCTION sql_saga.__api_add_era(%s) FROM PUBLIC;' % NEW_ADD_ERA_ARGS,
        'REVOKE ALL ON FUNCTION sql_saga.__api_set_era_provenance_columns(regclass, name, name[]) FROM PUBLIC;',
        'REVOKE ALL ON FUNCTION sql_saga.__internal_set_era_provenance_columns(regclass, name, name[]) FROM PUBLIC;',
    ),
    # add_era gains provenance_columns: DROP and CREATE (as 1.2 did for add_system_versioning), refused when the site
    # changed the old routine's EXECUTE privileges. The public function before its worker, which it calls by name.
    replaced_routines=(
        ('sql_saga.add_era(%s)' % OLD_ADD_ERA_ARGS, 'sql_saga.add_era(%s)' % NEW_ADD_ERA_ARGS),
        ('sql_saga.__api_add_era(%s)' % OLD_ADD_ERA_ARGS, 'sql_saga.__api_add_era(%s)' % NEW_ADD_ERA_ARGS),
    ),
    commented_routines=('sql_saga.__api_add_era', 'sql_saga.provenance_trigger', 'sql_saga.__api_set_era_provenance_columns'),
    repairs='upgrade_1_9_repairs.sql',
    # Embedded X--1.8 bodies with no native guard after their opening prefix (as for the X--1.7 bodies in 1.8).
    sources_without_inner_guards=('1.4', '1.5', '1.6', '1.7'),
    schema='upgrade_1_9_schema.sql',
    changed_tables=('sql_saga.era',),
)
build = RELEASE.build

if __name__ == '__main__':
    RELEASE.main('generate_update_scripts_1_9')
