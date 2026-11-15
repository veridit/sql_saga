#!/usr/bin/env python3
"""Direct 1.0..1.6 to 1.7 updates from authenticated v1.6 artifacts (devops/direct_update.py builds them).

Specified by devops/test_generate_update_scripts_1_7.py. The 1.7 delta: batched INSERT through the current view,
and devops/upgrade_1_7_repairs.sql adds the apply trigger to existing current views.
"""
from direct_update import Release, GeneratorError  # noqa: F401  (GeneratorError: for the tests)

RELEASE = Release(
    target='1.7',
    previous='1.6',
    sources=('1.0', '1.1', '1.2', '1.3', '1.4', '1.5', '1.6'),
    # Produced only from git archive 252c0f7 (v1.6), using that archive's generator; two builds were identical.
    frozen_sha256={
        'sql_saga--1.6.sql': 'e701a15531d3e679b794db83c272d7d9776dc9999f4e7ac5af6cbb57cf125a53',
        'sql_saga--1.0--1.6.sql': '9478c619b1c98c8eec42738a50c84ef07b4df5c7a72644be655d42aff8823574',
        'sql_saga--1.1--1.6.sql': '162412ee3563fc61d40cf6adeb2d0d8e022badc80702b5e77c270ba9dae32124',
        'sql_saga--1.2--1.6.sql': 'f61bfbdc686ee922a596c9b47d91a795611a829d33b44749a1f30b1ed65ccebd',
        'sql_saga--1.3--1.6.sql': 'ec39665b4008ab1af674e51267daa1b164054437a0f3e1faa7ae6f287955155b',
        'sql_saga--1.4--1.6.sql': '895467881031b8fd5a9d591f3d0b2fd80c7f89468857f11e10662d2e224b887b',
        'sql_saga--1.5--1.6.sql': '9938d0c177816d421cf242c2c7441700e8e8222b5ec33e0a2a14c324453fc568',
    },
    routine_manifest=(
        'sql_saga.__internal_view_apply_trigger_name(sql_saga.updatable_view_type, name)',
        'sql_saga.current_view_trigger()',
        'sql_saga.current_view_apply_trigger()',
        'sql_saga.__api_add_current_view(regclass, name, name, text)',
        'sql_saga.__internal_rename_following()',
        'sql_saga.check_integrity()',
        'sql_saga.__internal_updatable_view_follow_shape(oid[])',
    ),
    new_routines=('sql_saga.current_view_apply_trigger()',),
    new_privileges=('GRANT EXECUTE ON FUNCTION sql_saga.current_view_apply_trigger() TO PUBLIC;',),
    repairs='upgrade_1_7_repairs.sql',
    # The 1.4--1.6 and 1.5--1.6 bodies have no native guard after their opening prefix.
    sources_without_inner_guards=('1.4', '1.5'),
)
build = RELEASE.build

if __name__ == '__main__':
    RELEASE.main('generate_update_scripts_1_7')
