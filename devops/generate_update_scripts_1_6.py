#!/usr/bin/env python3
"""Direct 1.0..1.5 to 1.6 updates from authenticated v1.5 artifacts (devops/direct_update.py builds them).

Specified by devops/test_generate_update_scripts_1_6.py, against the released v1.6 source tree. The 1.6 delta:
batched writes through the for_portion_of view, and devops/upgrade_1_6_repairs.sql adds the apply trigger to
existing for_portion_of views. Its output must stay byte-identical to the frozen versions/sql_saga--*--1.6.sql.
"""
from direct_update import Release, GeneratorError  # noqa: F401  (GeneratorError: for the tests)

RELEASE = Release(
    target='1.6',
    previous='1.5',
    sources=('1.0', '1.1', '1.2', '1.3', '1.4', '1.5'),
    # Produced only from git archive 545839a (v1.5), using that archive's generator.
    frozen_sha256={
        'sql_saga--1.5.sql': '37dbcdbb3572dfd01fe5d1090ad098cbb91527ecbc2db728e65926669949d8a7',
        'sql_saga--1.0--1.5.sql': '5cb7ce6c3854fd7ae314f602e459df1663d3fec46376dd1d31c5729237ef92f8',
        'sql_saga--1.1--1.5.sql': '2070fea4c1fd602285f2f349ba26d991d79015cede64525be45beee5d4a1d7c8',
        'sql_saga--1.2--1.5.sql': 'd32ad1a720015d0ac1bae40e1d9da442ea18d6e89e31aec4f32bd1194ae01688',
        'sql_saga--1.3--1.5.sql': '399788ef7517c1a0189eadbe921ab7fe43b2af52e2695c7746e7940d3c4ad601',
        'sql_saga--1.4--1.5.sql': 'e8dc9f74f6c0fd87ac9cca4964e04d1059da86f5973e5928ff2fd564ba2d8b3d',
    },
    routine_manifest=(
        'sql_saga.__internal_view_apply_trigger_name(sql_saga.updatable_view_type, name)',
        'sql_saga.for_portion_of_trigger()',
        'sql_saga.for_portion_of_apply_trigger()',
        'sql_saga.__api_add_for_portion_of_view(regclass, name, name[])',
        'sql_saga.__internal_drop_protection()',
        'sql_saga.__internal_rename_following()',
        'sql_saga.check_integrity()',
        'sql_saga.__internal_updatable_view_follow_shape(oid[])',
    ),
    new_routines=(
        'sql_saga.__internal_view_apply_trigger_name(sql_saga.updatable_view_type, name)',
        'sql_saga.for_portion_of_apply_trigger()',
    ),
    new_privileges=(
        'GRANT EXECUTE ON FUNCTION sql_saga.for_portion_of_apply_trigger() TO PUBLIC;',
        'REVOKE ALL ON FUNCTION sql_saga.__internal_view_apply_trigger_name(sql_saga.updatable_view_type, name) FROM PUBLIC;',
    ),
    repairs='upgrade_1_6_repairs.sql',
    # The 1.4--1.5 body has no native guard after its opening prefix.
    sources_without_inner_guards=('1.4',),
)
build = RELEASE.build

if __name__ == '__main__':
    RELEASE.main('generate_update_scripts_1_6')
