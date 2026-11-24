/*
 * The hand-written part of the current release's update step (devops/update_step.py): catalog DDL and data repairs
 * that need the new definitions. It runs after every new and changed routine is in place, and the step's integrity
 * gate runs after it; a part between the lines "-- before routines" and "-- after routines" runs before the routines
 * (a type or table a new signature uses). Name each install-script change it makes in a line "-- handles: <key>"
 * (make prints the keys). Emptied after each release.
 */

/* 1.15: delete modes apply to the MERGE_ENTITY_* modes only; the other modes refuse them (hug 2hxkwx). */
-- handles: COMMENT ON TYPE sql_saga.temporal_merge_delete_mode
COMMENT ON TYPE sql_saga.temporal_merge_delete_mode IS
'Controls deletion for the MERGE_ENTITY_* modes; the other modes refuse any value but NONE.
- NONE (default): No deletions occur.
- DELETE_MISSING_TIMELINE: For entities in the source, any part of their target timeline not covered by the source is deleted.
- DELETE_MISSING_ENTITIES: Any entity in the target that is not present in the source is completely deleted.
- DELETE_MISSING_TIMELINE_AND_ENTITIES: A combination of both timeline and entity deletion.';
