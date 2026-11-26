/*
 * The hand-written part of the current release's update step (devops/update_step.py): catalog DDL and data repairs
 * that need the new definitions. It runs after every new and changed routine is in place, and the step's integrity
 * gate runs after it; a part between the lines "-- before routines" and "-- after routines" runs before the routines
 * (a type or table a new signature uses). Name each install-script change it makes in a line "-- handles: <key>"
 * (make prints the keys). Emptied after each release.
 */
