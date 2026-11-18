/*
 * sql_saga 1.8 update: repairs after the 1.8 definitions are loaded (devops/generate_update_scripts_1_8.py).
 * The generator emits the final integrity gate right after this.
 *
 * None: 1.8 drops two never-written catalog columns (devops/upgrade_1_8_schema.sql) and changes four routines.
 * The final integrity gate checks the persistence of every history table, which the 1.8 health check now
 * guards; a history table made UNLOGGED earlier refuses the update, naming ALTER TABLE ... SET LOGGED.
 */
