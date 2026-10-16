/* Internal integrity and attribution helpers are implementation details. */
REVOKE ALL ON FUNCTION sql_saga.__internal_system_versioning_query_functions(oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_system_versioning_function_is_attributable(oid, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_system_versioning_member_attribution(oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_trigger_is_attributable(oid, name, regprocedure, integer, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_system_versioning_view_is_attributable(oid, oid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_unique_key_inventory(name) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_unique_key_attribution(name) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_temporal_foreign_key_is_attributable(name) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_unique_key_object_is_attributable(name, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_display_name(name, name) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.check_integrity() FROM PUBLIC;
