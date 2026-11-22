/*
 * sql_saga 1.11 update: data repairs that need the 1.11 definitions. None: the typed decoder changes what later
 * temporal_merge calls write; rows written before are not inspected (a jsonb string written for a new entity's later
 * slice by the native planner cannot be told apart from a deliberate JSON string).
 */
