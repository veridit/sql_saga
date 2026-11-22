/*
 * sql_saga 1.12 update: data repairs that need the 1.12 definitions. None: rows written by the PL/pgSQL planner under
 * MERGE_ENTITY_PATCH lost nulls nested inside jsonb values, which cannot be told apart from values written that way.
 */
