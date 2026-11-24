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

/*
 * 1.15: a natural key holds one value per entity (hug 40nwvn). Every natural key on a table with a primary key gets the
 * guards a fresh 1.15 add_unique_key builds: the 1.14 guard (a value belongs to one entity) and, for each natural key
 * column outside the primary key, EXCLUDE (stable primary key WITH =, column WITH <>) (an entity holds one value;
 * NULL before the key starts and after it is retired stays allowed). Rows that break it refuse the update, listed by
 * PostgreSQL's DETAIL with the repair; no row changes. A guard that already exists exactly as built is kept.
 */
DO $bind_natural_keys_both_ways$
DECLARE
    k record;
    result jsonb;
    cmds text[];
    g record;
    existing text;
    conflict text;
    n integer := 0;
BEGIN
    FOR k IN
        SELECT nk.unique_key_name, nk.table_schema, nk.table_name, nk.column_names, nk.mutually_exclusive_columns,
               pg_catalog.format('%I.%I', nk.table_schema, nk.table_name)::pg_catalog.regclass AS table_oid,
               ARRAY(SELECT c FROM pg_catalog.unnest(pk.column_names) AS c WHERE c <> e.range_column_name) AS stable_pk
        FROM sql_saga.unique_keys AS nk
        JOIN sql_saga.unique_keys AS pk ON (pk.table_schema, pk.table_name, pk.key_type) = (nk.table_schema, nk.table_name, 'primary')
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (nk.table_schema, nk.table_name, nk.era_name)
        WHERE nk.key_type = 'natural'
        ORDER BY nk.table_schema, nk.table_name, nk.unique_key_name
    LOOP
        result := sql_saga.__internal_generate_pk_consistency_constraints(
            k.unique_key_name, k.column_names, k.mutually_exclusive_columns, k.stable_pk);
        cmds := '{}';
        FOR g IN
            SELECT x.cmd, en.conname
            FROM pg_catalog.jsonb_array_elements_text(result->'cmds') WITH ORDINALITY AS x(cmd, n)
            JOIN pg_catalog.jsonb_array_elements_text(result->'names') WITH ORDINALITY AS en(conname, n) USING (n)
        LOOP
            existing := NULL;
            SELECT pg_catalog.format('ADD CONSTRAINT %I %s', c.conname, pg_catalog.pg_get_constraintdef(c.oid)) INTO existing
            FROM pg_catalog.pg_constraint AS c WHERE (c.conrelid, c.conname) = (k.table_oid, g.conname);
            IF existing IS NULL THEN
                cmds := cmds || g.cmd;
            ELSIF existing <> g.cmd THEN
                RAISE EXCEPTION 'sql_saga: constraint % on table %.% is not the guard that binds natural key % to its primary key',
                    pg_catalog.quote_ident(g.conname), pg_catalog.quote_ident(k.table_schema), pg_catalog.quote_ident(k.table_name),
                    pg_catalog.quote_ident(k.unique_key_name)
                    USING ERRCODE = 'duplicate_object', DETAIL = pg_catalog.format('Found: %s. Expected: %s.', existing, g.cmd),
                          HINT = 'The update needs that name for the guard. Rename or drop that constraint, then retry.';
            END IF;
        END LOOP;
        IF pg_catalog.cardinality(cmds) > 0 THEN
            BEGIN
                EXECUTE pg_catalog.format('ALTER TABLE %I.%I %s', k.table_schema, k.table_name, pg_catalog.array_to_string(cmds, ', '));
            EXCEPTION WHEN exclusion_violation THEN
                -- PostgreSQL's DETAIL names the two rows: one entity with two values of the natural key.
                GET STACKED DIAGNOSTICS conflict = PG_EXCEPTION_DETAIL;
                RAISE EXCEPTION 'sql_saga: natural key % on table %.% holds more than one value for an entity',
                    pg_catalog.quote_ident(k.unique_key_name), pg_catalog.quote_ident(k.table_schema), pg_catalog.quote_ident(k.table_name)
                    USING ERRCODE = 'exclusion_violation', DETAIL = conflict,
                          HINT = 'sql_saga 1.15 lets an entity hold one value of each natural key (NULL before it starts and after it is retired). An identifier that changes belongs in another natural key column. Before updating, correct the rows so that each entity holds one value, then retry.';
            END;
            n := n + 1;
        END IF;
        UPDATE sql_saga.unique_keys AS uk
        SET pk_consistency_constraint_names = ARRAY(SELECT pg_catalog.jsonb_array_elements_text(result->'names'))::name[]
        WHERE uk.unique_key_name = k.unique_key_name;
    END LOOP;
    IF n > 0 THEN
        RAISE NOTICE 'sql_saga: Bound % natural keys to one value per entity', n;
    END IF;
END;
$bind_natural_keys_both_ways$;
