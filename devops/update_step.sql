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

/* 1.15: the CHECK of a natural key with mutually exclusive columns is recorded in check_constraint (hug bjd8v7). */
-- handles: COMMENT ON COLUMN sql_saga.unique_keys.check_constraint
COMMENT ON COLUMN sql_saga.unique_keys.check_constraint IS 'For natural keys with mutually_exclusive_columns, the name of the CHECK (num_nonnulls(<columns>) <= 1) constraint: at most one of those columns holds a value per row (all NULL is allowed, before the key starts). NULL for other keys.';
-- handles: COMMENT ON COLUMN sql_saga.unique_keys.mutually_exclusive_columns
COMMENT ON COLUMN sql_saga.unique_keys.mutually_exclusive_columns IS 'For natural keys, the subset of columns that are mutually exclusive: at most one holds a value per row (enforced by check_constraint); uniqueness uses NULLS NOT DISTINCT.';

/*
 * 1.15: a natural key with mutually exclusive columns allows at most one of them per row (hug bjd8v7). Since 1.0 the
 * CHECK that said so was not built. Every such key gets the CHECK a fresh 1.15 add_unique_key builds, recorded in
 * check_constraint; one of that name and definition is adopted. A row giving two of the columns refuses the update,
 * shown by PostgreSQL's DETAIL with the repair; no row changes.
 */
DO $add_xor_checks$
DECLARE
    k record;
    def text;
    cname name;
    pass integer;
    conflict text;
    n integer := 0;
BEGIN
    FOR k IN
        SELECT uk.unique_key_name, uk.table_schema, uk.table_name, uk.mutually_exclusive_columns,
               pg_catalog.format('%I.%I', uk.table_schema, uk.table_name)::pg_catalog.regclass AS table_oid
        FROM sql_saga.unique_keys AS uk
        WHERE uk.mutually_exclusive_columns IS NOT NULL
        ORDER BY uk.table_schema, uk.table_name, uk.unique_key_name
    LOOP
        def := sql_saga.__internal_xor_check_definition(k.mutually_exclusive_columns);
        cname := sql_saga.__internal_make_name(ARRAY[k.unique_key_name], 'xor_check');
        pass := 0;
        WHILE EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE c.conrelid = k.table_oid AND c.conname = cname || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END
              AND pg_catalog.pg_get_constraintdef(c.oid) <> def)
        LOOP
            pass := pass + 1;
        END LOOP;
        cname := cname || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE (c.conrelid, c.conname) = (k.table_oid, cname)) THEN
            BEGIN
                EXECUTE pg_catalog.format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s', k.table_schema, k.table_name, cname, def);
            EXCEPTION WHEN check_violation THEN
                -- ALTER TABLE ADD CONSTRAINT names no row; DETAIL lists the first five, as the preflight does.
                EXECUTE pg_catalog.format(
                    'SELECT pg_catalog.string_agg(r, '', '') FROM (SELECT pg_catalog.format(''(%%s)'', concat_ws('', '', %1$s)) AS r FROM %2$I.%3$I WHERE num_nonnulls(%1$s) > 1 ORDER BY 1 LIMIT 5) AS x',
                    (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(c), ', ') FROM pg_catalog.unnest(k.mutually_exclusive_columns) AS c),
                    k.table_schema, k.table_name)
                INTO conflict;
                conflict := pg_catalog.format('Rows giving (%s): %s.', pg_catalog.array_to_string(k.mutually_exclusive_columns, ', '), conflict);
                RAISE EXCEPTION 'sql_saga: natural key % on table %.% has rows giving more than one of its mutually exclusive columns %',
                    pg_catalog.quote_ident(k.unique_key_name), pg_catalog.quote_ident(k.table_schema), pg_catalog.quote_ident(k.table_name),
                    pg_catalog.array_to_string(k.mutually_exclusive_columns, ', ')
                    USING ERRCODE = 'check_violation', DETAIL = conflict,
                          HINT = pg_catalog.format('sql_saga 1.15 enforces at most one of them per row. Find the rows with SELECT * FROM %I.%I WHERE num_nonnulls(%s) > 1; correct them so that each gives at most one, then retry.',
                              k.table_schema, k.table_name,
                              (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(c), ', ') FROM pg_catalog.unnest(k.mutually_exclusive_columns) AS c));
            END;
            n := n + 1;
        END IF;
        UPDATE sql_saga.unique_keys AS uk SET check_constraint = cname WHERE uk.unique_key_name = k.unique_key_name;
    END LOOP;
    IF n > 0 THEN
        RAISE NOTICE 'sql_saga: Added the mutually exclusive column CHECK to % natural keys', n;
    END IF;
END;
$add_xor_checks$;

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
