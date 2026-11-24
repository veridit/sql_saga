/*
 * The hand-written part of the current release's update step (devops/update_step.py): catalog DDL and data repairs
 * that need the new definitions. It runs after every new and changed routine is in place, and the step's integrity
 * gate runs after it. Name each install-script change it makes in a line "-- handles: <key>" (make prints the keys).
 * Emptied after each release.
 */

/*
 * 1.14: every natural key is bound to its table's primary key (hug mnntde). Natural keys that an earlier release left
 * unbound (an explicit enforce_consistency_with_primary_key => false, or a composite primary key) get the guard a fresh
 * 1.14 add_unique_key builds. Rows that break it refuse the update, listed with the repair; no row changes.
 * Through 1.13, a natural key added before its primary key got its guard but kept the catalog value '{}' (the update
 * of the catalog row matched the primary key's name); such a guard, exactly as add_unique_key builds it, is recorded
 * instead of built again.
 */
DO $bind_natural_keys$
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
               nk.pk_consistency_constraint_names,
               pg_catalog.format('%I.%I', nk.table_schema, nk.table_name)::pg_catalog.regclass AS table_oid,
               ARRAY(SELECT c FROM pg_catalog.unnest(pk.column_names) AS c WHERE c <> e.range_column_name) AS stable_pk
        FROM sql_saga.unique_keys AS nk
        JOIN sql_saga.unique_keys AS pk ON (pk.table_schema, pk.table_name, pk.key_type) = (nk.table_schema, nk.table_name, 'primary')
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (nk.table_schema, nk.table_name, nk.era_name)
        WHERE nk.key_type = 'natural'
          AND (nk.pk_consistency_constraint_names IS NULL OR nk.pk_consistency_constraint_names = '{}')
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
                -- PostgreSQL's DETAIL names the two rows: one natural key value under two primary key values.
                GET STACKED DIAGNOSTICS conflict = PG_EXCEPTION_DETAIL;
                RAISE EXCEPTION 'sql_saga: natural key % on table %.% is not bound to its primary key: a natural key value belongs to more than one primary key value',
                    pg_catalog.quote_ident(k.unique_key_name), pg_catalog.quote_ident(k.table_schema), pg_catalog.quote_ident(k.table_name)
                    USING ERRCODE = 'exclusion_violation', DETAIL = conflict,
                          HINT = 'sql_saga 1.14 binds every natural key value to one primary key value. Before updating, correct the rows so that each natural key value belongs to one entity, then retry.';
            END;
            n := n + 1;
        END IF;
        UPDATE sql_saga.unique_keys AS uk
        SET pk_consistency_constraint_names = ARRAY(SELECT pg_catalog.jsonb_array_elements_text(result->'names'))::name[]
        WHERE uk.unique_key_name = k.unique_key_name;
    END LOOP;
    IF n > 0 THEN
        RAISE NOTICE 'sql_saga: Bound % natural keys to their primary keys', n;
    END IF;
END;
$bind_natural_keys$;

/*
 * 1.14: jsonb provenance values that 1.9 stored as a JSON string become the object they encode (hug k5zndr).
 * 1.9 evaluated each provenance DEFAULT as text, so a jsonb provenance column got "{\"by\": ...}" instead of
 * {"by": ...}. A provenance value comes only from the column's DEFAULT (the trigger overwrites a supplied one), so a
 * JSON string that parses to an object can only be that defect; anything else is left as it is. The rows of the
 * history table of a system-versioned table are repaired too (write_history copied the value).
 * The repair is a storage correction, not a change of data: no trigger of the table fires (the provenance trigger
 * would put the old value back, write_history would add a history row, generated_always would move system time).
 * Triggers are disabled only on a table that has such a value, and each gets its exact firing mode back.
 */
DO $repair_jsonb_provenance$
DECLARE
    rel record;
    trg record;
    fixable text;
    sets text;
    found_one boolean;
    n bigint;
    total bigint := 0;
    modes jsonb;
BEGIN
    FOR rel IN
        WITH prov AS (
            SELECT DISTINCT e.table_schema, e.table_name, p.col
            FROM sql_saga.era AS e
            CROSS JOIN LATERAL pg_catalog.unnest(e.provenance_columns) AS p(col)
        ), targets AS (
            SELECT c.oid AS rel_oid, pr.col FROM prov AS pr
            JOIN pg_catalog.pg_namespace AS ns ON ns.nspname = pr.table_schema
            JOIN pg_catalog.pg_class AS c ON (c.relnamespace, c.relname) = (ns.oid, pr.table_name)
            UNION
            SELECT h.oid, pr.col FROM prov AS pr
            JOIN sql_saga.system_versioning AS sv ON (sv.table_schema, sv.table_name) = (pr.table_schema, pr.table_name)
            JOIN pg_catalog.pg_namespace AS hn ON hn.nspname = sv.history_schema_name
            JOIN pg_catalog.pg_class AS h ON (h.relnamespace, h.relname) = (hn.oid, sv.history_table_name)
        )
        SELECT t.rel_oid, pg_catalog.array_agg(t.col ORDER BY t.col) AS cols
        FROM targets AS t
        JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (t.rel_oid, t.col) AND a.attnum > 0 AND NOT a.attisdropped
        WHERE 'pg_catalog.jsonb'::pg_catalog.regtype IN (
            WITH RECURSIVE base(oid) AS (
                SELECT a.atttypid
                UNION ALL
                SELECT ty.typbasetype FROM base JOIN pg_catalog.pg_type AS ty ON ty.oid = base.oid WHERE ty.typtype = 'd')
            SELECT base.oid FROM base)
        GROUP BY t.rel_oid
        ORDER BY t.rel_oid::pg_catalog.regclass::text
    LOOP
        -- A JSON string whose text is a JSON object (the parse is guarded, so other strings never raise).
        SELECT pg_catalog.string_agg(pg_catalog.format(
                   '(pg_catalog.jsonb_typeof(%1$I) = ''string'' AND CASE WHEN pg_catalog.pg_input_is_valid(%1$I #>> ''{}'', ''jsonb'') THEN pg_catalog.jsonb_typeof((%1$I #>> ''{}'')::pg_catalog.jsonb) = ''object'' ELSE false END)', c), ' OR '),
               pg_catalog.string_agg(pg_catalog.format(
                   '%1$I = CASE WHEN pg_catalog.jsonb_typeof(%1$I) = ''string'' AND CASE WHEN pg_catalog.pg_input_is_valid(%1$I #>> ''{}'', ''jsonb'') THEN pg_catalog.jsonb_typeof((%1$I #>> ''{}'')::pg_catalog.jsonb) = ''object'' ELSE false END THEN (%1$I #>> ''{}'')::pg_catalog.jsonb ELSE %1$I END', c), ', ')
        INTO fixable, sets
        FROM pg_catalog.unnest(rel.cols) AS c;
        EXECUTE pg_catalog.format('SELECT EXISTS (SELECT FROM %s WHERE %s)', rel.rel_oid::pg_catalog.regclass, fixable) INTO STRICT found_one;
        IF NOT found_one THEN
            CONTINUE;
        END IF;
        SELECT coalesce(pg_catalog.jsonb_object_agg(t.tgname, t.tgenabled), '{}') INTO modes
        FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = rel.rel_oid AND NOT t.tgisinternal AND t.tgenabled <> 'D';
        FOR trg IN SELECT k AS tgname FROM pg_catalog.jsonb_object_keys(modes) AS k ORDER BY k LOOP
            EXECUTE pg_catalog.format('ALTER TABLE %s DISABLE TRIGGER %I', rel.rel_oid::pg_catalog.regclass, trg.tgname);
        END LOOP;
        EXECUTE pg_catalog.format('UPDATE %s SET %s WHERE %s', rel.rel_oid::pg_catalog.regclass, sets, fixable);
        GET DIAGNOSTICS n = ROW_COUNT;
        total := total + n;
        FOR trg IN SELECT m.key AS tgname, m.value #>> '{}' AS mode FROM pg_catalog.jsonb_each(modes) AS m ORDER BY m.key LOOP
            EXECUTE pg_catalog.format('ALTER TABLE %s %s TRIGGER %I', rel.rel_oid::pg_catalog.regclass,
                CASE trg.mode WHEN 'O' THEN 'ENABLE' WHEN 'A' THEN 'ENABLE ALWAYS' WHEN 'R' THEN 'ENABLE REPLICA' END, trg.tgname);
        END LOOP;
    END LOOP;
    IF total > 0 THEN
        RAISE NOTICE 'sql_saga: Converted the jsonb provenance values of % rows from the JSON strings sql_saga 1.9 stored into the objects they encode', total;
    END IF;
END;
$repair_jsonb_provenance$;
