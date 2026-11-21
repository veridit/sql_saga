/*
 * Provenance columns (1.9, doc/design/provenance.md): ephemeral columns of an era that sql_saga fills from their
 * DEFAULT on changed and new pieces, and keeps on untouched pieces.
 *
 * sql_saga.era.provenance_columns lists them. Each must also be in the era's ephemeral_columns (so a change of
 * writer or time never counts as a change of data) and must have a DEFAULT. A non-empty list has one managed row
 * trigger on the table, "<era>_provenance", which applies the rule to every write:
 *
 *   INSERT, and an UPDATE that changes a non-ephemeral column: every provenance column gets its DEFAULT, evaluated
 *     now as the role running the write, whatever the statement supplied;
 *   an UPDATE that changes no non-ephemeral column (the period moved, or only ephemeral columns changed): every
 *     provenance column keeps OLD's value, whatever the statement supplied;
 *   an INSERT of an untouched piece by temporal_merge (the remainder of a split): kept as written. The executor
 *     marks those statements with the transaction-local setting sql_saga.provenance_carry = '<table oid>'. A caller
 *     could set it by hand: like sql_saga.rebuilding_system_versioning_views it guards against mistakes and is not
 *     an authorization (doc/design/provenance.md, "Not in this subset").
 *
 * A NOT NULL provenance column whose DEFAULT gives NULL for the writing role refuses the write, naming the column
 * and the role, instead of the generic not-null error.
 */

CREATE FUNCTION sql_saga.provenance_trigger()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
AS $provenance_trigger$
DECLARE
    v_era_name name := TG_ARGV[0];
    v_provenance name[];
    v_ephemeral name[];
    v_new jsonb;
    v_old jsonb;
    v_data_changed boolean;
    v_col name;
    v_default text;
    v_notnull boolean;
    v_value text;
    v_set jsonb := '{}'::jsonb;
BEGIN
    SELECT e.provenance_columns, e.ephemeral_columns
    INTO v_provenance, v_ephemeral
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (TG_TABLE_SCHEMA, TG_TABLE_NAME, v_era_name);
    IF coalesce(cardinality(v_provenance), 0) = 0 THEN
        RETURN NEW;
    END IF;

    v_new := pg_catalog.to_jsonb(NEW);
    IF TG_OP = 'UPDATE' THEN
        v_old := pg_catalog.to_jsonb(OLD);
        -- The period columns are not ephemeral, but moving a piece is not a change of its data.
        SELECT EXISTS (
            SELECT FROM pg_catalog.jsonb_each(v_new) AS n(key, value)
            WHERE n.value IS DISTINCT FROM v_old -> n.key
              AND n.key <> ALL (coalesce(v_ephemeral, '{}'))
              AND n.key NOT IN (SELECT c FROM sql_saga.era AS e,
                                     LATERAL (VALUES (e.range_column_name), (e.valid_from_column_name),
                                                     (e.valid_until_column_name), (e.valid_to_column_name)) AS t(c)
                                WHERE (e.table_schema, e.table_name) = (TG_TABLE_SCHEMA, TG_TABLE_NAME) AND c IS NOT NULL))
        INTO v_data_changed;
        IF NOT v_data_changed THEN
            SELECT pg_catalog.jsonb_object_agg(p.col, v_old -> p.col) INTO v_set FROM pg_catalog.unnest(v_provenance) AS p(col);
            NEW := pg_catalog.jsonb_populate_record(NEW, v_set);
            RETURN NEW;
        END IF;
    ELSIF coalesce(pg_catalog.current_setting('sql_saga.provenance_carry', true), '') = TG_RELID::text THEN
        -- An INSERT by temporal_merge: the planner gave an untouched piece its old values and a changed or new piece
        -- none, so keep what is there and fill only what is missing.
        SELECT coalesce(pg_catalog.array_agg(p.col), '{}') INTO v_provenance
        FROM pg_catalog.unnest(v_provenance) AS p(col) WHERE v_new -> p.col IS NULL OR v_new -> p.col = 'null'::jsonb;
    END IF;

    FOREACH v_col IN ARRAY v_provenance LOOP
        SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid), a.attnotnull
        INTO v_default, v_notnull
        FROM pg_catalog.pg_attribute AS a
        LEFT JOIN pg_catalog.pg_attrdef AS d ON (d.adrelid, d.adnum) = (a.attrelid, a.attnum)
        WHERE (a.attrelid, a.attname) = (TG_RELID, v_col) AND NOT a.attisdropped;
        IF v_default IS NULL THEN
            v_value := NULL;
        ELSE
            EXECUTE pg_catalog.format('SELECT (%s)::text', v_default) INTO v_value;
        END IF;
        IF v_value IS NULL AND v_notnull THEN
            RAISE EXCEPTION 'sql_saga: provenance column "%" of table "%": its DEFAULT gave NULL for the writing role %',
                v_col, pg_catalog.format('%s.%s', TG_TABLE_SCHEMA, TG_TABLE_NAME), current_user
                USING ERRCODE = 'not_null_violation',
                      HINT = 'The DEFAULT must name the writer for every role that writes this table.';
        END IF;
        v_set := v_set || pg_catalog.jsonb_build_object(v_col, v_value);
    END LOOP;
    NEW := pg_catalog.jsonb_populate_record(NEW, v_set);
    RETURN NEW;
END;
$provenance_trigger$;

COMMENT ON FUNCTION sql_saga.provenance_trigger() IS
'Managed row trigger "<era>_provenance" of an era with provenance columns (doc/design/provenance.md): a new piece and a change of data get each provenance column''s DEFAULT, evaluated as the writing role; a write that only moves the period keeps the old values.';

/*
 * Validate and store an era's provenance columns, and create or drop its managed trigger. Called by the workers of
 * add_era and set_era_provenance_columns, inside their owner check and serialization, so it runs as the extension
 * owner; called directly it fails on sql_saga.era. NULL or an empty list removes them.
 */
CREATE FUNCTION sql_saga.__internal_set_era_provenance_columns(table_oid regclass, era_name name, provenance_columns name[])
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog, pg_temp
AS $__internal_set_era_provenance_columns$
#variable_conflict use_variable
DECLARE
    v_schema name;
    v_table name;
    v_ephemeral name[];
    v_col name;
    v_trigger name := pg_catalog.format('%s_provenance', era_name);
    v_list name[] := NULLIF(provenance_columns, '{}');
BEGIN
    SELECT n.nspname, c.relname INTO v_schema, v_table
    FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;
    SELECT e.ephemeral_columns INTO v_ephemeral
    FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name, e.era_name) = (v_schema, v_table, era_name);
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Era "%" not found on table %', era_name, table_oid;
    END IF;

    FOREACH v_col IN ARRAY coalesce(v_list, '{}') LOOP
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                       WHERE (a.attrelid, a.attname) = (table_oid, v_col) AND a.attnum > 0 AND NOT a.attisdropped) THEN
            RAISE EXCEPTION 'column "%" does not exist in table %', v_col, table_oid;
        END IF;
        IF v_col <> ALL (coalesce(v_ephemeral, '{}')) THEN
            RAISE EXCEPTION 'sql_saga: provenance column "%" of table "%" is not an ephemeral column of era "%"',
                v_col, sql_saga.__internal_display_name(table_oid), era_name
                USING ERRCODE = 'invalid_parameter_value',
                      HINT = 'Add it to ephemeral_columns first (set_era_ephemeral_columns): writer and time must not count as a change of data.';
        END IF;
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                       JOIN pg_catalog.pg_attrdef AS d ON (d.adrelid, d.adnum) = (a.attrelid, a.attnum)
                       WHERE (a.attrelid, a.attname) = (table_oid, v_col)) THEN
            RAISE EXCEPTION 'sql_saga: provenance column "%" of table "%" has no DEFAULT', v_col, sql_saga.__internal_display_name(table_oid)
                USING ERRCODE = 'invalid_parameter_value',
                      HINT = pg_catalog.format('sql_saga fills a provenance column from its DEFAULT. Give it one, e.g. ALTER TABLE %s ALTER COLUMN %I SET DEFAULT ...',
                                               sql_saga.__internal_display_name(table_oid), v_col);
        END IF;
    END LOOP;

    UPDATE sql_saga.era AS e SET provenance_columns = v_list
    WHERE (e.table_schema, e.table_name, e.era_name) = (v_schema, v_table, era_name);

    IF v_list IS NULL THEN
        IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t
                   WHERE t.tgrelid = table_oid AND t.tgname = v_trigger AND t.tgfoid = 'sql_saga.provenance_trigger()'::regprocedure) THEN
            PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format('DROP TRIGGER %I ON %s', v_trigger, table_oid));
            RAISE NOTICE 'sql_saga: Dropped trigger "%" on table "%" for the provenance columns of era "%"', v_trigger, sql_saga.__internal_display_name(table_oid), era_name;
        END IF;
    ELSIF NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t
                      WHERE t.tgrelid = table_oid AND t.tgname = v_trigger AND t.tgfoid = 'sql_saga.provenance_trigger()'::regprocedure) THEN
        PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format(
            'CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION sql_saga.provenance_trigger(%L)',
            v_trigger, table_oid, era_name));
        RAISE NOTICE 'sql_saga: Created trigger "%" on table "%" for the provenance columns of era "%"', v_trigger, sql_saga.__internal_display_name(table_oid), era_name;
    END IF;
END;
$__internal_set_era_provenance_columns$;

CREATE FUNCTION sql_saga.__api_set_era_provenance_columns(
    table_class regclass,
    era_name name DEFAULT 'valid',
    provenance_columns name[] DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, pg_temp
AS $set_era_provenance_columns$
#variable_conflict use_variable
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    PERFORM sql_saga.__internal_require_owner(table_class, 'set_era_provenance_columns');
    PERFORM sql_saga.__internal_serialize(table_class);
    PERFORM sql_saga.__internal_set_era_provenance_columns(table_class, era_name, provenance_columns);
END;
$set_era_provenance_columns$;

COMMENT ON FUNCTION sql_saga.__api_set_era_provenance_columns IS
'Sets the provenance columns of an era (doc/design/provenance.md): ephemeral columns that sql_saga fills from their DEFAULT on changed and new pieces, evaluated as the writing role, and keeps on untouched pieces. Each must be in the era''s ephemeral_columns and have a DEFAULT. Creates the managed trigger "<era>_provenance"; NULL or an empty list removes the columns and the trigger.

Parameters:
- table_class: The table OID or name
- era_name: The era name (defaults to ''valid'')
- provenance_columns: Array of column names, or NULL to remove';
