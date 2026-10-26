CREATE OR REPLACE FUNCTION sql_saga.__find_or_create_fk_index(
    p_fk_table_oid regclass,
    p_fk_column_names name[],
    p_fk_era_row sql_saga.era,
    p_type sql_saga.fg_type,
    p_create_index boolean
) RETURNS name
LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    v_fk_schema_name name;
    v_fk_table_name name;
    v_index_def text;
    v_index_name name;
    v_index_type name;
    v_existing_index_name name;
    v_fk_attnums smallint[];
BEGIN
    SELECT n.nspname, c.relname INTO v_fk_schema_name, v_fk_table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = p_fk_table_oid;

    SELECT array_agg(a.attnum ORDER BY u.ord) INTO v_fk_attnums
    FROM unnest(p_fk_column_names) WITH ORDINALITY u(col, ord)
    JOIN pg_attribute a ON a.attrelid = p_fk_table_oid AND a.attname = u.col;

    IF p_type = 'temporal_to_temporal' THEN
        -- For temporal FKs, we need a GiST index on the columns and the era range.
        -- Finding a compatible index is very complex due to expression indexes.
        -- This check is intentionally basic: it finds a GiST index where the FK columns are a prefix.
        -- It does not validate the rest of the index (e.g., the range expression), as that is not
        -- feasible in PL/pgSQL. It is a best-effort check to avoid creating a redundant index.
        v_index_type := 'GIST';
        v_index_name := sql_saga.__internal_make_name(
            ARRAY[v_fk_table_name] || p_fk_column_names || ARRAY[p_fk_era_row.era_name, 'gist', 'idx']
        );
        v_index_def := format(
            'CREATE INDEX %I ON %s USING GIST (%s, %I)',
            v_index_name,
            p_fk_table_oid::text,
            (SELECT string_agg(quote_ident(c), ', ' ORDER BY o) FROM unnest(p_fk_column_names) WITH ORDINALITY AS u (c, o)),
            p_fk_era_row.range_column_name
        );

        SELECT c.relname INTO v_existing_index_name
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid = c.relam
        WHERE i.indrelid = p_fk_table_oid
          AND i.indisvalid AND am.amname = 'gist'
          AND i.indnkeyatts >= array_length(v_fk_attnums, 1)
          AND (i.indkey::smallint[])[0:array_length(v_fk_attnums, 1)-1] = v_fk_attnums
        LIMIT 1;

    ELSE -- regular_to_temporal
        v_index_type := 'BTREE';
        v_index_name := sql_saga.__internal_make_name(
            ARRAY[v_fk_table_name] || p_fk_column_names || ARRAY['idx']
        );
        v_index_def := format(
            'CREATE INDEX %I ON %s USING BTREE (%s)',
            v_index_name,
            p_fk_table_oid::text,
            (SELECT string_agg(quote_ident(c), ', ' ORDER BY o) FROM unnest(p_fk_column_names) WITH ORDINALITY AS u (c, o))
        );

        -- Look for an existing btree index where the FK columns are a prefix.
        SELECT c.relname INTO v_existing_index_name
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid = c.relam
        WHERE i.indrelid = p_fk_table_oid
          AND i.indisvalid AND am.amname = 'btree'
          AND i.indnkeyatts >= array_length(v_fk_attnums, 1)
          AND (i.indkey::smallint[])[0:array_length(v_fk_attnums, 1)-1] = v_fk_attnums
        LIMIT 1;
    END IF;

    IF v_existing_index_name IS NOT NULL THEN
        RAISE DEBUG 'Found existing compatible index "%" for foreign key on table %', v_existing_index_name, p_fk_table_oid::text;
        RETURN NULL; -- Compatible index exists, do nothing.
    END IF;

    IF p_create_index THEN
        RAISE NOTICE 'sql_saga: No compatible index found for foreign key on table %. Creating new index: %', p_fk_table_oid::text, v_index_def;
        EXECUTE v_index_def;
        RETURN v_index_name;
    ELSE
        RAISE WARNING 'No index found on table % for foreign key columns (%). Performance may be poor.',
            p_fk_table_oid::text, p_fk_column_names
        USING HINT = format('Consider creating this index: %s', v_index_def);
        RETURN NULL;
    END IF;
END;
$function$;

-- Declarative wrapper for creating foreign keys.
-- This function automatically determines whether to create a temporal or regular FK
-- and looks up the internal unique key name. fk_era_name is only needed to disambiguate
-- when the FK table has multiple eras.
CREATE FUNCTION sql_saga.__api_add_foreign_key(
        fk_table_oid regclass,
        fk_column_names name[],
        pk_table_oid regclass,
        pk_column_names name[],
        fk_era_name name DEFAULT NULL,
        match_type sql_saga.fk_match_types DEFAULT 'SIMPLE',
        update_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        delete_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        foreign_key_name name DEFAULT NULL,
        create_index boolean DEFAULT true
)
RETURNS name
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function_declarative_fk$
#variable_conflict use_variable
DECLARE
    v_fk_schema_name name;
    v_fk_table_name name;
    v_pk_schema_name name;
    v_pk_table_name name;
    v_is_fk_temporal boolean;
    v_unique_key_name name;
    v_key_ordered_fk_columns name[];
    v_fk_era_name name := fk_era_name;
BEGIN
    -- Get schema and table names for both tables
    SELECT n.nspname, c.relname INTO v_fk_schema_name, v_fk_table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = fk_table_oid;

    SELECT n.nspname, c.relname INTO v_pk_schema_name, v_pk_table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = pk_table_oid;

    -- Look up the unique key name based on the PK table and columns.
    -- We match on primary or natural keys, as they are the intended targets for FKs.
    -- The ENUM order ('primary', 'natural', ...) ensures we prefer primary keys.
    -- pk_column_names may list the key's columns in any order: they are compared as a set, and the child
    -- columns, paired with them by position as given, are put into the key's own column order, which is the
    -- order add_regular_foreign_key and add_temporal_foreign_key pair them in.
    SELECT uk.unique_key_name,
           ARRAY(SELECT fk_column_names[pg_catalog.array_position(pk_column_names, u.c)]
                 FROM unnest(uk.column_names) WITH ORDINALITY AS u (c, o)
                 ORDER BY u.o)
    INTO v_unique_key_name, v_key_ordered_fk_columns
    FROM sql_saga.unique_keys uk
    WHERE uk.table_schema = v_pk_schema_name
      AND uk.table_name = v_pk_table_name
      AND cardinality(uk.column_names) = cardinality(pk_column_names)
      AND ARRAY(SELECT c FROM unnest(uk.column_names) AS c ORDER BY c COLLATE "C")
        = ARRAY(SELECT c FROM unnest(pk_column_names) AS c ORDER BY c COLLATE "C")
      AND uk.key_type IN ('primary', 'natural')
    ORDER BY uk.key_type
    LIMIT 1;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'No primary or natural unique key found on table %.% for columns %',
            quote_ident(v_pk_schema_name), quote_ident(v_pk_table_name), pk_column_names;
    END IF;

    IF cardinality(fk_column_names) = cardinality(pk_column_names) THEN
        fk_column_names := v_key_ordered_fk_columns;
    END IF;

    -- Determine if the FK table is temporal
    SELECT EXISTS (
        SELECT 1 FROM sql_saga.era e
        WHERE (e.table_schema, e.table_name) = (v_fk_schema_name, v_fk_table_name)
    ) INTO v_is_fk_temporal;

    IF v_is_fk_temporal THEN
        -- If fk_era_name is not provided, try to auto-detect it.
        IF v_fk_era_name IS NULL THEN
            SELECT e.era_name INTO v_fk_era_name
            FROM sql_saga.era e
            WHERE (e.table_schema, e.table_name) = (v_fk_schema_name, v_fk_table_name);

            -- If there's more than one era, the user must specify which one to use.
            IF (SELECT count(*) FROM sql_saga.era e WHERE (e.table_schema, e.table_name) = (v_fk_schema_name, v_fk_table_name)) > 1 THEN
                RAISE EXCEPTION 'Table %.% has multiple eras. Please specify the fk_era_name parameter.',
                    quote_ident(v_fk_schema_name), quote_ident(v_fk_table_name);
            END IF;
        END IF;

        RETURN sql_saga.add_temporal_foreign_key(
            fk_table_oid => fk_table_oid,
            fk_column_names => fk_column_names,
            fk_era_name => v_fk_era_name,
            unique_key_name => v_unique_key_name,
            match_type => match_type,
            update_action => update_action,
            delete_action => delete_action,
            foreign_key_name => foreign_key_name,
            create_index => create_index
        );
    ELSE
        RETURN sql_saga.add_regular_foreign_key(
            fk_table_oid => fk_table_oid,
            fk_column_names => fk_column_names,
            unique_key_name => v_unique_key_name,
            match_type => match_type,
            update_action => update_action,
            delete_action => delete_action,
            foreign_key_name => foreign_key_name,
            create_index => create_index
        );
    END IF;
END;
$function_declarative_fk$;


/*
 * The referencing side of a regular foreign key: a constraint trigger on the child table, created from the
 * sql_saga.foreign_keys row of the given foreign key, under its fk_check_constraint name:
 *     CREATE CONSTRAINT TRIGGER <fk_check_constraint> AFTER INSERT OR UPDATE ON <child> FROM <parent>
 *     DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION sql_saga.regular_fk_check_c(<arguments>)
 * The arguments are those of the referenced-side triggers (sql_saga.__internal_regular_fk_trigger_args), so
 * the child and referenced columns are paired by position. Unlike a CHECK constraint reading the parent,
 * pg_dump emits the trigger as a separate post-data item, so a restore loads the data of both tables before
 * it exists, whatever order it loads them in. It fires on every UPDATE, not only UPDATE OF the key columns:
 * a deferred check skips a row version that is no longer live, so the version that replaces it (also through
 * an UPDATE of other columns) must queue a check of its own (as PostgreSQL's RI_FKey_fk_upd_check_required
 * does). The new trigger fires in origin mode.
 *
 * Shared by add_regular_foreign_key and the rename following that re-creates the trigger. It neither
 * validates existing rows nor writes the catalog nor raises notices; it refuses when the child table already
 * has a trigger or constraint of that name.
 */
CREATE FUNCTION sql_saga.__internal_create_regular_fk_child_trigger(foreign_key_name name)
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_create_regular_fk_child_trigger$
#variable_conflict use_variable
DECLARE
    fk sql_saga.foreign_keys;
    uk sql_saga.unique_keys;
    uk_era sql_saga.era;
    child_oid regclass;
BEGIN
    SELECT f.* INTO fk FROM sql_saga.foreign_keys AS f WHERE f.foreign_key_name = foreign_key_name;
    IF NOT FOUND OR fk.type IS DISTINCT FROM 'regular_to_temporal' THEN
        RAISE EXCEPTION 'sql_saga: regular foreign key "%" not found', foreign_key_name;
    END IF;
    IF fk.fk_check_constraint IS NULL OR fk.fk_check_constraint = '' THEN
        RAISE EXCEPTION 'sql_saga: regular foreign key "%" names no referencing-side trigger', foreign_key_name;
    END IF;
    SELECT u.* INTO uk FROM sql_saga.unique_keys AS u WHERE u.unique_key_name = fk.unique_key_name;
    SELECT e.* INTO uk_era FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name);
    IF uk_era.era_name IS NULL OR pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name)) IS NULL THEN
        RAISE EXCEPTION 'sql_saga: referenced table of foreign key "%" not found', foreign_key_name;
    END IF;
    child_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', fk.table_schema, fk.table_name));
    IF child_oid IS NULL THEN
        RAISE EXCEPTION 'sql_saga: referencing table of foreign key "%" not found', foreign_key_name;
    END IF;
    IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = child_oid AND t.tgname = fk.fk_check_constraint)
       OR EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = child_oid AND c.conname = fk.fk_check_constraint) THEN
        RAISE EXCEPTION 'sql_saga: cannot create the referencing-side trigger of foreign key %: table % already has a trigger or constraint named %',
            pg_catalog.quote_ident(fk.foreign_key_name), child_oid, pg_catalog.quote_ident(fk.fk_check_constraint)
            USING ERRCODE = 'duplicate_object';
    END IF;

    EXECUTE pg_catalog.format($$
        CREATE CONSTRAINT TRIGGER %1$I AFTER INSERT OR UPDATE ON %2$I.%3$I FROM %4$I.%5$I DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION
        sql_saga.regular_fk_check_c(%6$L, %2$L, %3$L, %7$L, '', '', '', %4$L, %5$L, %8$L, %9$L, %10$L, %11$L, %12$L, %13$L, %14$L, 'regular_to_temporal');
        $$
        , /* %1$I */ fk.fk_check_constraint
        , /* %2$I */ fk.table_schema
        , /* %3$I */ fk.table_name
        , /* %4$I */ uk.table_schema
        , /* %5$I */ uk.table_name
        , /* %6$L */ fk.foreign_key_name
        , /* %7$L */ fk.column_names::text
        , /* %8$L */ uk.column_names::text
        , /* %9$L */ uk_era.era_name
        , /* %10$L*/ coalesce(uk_era.valid_from_column_name::text, 'null')
        , /* %11$L*/ coalesce(uk_era.valid_until_column_name::text, 'null')
        , /* %12$L*/ fk.match_type
        , /* %13$L*/ fk.update_action
        , /* %14$L*/ fk.delete_action
    );
END;
$__internal_create_regular_fk_child_trigger$;

REVOKE ALL ON FUNCTION sql_saga.__internal_create_regular_fk_child_trigger(name) FROM PUBLIC;

/*
 * The TRUNCATE trigger of a regular foreign key on the referenced (parent) table (1.4), from its catalog row:
 *     CREATE TRIGGER <uk_truncate_trigger> AFTER TRUNCATE ON <parent> FOR EACH STATEMENT
 *     EXECUTE FUNCTION sql_saga.uk_truncate_check_c(<the same 17 arguments as the other regular FK triggers>)
 * The one definition, used by add_regular_foreign_key, by the rebuild that follows column renames and by the
 * update to 1.4.
 */
CREATE FUNCTION sql_saga.__internal_create_regular_fk_truncate_trigger(foreign_key_name name)
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_create_regular_fk_truncate_trigger$
#variable_conflict use_variable
DECLARE
    fk sql_saga.foreign_keys;
    uk sql_saga.unique_keys;
    uk_era sql_saga.era;
    parent_oid regclass;
BEGIN
    SELECT f.* INTO fk FROM sql_saga.foreign_keys AS f WHERE f.foreign_key_name = foreign_key_name;
    IF NOT FOUND OR fk.type IS DISTINCT FROM 'regular_to_temporal' THEN
        RAISE EXCEPTION 'sql_saga: regular foreign key "%" not found', foreign_key_name;
    END IF;
    IF fk.uk_truncate_trigger IS NULL OR fk.uk_truncate_trigger = '' THEN
        RAISE EXCEPTION 'sql_saga: regular foreign key "%" names no TRUNCATE trigger', foreign_key_name;
    END IF;
    SELECT u.* INTO uk FROM sql_saga.unique_keys AS u WHERE u.unique_key_name = fk.unique_key_name;
    SELECT e.* INTO uk_era FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name);
    parent_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name));
    IF uk_era.era_name IS NULL OR parent_oid IS NULL THEN
        RAISE EXCEPTION 'sql_saga: referenced table of foreign key "%" not found', foreign_key_name;
    END IF;
    IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = parent_oid AND t.tgname = fk.uk_truncate_trigger) THEN
        RAISE EXCEPTION 'sql_saga: cannot create the TRUNCATE trigger of foreign key %: table % already has a trigger named %',
            pg_catalog.quote_ident(fk.foreign_key_name), parent_oid, pg_catalog.quote_ident(fk.uk_truncate_trigger)
            USING ERRCODE = 'duplicate_object';
    END IF;

    EXECUTE pg_catalog.format($$
        CREATE TRIGGER %1$I AFTER TRUNCATE ON %4$I.%5$I FOR EACH STATEMENT EXECUTE FUNCTION
        sql_saga.uk_truncate_check_c(%6$L, %2$L, %3$L, %7$L, '', '', '', %4$L, %5$L, %8$L, %9$L, %10$L, %11$L, %12$L, %13$L, %14$L, 'regular_to_temporal');
        $$
        , /* %1$I */ fk.uk_truncate_trigger
        , /* %2$L */ fk.table_schema
        , /* %3$L */ fk.table_name
        , /* %4$I */ uk.table_schema
        , /* %5$I */ uk.table_name
        , /* %6$L */ fk.foreign_key_name
        , /* %7$L */ fk.column_names::text
        , /* %8$L */ uk.column_names::text
        , /* %9$L */ uk_era.era_name
        , /* %10$L*/ coalesce(uk_era.valid_from_column_name::text, 'null')
        , /* %11$L*/ coalesce(uk_era.valid_until_column_name::text, 'null')
        , /* %12$L*/ fk.match_type
        , /* %13$L*/ fk.update_action
        , /* %14$L*/ fk.delete_action
    );
END;
$__internal_create_regular_fk_truncate_trigger$;

REVOKE ALL ON FUNCTION sql_saga.__internal_create_regular_fk_truncate_trigger(name) FROM PUBLIC;


-- Overloaded function for regular (non-temporal) to temporal foreign keys
CREATE FUNCTION sql_saga.__api_add_regular_foreign_key(
        fk_table_oid regclass,
        fk_column_names name[],
        unique_key_name name,
        match_type sql_saga.fk_match_types DEFAULT 'SIMPLE',
        update_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        delete_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        foreign_key_name name DEFAULT NULL,
        fk_check_constraint name DEFAULT NULL,
        fk_helper_function text DEFAULT NULL,
        uk_update_trigger name DEFAULT NULL,
        uk_delete_trigger name DEFAULT NULL,
        create_index boolean DEFAULT true)
 RETURNS name
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function_regular_fk$
#variable_conflict use_variable
DECLARE
    uk_era_row sql_saga.era;
    uk_row sql_saga.unique_keys;
    fk_schema_name name;
    fk_table_name name;
    fk_table_columns_snapshot name[];
    uk_table_oid regclass;
    uk_schema_name name;
    uk_table_name name;
    pass integer;
    unique_columns_with_era_columns text;
    uk_where_clause text;
    v_fk_index_name name;
    v_uk_stable_cols name[];
    v_missing_equality boolean;
BEGIN
    IF fk_table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;
    /* As ALTER TABLE ... ADD FOREIGN KEY: own the referencing table; REFERENCES on the referenced one (below). */
    PERFORM sql_saga.__internal_require_owner(fk_table_oid, 'add_foreign_key');

    PERFORM sql_saga.__internal_serialize(fk_table_oid);

    SELECT n.nspname, c.relname INTO fk_schema_name, fk_table_name
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = fk_table_oid;

    SELECT array_agg(a.attname ORDER BY a.attnum) INTO fk_table_columns_snapshot
    FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = fk_table_oid AND a.attnum > 0 AND NOT a.attisdropped;

    /*
     * A partitioned table would get the referencing trigger cloned onto its partitions, which ATTACH PARTITION does
     * not validate, and sql_saga supports no partitioned tables. A partition can reference by itself. Like a native
     * foreign key, a regular foreign key covers only its own tables, not inheritance children (ONLY on both sides).
     */
    IF (SELECT c.relkind FROM pg_catalog.pg_class AS c WHERE c.oid = fk_table_oid) = 'p' THEN
        RAISE EXCEPTION 'table "%" is partitioned; a regular foreign key cannot reference from a partitioned table',
            sql_saga.__internal_display_name(fk_table_oid)
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'Add the foreign key to each partition instead.';
    END IF;

    -- Verify that the referencing table is NOT temporal.
    IF EXISTS (
        SELECT 1 FROM sql_saga.era e
        WHERE (e.table_schema, e.table_name) = (fk_schema_name, fk_table_name)
    ) THEN
        RAISE EXCEPTION 'Table %.% is a temporal table. Use the temporal-to-temporal version of add_temporal_foreign_key by providing fk_era_name.',
            quote_ident(fk_schema_name), quote_ident(fk_table_name);
    END IF;

    /* Get the unique key we're linking to */
    SELECT uk.* INTO uk_row
    FROM sql_saga.unique_keys AS uk
    WHERE uk.unique_key_name = unique_key_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'unique key "%" does not exist', unique_key_name;
    END IF;

    /*
     * A predicated key is unique only among the rows its predicate admits, and the key lookups (child check, the
     * referenced side's last-row rule, the cascade) do not apply the predicate. A temporal foreign key cannot
     * reference one either (it has no unique constraint).
     */
    IF uk_row.key_type = 'predicated' THEN
        RAISE EXCEPTION 'unique key "%" is a predicated key; a regular foreign key can only reference a primary or natural key', unique_key_name
            USING ERRCODE = 'feature_not_supported';
    END IF;

    /* Get the unique key's era */
    SELECT e.* INTO uk_era_row
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (uk_row.table_schema, uk_row.table_name, uk_row.era_name);

    -- For regular-to-temporal FKs, we only care about the stable (non-temporal) part of the key.
    SELECT array_agg(c ORDER BY o) INTO v_uk_stable_cols
    FROM unnest(uk_row.column_names) WITH ORDINALITY AS u (c, o)
    WHERE c <> uk_era_row.range_column_name;

    uk_schema_name := uk_row.table_schema;
    uk_table_name := uk_row.table_name;
    uk_table_oid := format('%I.%I', uk_schema_name /* %I */, uk_table_name /* %I */)::regclass;
    -- As ADD FOREIGN KEY ... REFERENCES t (key columns): table-level or per-column REFERENCES (Q3).
    PERFORM sql_saga.__internal_require_references(uk_table_oid, v_uk_stable_cols);

    v_fk_index_name := sql_saga.__find_or_create_fk_index(
        fk_table_oid,
        fk_column_names,
        NULL, -- no era row for regular fk
        'regular_to_temporal',
        create_index
    );

    /* Check that all the columns match */
    IF array_length(fk_column_names, 1) <> array_length(v_uk_stable_cols, 1) THEN
        RAISE EXCEPTION 'foreign key column count (%) does not match unique key stable column count (%)',
             array_length(fk_column_names, 1), array_length(v_uk_stable_cols, 1);
    END IF;

    IF EXISTS (
        SELECT FROM unnest(fk_column_names, v_uk_stable_cols) AS u (fk_attname, uk_attname)
        JOIN pg_catalog.pg_attribute AS fa ON (fa.attrelid, fa.attname) = (fk_table_oid, u.fk_attname)
        JOIN pg_catalog.pg_attribute AS ua ON (ua.attrelid, ua.attname) = (uk_table_oid, u.uk_attname)
        WHERE (fa.atttypid, fa.atttypmod, fa.attcollation) <> (ua.atttypid, ua.atttypmod, ua.attcollation))
    THEN
        RAISE EXCEPTION 'column types do not match';
    END IF;

    /* Generate a name for the foreign constraint. */
    IF foreign_key_name IS NULL THEN
        foreign_key_name := sql_saga.__internal_make_name(
            ARRAY[fk_table_name] || fk_column_names || ARRAY['fkey']);
    END IF;
    pass := 0;
    WHILE EXISTS (
       SELECT FROM sql_saga.foreign_keys AS fk
       WHERE fk.foreign_key_name = foreign_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END)
    LOOP
       pass := pass + 1;
    END LOOP;
    foreign_key_name := foreign_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;

    /* The referencing side is created after the catalog row (__internal_create_regular_fk_child_trigger). */
    IF match_type = 'PARTIAL' THEN
        RAISE EXCEPTION 'sql_saga: MATCH PARTIAL is not supported for regular foreign keys'
            USING ERRCODE = 'feature_not_supported';
    END IF;
    IF delete_action IN ('SET NULL', 'SET DEFAULT') THEN
        RAISE EXCEPTION 'sql_saga: ON DELETE % is not supported for regular foreign keys', delete_action
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'Use NO ACTION, RESTRICT or CASCADE.';
    END IF;
    IF update_action NOT IN ('NO ACTION', 'RESTRICT') THEN
        RAISE EXCEPTION 'sql_saga: ON UPDATE % is not supported for regular foreign keys', update_action
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'Use NO ACTION or RESTRICT.';
    END IF;
    IF fk_helper_function IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: fk_helper_function is no longer used: since 1.3 a regular foreign key has no helper function'
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'Omit fk_helper_function.';
    END IF;

    SELECT bool_or(eq.oid IS NULL)
    INTO v_missing_equality
    FROM unnest(v_uk_stable_cols) AS u(name)
    JOIN pg_catalog.pg_attribute AS ua ON (ua.attrelid, ua.attname) = (uk_table_oid, u.name)
    LEFT JOIN LATERAL (
        SELECT o.oid
        FROM pg_catalog.pg_opclass AS opc
        JOIN pg_catalog.pg_am AS am ON am.oid = opc.opcmethod AND am.amname = 'btree'
        JOIN pg_catalog.pg_amop AS amop ON amop.amopfamily = opc.opcfamily
             AND amop.amoplefttype = opc.opcintype AND amop.amoprighttype = opc.opcintype
             AND amop.amopstrategy = 3
        JOIN pg_catalog.pg_operator AS o ON o.oid = amop.amopopr
        WHERE opc.opcdefault
          AND opc.opcintype = (SELECT coalesce(nullif(t.typbasetype, 0), t.oid)
                               FROM pg_catalog.pg_type AS t WHERE t.oid = ua.atttypid)
        LIMIT 1
    ) AS eq ON true;
    IF v_missing_equality THEN
        RAISE EXCEPTION 'a column of unique key % has a type without a default btree equality operator', unique_key_name;
    END IF;

    fk_check_constraint := coalesce(fk_check_constraint, sql_saga.__internal_make_name(
        ARRAY[fk_table_name] || fk_column_names || ARRAY['check']
    ));
    -- Two foreign keys on the same child columns (to different keys) would generate the same name.
    pass := 0;
    WHILE EXISTS (
        SELECT FROM pg_catalog.pg_constraint AS c
        WHERE c.conrelid = fk_table_oid
          AND c.conname = fk_check_constraint || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END)
       OR EXISTS (
        SELECT FROM pg_catalog.pg_trigger AS t
        WHERE t.tgrelid = fk_table_oid
          AND t.tgname = fk_check_constraint || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END)
    LOOP
        pass := pass + 1;
    END LOOP;
    fk_check_constraint := fk_check_constraint || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;

    /* Get the columns that require checking the constraint on the UK table */
    SELECT string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality)
    INTO unique_columns_with_era_columns
    FROM unnest(uk_row.column_names || uk_era_row.valid_from_column_name || uk_era_row.valid_until_column_name) WITH ORDINALITY AS u (column_name, ordinality);

    DECLARE
        fk_column_names_arr_str text := fk_column_names::text;
        uk_column_names_arr_str text := uk_row.column_names::text;
    BEGIN
        uk_update_trigger := coalesce(uk_update_trigger, sql_saga.__internal_make_name(ARRAY[foreign_key_name], 'uk_update'));
        EXECUTE format($$
            CREATE CONSTRAINT TRIGGER %1$I AFTER UPDATE OF %2$s ON %3$I.%4$I FROM %5$I.%6$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
            sql_saga.uk_update_check_c(%7$L, %5$L, %6$L, %8$L, %9$L, %10$L, %11$L, %3$L, %4$L, %12$L, %13$L, %14$L, %15$L, %16$L, %17$L, %18$L, 'regular_to_temporal');
            $$
            , /* %1$I */ uk_update_trigger
            , /* %2$s */ unique_columns_with_era_columns
            , /* %3$I */ uk_schema_name
            , /* %4$I */ uk_table_name
            , /* %5$I */ fk_schema_name
            , /* %6$I */ fk_table_name
            -- Parameters for C function
            , /* %7$L */ foreign_key_name
            , /* %8$L */ fk_column_names_arr_str
            , /* %9$L */ '' -- fk_era_name
            , /* %10$L*/ '' -- fk_valid_from_column_name
            , /* %11$L*/ '' -- fk_valid_until_column_name
            , /* %12$L*/ uk_column_names_arr_str
            , /* %13$L*/ uk_era_row.era_name
            , /* %14$L*/ uk_era_row.valid_from_column_name
            , /* %15$L*/ uk_era_row.valid_until_column_name
            , /* %16$L*/ match_type
            , /* %17$L*/ update_action
            , /* %18$L*/ delete_action
        );

        uk_delete_trigger := coalesce(uk_delete_trigger, sql_saga.__internal_make_name(ARRAY[foreign_key_name], 'uk_delete'));
        EXECUTE format($$
            CREATE CONSTRAINT TRIGGER %1$I AFTER DELETE ON %2$I.%3$I FROM %4$I.%5$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
            sql_saga.uk_delete_check_c(%6$L, %4$L, %5$L, %7$L, %8$L, %9$L, %10$L, %2$L, %3$L, %11$L, %12$L, %13$L, %14$L, %15$L, %16$L, %17$L, 'regular_to_temporal');
            $$
            , /* %1$I */ uk_delete_trigger
            , /* %2$I */ uk_schema_name
            , /* %3$I */ uk_table_name
            , /* %4$I */ fk_schema_name
            , /* %5$I */ fk_table_name
            -- Parameters for C function
            , /* %6$L */ foreign_key_name
            , /* %7$L */ fk_column_names_arr_str
            , /* %8$L */ '' -- fk_era_name
            , /* %9$L */ '' -- fk_valid_from_column_name
            , /* %10$L*/ '' -- fk_valid_until_column_name
            , /* %11$L*/ uk_column_names_arr_str
            , /* %12$L*/ uk_era_row.era_name
            , /* %13$L*/ uk_era_row.valid_from_column_name
            , /* %14$L*/ uk_era_row.valid_until_column_name
            , /* %15$L*/ match_type
            , /* %16$L*/ update_action
            , /* %17$L*/ delete_action
        );
    END;

    INSERT INTO sql_saga.foreign_keys
        ( foreign_key_name, type,              table_schema,   table_name,    column_names,    fk_table_columns_snapshot, unique_key_name, match_type, update_action, delete_action, fk_check_constraint, fk_helper_function, uk_update_trigger, uk_delete_trigger, uk_truncate_trigger, fk_index_name)
    VALUES
        ( foreign_key_name, 'regular_to_temporal', fk_schema_name, fk_table_name, fk_column_names, fk_table_columns_snapshot, unique_key_name, match_type, update_action, delete_action, fk_check_constraint, NULL, uk_update_trigger, uk_delete_trigger,
          sql_saga.__internal_make_name(ARRAY[foreign_key_name], 'uk_truncate'), v_fk_index_name);

    /* The referencing side, from the catalog row just written (see __internal_create_regular_fk_child_trigger). */
    PERFORM sql_saga.__internal_create_regular_fk_child_trigger(foreign_key_name);
    /* TRUNCATE of the parent alone is refused while a child row references it (1.4). */
    PERFORM sql_saga.__internal_create_regular_fk_truncate_trigger(foreign_key_name);


    /* Validate the constraint on existing data. */
    DECLARE
        violating_row_found boolean;
        fk_not_null_clause text;
    BEGIN
        -- MATCH SIMPLE checks rows whose key columns are all non-NULL, MATCH FULL every row that is not all NULL
        -- (a partly NULL key is a violation then).
        SELECT CASE WHEN match_type = 'FULL'
                    THEN 'NOT (' || string_agg(format('fk.%I IS NULL', u.fkc /* %I */), ' AND ' ORDER BY u.o) || ')'
                    ELSE string_agg(format('fk.%I IS NOT NULL', u.fkc /* %I */), ' AND ' ORDER BY u.o) END
        INTO fk_not_null_clause
        FROM unnest(fk_column_names) WITH ORDINALITY AS u(fkc, o);

        -- The comparison the trigger uses: the default btree equality operator of each column type.
        SELECT string_agg(format('uk.%I OPERATOR(%I.=) fk.%I', u.ukc /* %I */, eq.nspname /* %I */, u.fkc /* %I */), ' AND ' ORDER BY u.o)
        INTO uk_where_clause
        FROM unnest(v_uk_stable_cols, fk_column_names) WITH ORDINALITY AS u(ukc, fkc, o)
        JOIN pg_catalog.pg_attribute AS ua ON (ua.attrelid, ua.attname) = (uk_table_oid, u.ukc)
        CROSS JOIN LATERAL (
            SELECT n.nspname
            FROM pg_catalog.pg_opclass AS opc
            JOIN pg_catalog.pg_am AS am ON am.oid = opc.opcmethod AND am.amname = 'btree'
            JOIN pg_catalog.pg_amop AS amop ON amop.amopfamily = opc.opcfamily
                 AND amop.amoplefttype = opc.opcintype AND amop.amoprighttype = opc.opcintype
                 AND amop.amopstrategy = 3
            JOIN pg_catalog.pg_operator AS o ON o.oid = amop.amopopr
            JOIN pg_catalog.pg_namespace AS n ON n.oid = o.oprnamespace
            WHERE opc.opcdefault
              AND opc.opcintype = (SELECT coalesce(nullif(t.typbasetype, 0), t.oid)
                                   FROM pg_catalog.pg_type AS t WHERE t.oid = ua.atttypid)
            LIMIT 1
        ) AS eq;

        EXECUTE format('SELECT EXISTS( ' ||
            'SELECT 1 FROM ONLY %1$I.%2$I AS fk ' ||
            'WHERE %3$s AND NOT EXISTS ( ' ||
            '  SELECT 1 FROM ONLY %4$I.%5$I AS uk WHERE %6$s' ||
            '))',
            fk_schema_name,       -- %1$I
            fk_table_name,        -- %2$I
            fk_not_null_clause,   -- %3$s
            uk_schema_name,       -- %4$I
            uk_table_name,        -- %5$I
            uk_where_clause       -- %6$s
        ) INTO violating_row_found;

        IF violating_row_found THEN
            RAISE EXCEPTION 'insert or update on table "%" violates foreign key constraint "%"',
                sql_saga.__internal_display_name(fk_table_oid),
                foreign_key_name
                USING ERRCODE = 'foreign_key_violation',
                      DETAIL = 'A row of the table references a key that is not present in the referenced table.';
        END IF;
    END;


    RETURN foreign_key_name;
END;
$function_regular_fk$;


-- Original function for temporal-to-temporal FKs
-- This has been refactored to use native PostgreSQL 18+ temporal foreign keys.
-- The trigger-based implementation has been removed. uk_update_trigger and uk_delete_trigger are unused
-- and kept for signature compatibility.
CREATE OR REPLACE FUNCTION sql_saga.__api_add_temporal_foreign_key(
        fk_table_oid regclass,
        fk_column_names name[],
        fk_era_name name,
        unique_key_name name,
        match_type sql_saga.fk_match_types DEFAULT 'SIMPLE',
        update_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        delete_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        foreign_key_name name DEFAULT NULL,
        uk_update_trigger name DEFAULT NULL,
        uk_delete_trigger name DEFAULT NULL,
        create_index boolean DEFAULT true)
 RETURNS name
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    fk_era_row sql_saga.era;
    uk_era_row sql_saga.era;
    uk_row sql_saga.unique_keys;
    fk_schema_name name;
    fk_table_name name;
    fk_table_columns_snapshot name[];
    uk_table_oid regclass;
    uk_schema_name name;
    uk_table_name name;
    pass integer;
    v_fk_index_name name;
    v_uk_stable_cols_list text;
    v_uk_stable_cols name[];
BEGIN
    IF fk_table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;
    /* As ALTER TABLE ... ADD FOREIGN KEY: own the referencing table; REFERENCES on the referenced one (below). */
    PERFORM sql_saga.__internal_require_owner(fk_table_oid, 'add_foreign_key');

    IF delete_action IN ('CASCADE', 'SET NULL', 'SET DEFAULT') OR update_action IN ('CASCADE', 'SET NULL', 'SET DEFAULT') THEN
        RAISE EXCEPTION 'sql_saga: ON % % is not supported for temporal foreign keys',
            CASE WHEN delete_action IN ('CASCADE', 'SET NULL', 'SET DEFAULT') THEN 'DELETE' ELSE 'UPDATE' END,
            CASE WHEN delete_action IN ('CASCADE', 'SET NULL', 'SET DEFAULT') THEN delete_action ELSE update_action END
            USING ERRCODE = 'feature_not_supported',
                  DETAIL = 'PostgreSQL does not support referential actions on foreign keys using PERIOD.',
                  HINT = 'Use NO ACTION or RESTRICT. ON DELETE CASCADE is supported for foreign keys from a regular table.';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(fk_table_oid);

    SELECT n.nspname, c.relname INTO fk_schema_name, fk_table_name
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = fk_table_oid;

    SELECT array_agg(a.attname ORDER BY a.attnum) INTO fk_table_columns_snapshot
    FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = fk_table_oid AND a.attnum > 0 AND NOT a.attisdropped;

    /* Get the period involved */
    SELECT e.*
    INTO fk_era_row
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (fk_schema_name, fk_table_name, fk_era_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'era "%" does not exist on table %', fk_era_name, fk_table_oid;
    END IF;

    /* Get the unique key we're linking to */
    SELECT uk.*
    INTO uk_row
    FROM sql_saga.unique_keys AS uk
    WHERE uk.unique_key_name = unique_key_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'unique key "%" does not exist', unique_key_name;
    END IF;

    /* Get the unique key's era */
    SELECT e.*
    INTO uk_era_row
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (uk_row.table_schema, uk_row.table_name, uk_row.era_name);

    SELECT array_agg(c ORDER BY o), string_agg(quote_ident(c), ', ' ORDER BY o) -- in constraint order, so the native FK pairs position i with key column i
    INTO v_uk_stable_cols, v_uk_stable_cols_list
    FROM unnest(sql_saga.__internal_unique_key_constraint_columns(uk_row.unique_key_name)) WITH ORDINALITY AS u (c, o)
    WHERE c <> uk_era_row.range_column_name;

    IF fk_era_row.range_type <> uk_era_row.range_type THEN
        RAISE EXCEPTION 'era range types for foreign key and unique key do not match';
    END IF;

    uk_schema_name := uk_row.table_schema;
    uk_table_name := uk_row.table_name;
    uk_table_oid := format('%I.%I', uk_schema_name, uk_table_name)::regclass;
    -- As ADD FOREIGN KEY ... REFERENCES t (key columns, PERIOD range): table-level or per-column REFERENCES (Q3).
    PERFORM sql_saga.__internal_require_references(uk_table_oid, v_uk_stable_cols || uk_era_row.range_column_name);

    /* Check that all the columns match */
    IF array_length(fk_column_names, 1) <> array_length(v_uk_stable_cols, 1) THEN
        RAISE EXCEPTION 'foreign key column count (%) does not match unique key stable column count (%)',
             array_length(fk_column_names, 1), array_length(v_uk_stable_cols, 1);
    END IF;

    IF EXISTS (
        SELECT FROM unnest(fk_column_names, v_uk_stable_cols) AS u (fk_attname, uk_attname)
        JOIN pg_catalog.pg_attribute AS fa ON (fa.attrelid, fa.attname) = (fk_table_oid, u.fk_attname)
        JOIN pg_catalog.pg_attribute AS ua ON (ua.attrelid, ua.attname) = (uk_table_oid, u.uk_attname)
        WHERE (fa.atttypid, fa.atttypmod, fa.attcollation) <> (ua.atttypid, ua.atttypmod, ua.attcollation))
    THEN
        RAISE EXCEPTION 'column types do not match';
    END IF;

    /* Generate a name for the foreign constraint. */
    IF foreign_key_name IS NULL THEN
        foreign_key_name := sql_saga.__internal_make_name(
            ARRAY[fk_table_name] || fk_column_names || ARRAY[fk_era_name]);
    END IF;
    pass := 0;
    WHILE EXISTS (
       SELECT FROM sql_saga.foreign_keys AS fk
       WHERE fk.foreign_key_name = foreign_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END)
    LOOP
       pass := pass + 1;
    END LOOP;
    foreign_key_name := foreign_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;

    v_fk_index_name := sql_saga.__find_or_create_fk_index(
        fk_table_oid,
        fk_column_names,
        fk_era_row,
        'temporal_to_temporal',
        create_index
    );

    /* Create the native temporal foreign key. This automatically validates existing data. */
    EXECUTE format($$
        ALTER TABLE %1$s ADD CONSTRAINT %2$I
        FOREIGN KEY (%3$s, PERIOD %4$I) REFERENCES %5$s (%6$s)
        MATCH %7$s ON UPDATE %8$s ON DELETE %9$s
        DEFERRABLE
        $$,
        fk_table_oid::text,                                                              /* %1$s */
        foreign_key_name,                                                                /* %2$I */
        (SELECT string_agg(quote_ident(c), ', ' ORDER BY o) FROM unnest(fk_column_names) WITH ORDINALITY AS u (c, o)), /* %3$s */
        fk_era_row.range_column_name,                                                    /* %4$I */
        uk_table_oid::text,                                                              /* %5$s */
        CASE
            WHEN v_uk_stable_cols_list IS NULL THEN format('PERIOD %I', uk_era_row.range_column_name)
            ELSE format('%s, PERIOD %I', v_uk_stable_cols_list, uk_era_row.range_column_name)
        END,                                                                             /* %6$s */
        match_type,                                                                      /* %7$s */
        update_action,                                                                   /* %8$s */
        delete_action                                                                    /* %9$s */
    );

    INSERT INTO sql_saga.foreign_keys
        ( foreign_key_name, type, table_schema, table_name, column_names, fk_era_name, fk_table_columns_snapshot, unique_key_name, match_type, update_action, delete_action, fk_index_name,
          uk_update_trigger, uk_delete_trigger)
    VALUES
        ( foreign_key_name, 'temporal_to_temporal', fk_schema_name, fk_table_name, fk_column_names, fk_era_name, fk_table_columns_snapshot, uk_row.unique_key_name, match_type, update_action, delete_action, v_fk_index_name,
          NULL, NULL);

    RETURN foreign_key_name;
END;
$function$;

COMMENT ON FUNCTION sql_saga.__api_add_regular_foreign_key(regclass, name[], name, sql_saga.fk_match_types, sql_saga.fk_actions, sql_saga.fk_actions, name, name, text, name, name, boolean) IS
'Adds a foreign key from a regular (non-temporal) table to a temporal table. It ensures that any referenced key exists at some point in the target''s history. Since 1.3 the referencing-side check is a DEFERRABLE INITIALLY IMMEDIATE constraint trigger; fk_check_constraint names that trigger and its constraint, not a CHECK expression. fk_helper_function remains in the signature for call compatibility but must be NULL: no helper function is created, and an explicit one is refused with SQLSTATE 0A000. MATCH SIMPLE and MATCH FULL are supported; MATCH PARTIAL is refused with SQLSTATE 0A000. delete_action NO ACTION (the default) and RESTRICT refuse deleting the last parent row that has a referenced key; CASCADE (since 1.4) deletes the child rows of that key instead. Deleting one timeline row while another row of the same key remains never cascades, and temporal_merge reshaping a timeline does not either. SET NULL and SET DEFAULT, and every update_action other than NO ACTION and RESTRICT, are refused with SQLSTATE 0A000.';

COMMENT ON FUNCTION sql_saga.__api_add_temporal_foreign_key(regclass, name[], name, name, sql_saga.fk_match_types, sql_saga.fk_actions, sql_saga.fk_actions, name, name, name, boolean) IS
'Adds a temporal foreign key from one temporal table to another. It ensures that for any given time slice in the referencing table, a corresponding valid time slice exists in the referenced table.';

COMMENT ON FUNCTION sql_saga.__api_add_foreign_key(regclass, name[], regclass, name[], name, sql_saga.fk_match_types, sql_saga.fk_actions, sql_saga.fk_actions, name, boolean) IS
'Adds a foreign key constraint. This is a declarative wrapper that automatically determines whether to create a temporal or regular foreign key by introspecting the schema, and looks up the internal unique key name. The two column arrays are paired positionally. The referenced columns may be listed in another order than the registered unique key, provided they name the same set; both arrays are normalized together to the unique key''s order, preserving each child-to-parent pairing.';
