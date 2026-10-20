\echo Use "ALTER EXTENSION sql_saga UPDATE TO '1.2'" to load this file. \quit
/*
 * 1.2 adds a parameter to add_system_versioning, so this script replaces the 1.1 function. Its
 * privileges cannot be carried over: GRANT and REVOKE in an extension script become extension initial
 * privileges, which pg_dump omits. So this check runs first: it compares the 1.1 function's effective
 * EXECUTE privileges (grantor ignored, grant options included) with what its replacement will get.
 * That is read from a probe function, created with the current default privileges (global and schema
 * ones alike) and dropped again immediately. The probe exists only briefly inside this update's
 * transaction, as a member of the extension like everything the script creates, and never survives
 * it. When the privileges differ the update is refused, and the refusal rolls the whole transaction
 * back, probe included, leaving the database exactly as before. The hint gives the exact statements:
 * reset the whole privilege set of the 1.1 function to the replacement's (a cascading REVOKE also
 * removes grants made through a grant option, so every non-owner grantee is revoked and the complete
 * target set granted again), update, then do the same on the new signature with the original set.
 */
DO $check_add_system_versioning_acl$
DECLARE
    old_proc CONSTANT text := 'sql_saga.add_system_versioning(regclass, name, name, name, name, name, name)';
    new_proc CONSTANT text := 'sql_saga.add_system_versioning(regclass, name, name, name, name, name, name, boolean)';
    probe CONSTANT text := 'sql_saga.__sql_saga_1_2_upgrade_acl_probe()';
    probe_acl pg_catalog.aclitem[];
    probe_owner pg_catalog.oid;
    differs boolean;
    old_grantees text;
    new_grantees text;
    old_grants text;
    new_grants text;
    cascade_word CONSTANT text := 'CAS' || 'CADE';
BEGIN
    IF pg_catalog.to_regprocedure(old_proc) IS NULL THEN
        RETURN;
    END IF;
    IF pg_catalog.to_regprocedure(probe) IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: function % already exists; the 1.2 update needs that name for a temporary probe', probe
            USING ERRCODE = 'duplicate_function',
                  HINT = 'Drop or rename that function, then retry the update.';
    END IF;
    EXECUTE 'CREATE FUNCTION ' || probe || ' RETURNS void LANGUAGE sql AS ''SELECT''';
    SELECT p.proacl, p.proowner INTO probe_acl, probe_owner
    FROM pg_catalog.pg_proc AS p
    WHERE p.oid = pg_catalog.to_regprocedure(probe);
    EXECUTE 'DROP FUNCTION ' || probe;

    WITH old_set AS (
        SELECT a.grantee, pg_catalog.bool_or(a.is_grantable) AS grantable
        FROM pg_catalog.pg_proc AS p
        CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS a
        WHERE p.oid = pg_catalog.to_regprocedure(old_proc)
          AND a.privilege_type = 'EXECUTE'
          AND a.grantee OPERATOR(pg_catalog.<>) p.proowner
        GROUP BY a.grantee
    ), new_set AS (
        SELECT a.grantee, pg_catalog.bool_or(a.is_grantable) AS grantable
        FROM pg_catalog.aclexplode(coalesce(probe_acl, pg_catalog.acldefault('f', probe_owner))) AS a
        WHERE a.privilege_type = 'EXECUTE'
          AND a.grantee OPERATOR(pg_catalog.<>) probe_owner
        GROUP BY a.grantee
    ), both_sets AS (
        SELECT 'old' AS side, o.grantee, o.grantable FROM old_set AS o
        UNION ALL
        SELECT 'new', n.grantee, n.grantable FROM new_set AS n
    ), named AS (
        SELECT b.side, b.grantable, b.grantee = 0 AS is_public,
               CASE WHEN b.grantee = 0 THEN 'PUBLIC' ELSE b.grantee::pg_catalog.regrole::text END AS who
        FROM both_sets AS b
    )
    SELECT
        EXISTS (SELECT FROM old_set AS o FULL JOIN new_set AS n ON n.grantee = o.grantee
                WHERE o.grantable IS DISTINCT FROM n.grantable),
        pg_catalog.string_agg(nm.who, ', ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'old'),
        pg_catalog.string_agg(nm.who, ', ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'new'),
        pg_catalog.string_agg(pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO %s%s;', new_proc, nm.who,
                                                CASE WHEN nm.grantable THEN ' WITH GRANT OPTION' ELSE '' END),
                              ' ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'old'),
        pg_catalog.string_agg(pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO %s%s;', old_proc, nm.who,
                                                CASE WHEN nm.grantable THEN ' WITH GRANT OPTION' ELSE '' END),
                              ' ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'new')
    INTO differs, old_grantees, new_grantees, old_grants, new_grants
    FROM named AS nm;

    IF differs THEN
        RAISE EXCEPTION 'sql_saga: the privileges on % differ from those its 1.2 replacement gets; the update replaces this function, and cannot carry changed privileges over', old_proc
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = pg_catalog.concat_ws(' ',
                      'Reset them, update, and apply them to the new signature after the update. Run:',
                      CASE WHEN old_grantees IS NOT NULL THEN pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %s %s;', old_proc, old_grantees, cascade_word) END,
                      new_grants,
                      'ALTER EXTENSION sql_saga UPDATE TO ''1.2'';',
                      CASE WHEN new_grantees IS NOT NULL THEN pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %s %s;', new_proc, new_grantees, cascade_word) END,
                      old_grants);
    END IF;
END;
$check_add_system_versioning_acl$;

/* Validate every cataloged regular-FK helper before any update mutation. */
DO $validate_fk_helpers$
DECLARE
    h record;
    p pg_catalog.pg_proc;
    v11_where_clause text;
    expected_v11_body text;
    expected_argtypes pg_catalog.oidvector;
    expected_owner pg_catalog.oid;
    missing_equality boolean;
BEGIN
    FOR h IN
        SELECT fk.fk_helper_function, uk.table_schema, uk.table_name, tc.relowner,
               pg_catalog.array_agg(c.name ORDER BY c.ordinality) AS stable_columns
        -- One row per helper and key: regular foreign keys on the same key share one helper, and
        -- aggregating per foreign key would repeat the key columns.
        FROM (SELECT DISTINCT f.fk_helper_function, f.unique_key_name, f.type FROM sql_saga.foreign_keys AS f) AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        JOIN sql_saga.era AS e
          ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_catalog.pg_class AS tc
          ON tc.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name))
        CROSS JOIN LATERAL pg_catalog.unnest(uk.column_names) WITH ORDINALITY AS c(name, ordinality)
        WHERE fk.type = 'regular_to_temporal'
          AND c.name OPERATOR(pg_catalog.<>) e.range_column_name
        GROUP BY fk.fk_helper_function, uk.table_schema, uk.table_name, tc.relowner
        ORDER BY fk.fk_helper_function
    LOOP
        SELECT pr.* INTO p
        FROM pg_catalog.pg_proc AS pr
        WHERE pr.oid = pg_catalog.to_regprocedure(h.fk_helper_function);
        IF NOT FOUND THEN
            RAISE EXCEPTION 'sql_saga: regular foreign key helper % does not exist', h.fk_helper_function
                USING ERRCODE = 'undefined_function',
                      HINT = 'Restore the exact helper generated by the released sql_saga 1.1, or recreate the foreign key with sql_saga, then retry the upgrade.';
        END IF;

        SELECT pg_catalog.string_agg(pg_catalog.format('uk.%I OPERATOR(%I.=) $%s',
                                                       u.name, eq.nspname, u.ordinality),
                                     ' AND ' ORDER BY u.ordinality),
               pg_catalog.array_to_string(pg_catalog.array_agg(a.atttypid ORDER BY u.ordinality), ' ')::pg_catalog.oidvector,
               pg_catalog.bool_or(eq.oid IS NULL)
        INTO v11_where_clause, expected_argtypes, missing_equality
        FROM pg_catalog.unnest(h.stable_columns) WITH ORDINALITY AS u(name, ordinality)
        JOIN pg_catalog.pg_class AS c
          ON c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', h.table_schema, h.table_name))
        JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (c.oid, u.name)
        LEFT JOIN LATERAL (
            SELECT o.oid, n.nspname
            FROM pg_catalog.pg_opclass AS opc
            JOIN pg_catalog.pg_am AS am ON am.oid = opc.opcmethod AND am.amname = 'btree'
            JOIN pg_catalog.pg_amop AS amop ON amop.amopfamily = opc.opcfamily
                 AND amop.amoplefttype = opc.opcintype AND amop.amoprighttype = opc.opcintype
                 AND amop.amopstrategy = 3
            JOIN pg_catalog.pg_operator AS o ON o.oid = amop.amopopr
            JOIN pg_catalog.pg_namespace AS n ON n.oid = o.oprnamespace
            WHERE opc.opcdefault
              AND opc.opcintype = (SELECT CASE WHEN t.typbasetype = 0 THEN t.oid ELSE t.typbasetype END
                                   FROM pg_catalog.pg_type AS t WHERE t.oid = a.atttypid)
            LIMIT 1
        ) AS eq ON true;
        IF missing_equality THEN
            RAISE EXCEPTION 'sql_saga: a column used by regular foreign key helper % has no default btree equality operator', h.fk_helper_function
                USING ERRCODE = 'undefined_function',
                      HINT = 'Restore the referenced column type''s default btree operator class before retrying the upgrade.';
        END IF;

        expected_owner := h.relowner;
        expected_v11_body := pg_catalog.format($body$
                SELECT EXISTS (SELECT 1 FROM %1$I.%2$I AS uk WHERE %3$s);
            $body$, h.table_schema, h.table_name, v11_where_clause);

        IF p.prolang OPERATOR(pg_catalog.<>) (SELECT l.oid FROM pg_catalog.pg_language AS l WHERE l.lanname = 'sql')
           OR p.prorettype OPERATOR(pg_catalog.<>) 'pg_catalog.bool'::pg_catalog.regtype
           OR NOT p.proisstrict
           OR p.provolatile OPERATOR(pg_catalog.<>) 's'
           OR p.prokind OPERATOR(pg_catalog.<>) 'f'
           OR p.proargtypes OPERATOR(pg_catalog.<>) expected_argtypes
           OR p.proowner OPERATOR(pg_catalog.<>) expected_owner
           OR p.prosrc OPERATOR(pg_catalog.<>) expected_v11_body
           OR NOT p.prosecdef
           OR p.proconfig IS DISTINCT FROM ARRAY['search_path=pg_catalog']::text[] THEN
            RAISE EXCEPTION 'sql_saga: function % is not the exact canonical released 1.1 regular foreign key helper', h.fk_helper_function
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the exact helper generated by the released sql_saga 1.1, or recreate the foreign key with sql_saga, then retry the upgrade.';
        END IF;
    END LOOP;
END;
$validate_fk_helpers$;

CREATE OR REPLACE FUNCTION sql_saga.native_library_version() RETURNS text LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS 'sql_saga_native', 'sql_saga_native_version_wrapper';
DO $native_guard$
BEGIN
    IF sql_saga.native_library_version() <> '1.2' THEN
        RAISE EXCEPTION 'sql_saga: native library version % does not match extension update version 1.2', sql_saga.native_library_version()
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Install the sql_saga 1.2 native library and restart PostgreSQL before retrying ALTER EXTENSION.';
    END IF;
END;
$native_guard$;

SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga 1.1 to 1.2 upgrade'));

DO $preflight$
DECLARE
    p record;
BEGIN
    FOR p IN
        SELECT pn.nspname, pc.relname, pol.polname
        FROM sql_saga.system_versioning AS sv
        JOIN pg_catalog.pg_namespace AS pn ON pn.nspname = sv.history_schema_name
        JOIN pg_catalog.pg_class AS pc ON pc.relnamespace = pn.oid AND pc.relname = sv.history_table_name
        JOIN pg_catalog.pg_policy AS pol ON pol.polrelid = pc.oid
        WHERE pol.polname !~ '^sql_saga_'
        ORDER BY pn.nspname, pc.relname, pol.polname
    LOOP
        RAISE EXCEPTION 'sql_saga: history table %.% has non-managed policy %', p.nspname, p.relname, p.polname
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Move the policy to the base table, drop it from history, and retry the upgrade.';
    END LOOP;
END;
$preflight$;

CREATE FUNCTION sql_saga.__internal_display_name(schema_name name, object_name name)
RETURNS text
LANGUAGE sql
STABLE
AS $__internal_display_name$
    SELECT sql_saga.__internal_display_name(
        CASE WHEN schema_name = ANY (pg_catalog.current_schemas(true))
             THEN pg_catalog.quote_ident(object_name)
             ELSE pg_catalog.format('%I.%I', schema_name, object_name)
        END);
$__internal_display_name$;
/*
 * Private, transaction-scoped authorization for the nested policy DDL performed by
 * __internal_sync_history_policies. Names, not OIDs, are stored. Ordinary callers cannot write or
 * inspect this table; the synchronizer and drop_protection run as the extension owner.
 */
CREATE TABLE sql_saga.history_policy_sync_guard (
    backend_pid integer NOT NULL,
    transaction_id bigint NOT NULL,
    base_schema name NOT NULL,
    base_table name NOT NULL,
    nesting_depth integer NOT NULL CHECK (nesting_depth > 0),
    PRIMARY KEY (backend_pid, transaction_id, base_schema, base_table)
);
REVOKE ALL ON TABLE sql_saga.history_policy_sync_guard FROM PUBLIC;

CREATE FUNCTION sql_saga.__internal_history_policy_sync_active(table_oid oid)
RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog
AS $__internal_history_policy_sync_active$
    SELECT EXISTS (
        SELECT 1
        FROM sql_saga.history_policy_sync_guard AS g
        JOIN pg_catalog.pg_class AS c ON c.oid = table_oid
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        WHERE g.backend_pid = pg_catalog.pg_backend_pid()
          AND g.transaction_id = pg_catalog.pg_current_xact_id()::text::bigint
          AND (g.base_schema, g.base_table) = (n.nspname, c.relname));
$__internal_history_policy_sync_active$;
REVOKE ALL ON FUNCTION sql_saga.__internal_history_policy_sync_active(oid) FROM PUBLIC;
CREATE OR REPLACE FUNCTION sql_saga.drop_protection()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    table_oid regclass;
    era_name name;
    v_teardown boolean := sql_saga.__internal_is_clean_teardown();
    v_dropped_objects jsonb[];
    v_sanctioned_bases jsonb[];
    v_refusal_objects jsonb[];
    v_candidate record;
    v_object_types text[];
    v_teardown_family boolean := false;
BEGIN
    /* Materialize this invocation once. Nested DDL gets its own function frame. */
    SELECT coalesce(
        array_agg(to_jsonb(dobj) ORDER BY dobj.ordinality),
        ARRAY[]::jsonb[])
    INTO v_dropped_objects
    FROM pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj;

    SELECT pg_catalog.array_agg(DISTINCT u.data->>'object_type' ORDER BY u.data->>'object_type')
    INTO v_object_types
    FROM unnest(v_dropped_objects) AS u(data);

    /*
     * During a recognized clean teardown, any non-policy object addressed on a system-versioned base or
     * history table must reach the teardown cleanup below (it drops the installation-token history
     * policies). Neither early exit may skip it. Outside a teardown this costs nothing.
     */
    IF v_teardown THEN
        SELECT EXISTS (
            SELECT 1
            FROM unnest(v_dropped_objects) AS u(data)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(object_type text, address_names text[])
            JOIN sql_saga.system_versioning AS sv
              ON (d.address_names[1], d.address_names[2]) IN (
                  (sv.table_schema, sv.table_name), (sv.history_schema_name, sv.history_table_name))
            WHERE d.object_type <> 'policy')
        INTO v_teardown_family;
    END IF;

    IF v_teardown_family THEN
        NULL; /* Admitted: see above. */
    ELSIF v_object_types = ARRAY['function'] AND NOT EXISTS (
        SELECT 1
        FROM unnest(v_dropped_objects) AS u(data)
        JOIN sql_saga.system_versioning AS sv
          ON u.data->>'object_identity' = ANY (
              ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to])) THEN
        RETURN;
    ELSIF v_object_types = ARRAY['view'] AND NOT EXISTS (
        SELECT 1
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(address_names text[])
        WHERE EXISTS (
                  SELECT FROM sql_saga.updatable_view AS v
                  WHERE (v.view_schema, v.view_name) = (d.address_names[1], d.address_names[2]))
           OR EXISTS (
                  SELECT FROM sql_saga.system_versioning AS sv
                  WHERE (d.address_names[1], d.address_names[2]) IN (
                      (sv.view_schema_name, sv.view_table_name),
                      (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name))))) THEN
        RETURN;
    ELSIF v_object_types = ARRAY['table'] AND NOT EXISTS (
        SELECT 1
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(address_names text[])
        WHERE EXISTS (
                  SELECT FROM sql_saga.era AS e
                  WHERE (e.table_schema, e.table_name) = (d.address_names[1], d.address_names[2]))
           OR EXISTS (
                  SELECT FROM sql_saga.system_versioning AS sv
                  WHERE (sv.history_schema_name, sv.history_table_name) =
                        (d.address_names[1], d.address_names[2]))) THEN
        RETURN;
    ELSIF v_object_types = ARRAY['trigger'] AND NOT EXISTS (
        SELECT 1
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(address_names text[])
        WHERE EXISTS (
                  SELECT FROM sql_saga.system_time_era AS ste
                  WHERE (ste.table_schema, ste.table_name) = (d.address_names[1], d.address_names[2])
                    AND d.address_names[3] IN (
                        ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger))
           OR EXISTS (
                  SELECT FROM sql_saga.updatable_view AS v
                  WHERE (v.view_schema, v.view_name, v.trigger_name) =
                        (d.address_names[1], d.address_names[2], d.address_names[3]))
           OR EXISTS (
                  SELECT FROM sql_saga.foreign_keys AS fk
                  JOIN sql_saga.unique_keys AS uk USING (unique_key_name)
                  WHERE (uk.table_schema, uk.table_name) = (d.address_names[1], d.address_names[2])
                    AND d.address_names[3] IN (fk.uk_update_trigger, fk.uk_delete_trigger))) THEN
        RETURN;
    END IF;

    /* Ordinary and metadata-first DDL must not pay for protected-object analysis. */
    IF NOT v_teardown_family AND NOT EXISTS (
        WITH dropped AS MATERIALIZED (
            SELECT d.*
            FROM unnest(v_dropped_objects) AS u(data)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, object_identity text, address_names text[], objid oid)
        )
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.era AS e
          ON (d.object_type = 'table'
              AND (d.address_names[1], d.address_names[2]) = (e.table_schema, e.table_name))
          OR (d.object_type = 'table column'
              AND (d.address_names[1], d.address_names[2]) = (e.table_schema, e.table_name)
              AND d.address_names[3] IN (e.valid_from_column_name, e.valid_until_column_name))
          OR d.objid = e.range_type::oid
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.system_time_era AS ste
          ON (d.object_type = 'table constraint'
              AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
                  (ste.table_schema, ste.table_name, ste.infinity_check_constraint))
          OR (d.object_type = 'trigger'
              AND (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
              AND d.address_names[3] IN (ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger))
          OR (d.object_type = 'table column'
              AND (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
              AND d.address_names[3] = ANY (ste.excluded_column_names))
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.updatable_view AS v
          ON (d.object_type = 'view'
              AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name))
          OR (d.object_type = 'trigger'
              AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
                  (v.view_schema, v.view_name, v.trigger_name))
          OR (d.object_type IN ('table column', 'table constraint', 'index')
              AND (d.address_names[1], d.address_names[2]) = (v.table_schema, v.table_name))
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.unique_keys AS uk
          ON (d.object_type = 'table constraint'
              AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
              AND (d.address_names[3] IN (uk.unique_constraint, uk.exclude_constraint, uk.check_constraint)
                   OR d.address_names[3] = ANY (coalesce(uk.pk_consistency_constraint_names, '{}'))))
          OR (d.object_type = 'index'
              AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.unique_constraint))
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.foreign_keys AS fk
          ON d.object_type = 'trigger'
         AND d.address_names[3] IN (fk.uk_update_trigger, fk.uk_delete_trigger)
        JOIN sql_saga.unique_keys AS uk
          ON uk.unique_key_name = fk.unique_key_name
         AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.system_versioning AS sv
          ON (d.object_type = 'table'
              AND (d.address_names[1], d.address_names[2]) = (sv.history_schema_name, sv.history_table_name))
          OR (d.object_type = 'view'
              AND (d.address_names[1], d.address_names[2]) IN (
                  (sv.view_schema_name, sv.view_table_name),
                  (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name))))
          OR (d.object_type = 'function'
              AND d.object_identity = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]))
        UNION ALL
        SELECT 1
        FROM dropped AS d
        CROSS JOIN LATERAL sql_saga.__internal_history_family(
            to_regclass(format('%I.%I', d.address_names[1], d.address_names[2]))) AS f
        WHERE d.object_type = 'policy'
    ) THEN
        RETURN;
    END IF;

    /*
     * Choose exactly one refusal from this statement's payload. Priority preserves the historical
     * source-class order; ordinality preserves PostgreSQL's dependency order within a class.
     */
    IF NOT v_teardown THEN
        /*
         * Remove complete families before constructing refusal candidates.  The ordinalities
         * identify rows in this invocation's materialized payload, so no dropped-object OID is
         * retained beyond the statement.  Objects not belonging to a dropped base family remain
         * available for mixed sanctioned/unsanctioned statements.
         */
        IF EXISTS (
            SELECT 1
            FROM unnest(v_dropped_objects) AS u(data)
            WHERE u.data->>'object_type' = 'table'
        ) THEN
            SELECT coalesce(
                array_agg(DISTINCT jsonb_build_array(e.table_schema, e.table_name)),
                ARRAY[]::jsonb[])
            INTO v_sanctioned_bases
            FROM unnest(v_dropped_objects) AS u(data)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, address_names text[])
            JOIN sql_saga.era AS e
              ON d.object_type = 'table'
             AND (d.address_names[1], d.address_names[2]) = (e.table_schema, e.table_name);
        ELSE
            v_sanctioned_bases := ARRAY[]::jsonb[];
        END IF;

        IF cardinality(v_sanctioned_bases) = 0 THEN
            v_refusal_objects := v_dropped_objects;
        ELSIF EXISTS (
            SELECT 1
            FROM unnest(v_dropped_objects) AS u(data)
            WHERE u.data->>'object_type' = 'schema'
        ) AND NOT EXISTS (
            WITH dropped AS MATERIALIZED (
                SELECT d.*
                FROM unnest(v_dropped_objects) AS u(data)
                CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                    object_type text, object_identity text, address_names text[])
            )
            SELECT 1
            FROM dropped AS d
            JOIN sql_saga.updatable_view AS v
              ON ((d.object_type = 'view'
                   AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name))
                  OR (d.object_type = 'trigger'
                      AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
                          (v.view_schema, v.view_name, v.trigger_name)))
            WHERE NOT jsonb_build_array(v.table_schema, v.table_name) = ANY (v_sanctioned_bases)
            UNION ALL
            SELECT 1
            FROM dropped AS d
            JOIN sql_saga.system_versioning AS sv
              ON (d.object_type = 'table'
                  AND (d.address_names[1], d.address_names[2]) =
                      (sv.history_schema_name, sv.history_table_name))
              OR (d.object_type = 'view'
                  AND (d.address_names[1], d.address_names[2]) IN (
                      (sv.view_schema_name, sv.view_table_name),
                      (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name))))
              OR (d.object_type = 'function'
                  AND d.object_identity = ANY (ARRAY[
                      sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]))
            WHERE NOT jsonb_build_array(sv.table_schema, sv.table_name) = ANY (v_sanctioned_bases)
        ) THEN
            v_refusal_objects := ARRAY[]::jsonb[];
        ELSE
        WITH dropped AS MATERIALIZED (
            SELECT u.ordinality AS dropped_ordinality, u.data, d.*
            FROM unnest(v_dropped_objects) WITH ORDINALITY AS u(data, ordinality)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, object_identity text, address_names text[], objid oid)
        ), sanctioned_bases AS MATERIALIZED (
            SELECT b.data->>0 AS table_schema, b.data->>1 AS table_name
            FROM unnest(v_sanctioned_bases) AS b(data)
        )
        SELECT coalesce(
            array_agg(d.data ORDER BY d.dropped_ordinality) FILTER (WHERE NOT EXISTS (
                SELECT 1
                FROM sanctioned_bases AS b
                WHERE (d.address_names[1], d.address_names[2]) = (b.table_schema, b.table_name)
                   OR EXISTS (
                        SELECT 1
                        FROM sql_saga.era AS e
                        WHERE (e.table_schema, e.table_name) = (b.table_schema, b.table_name)
                          AND d.objid = e.range_type::oid)
                   OR (d.object_type = 'index' AND EXISTS (
                        SELECT 1
                        FROM sql_saga.unique_keys AS uk
                        WHERE (uk.table_schema, uk.table_name) = (b.table_schema, b.table_name)
                          AND (d.address_names[1], d.address_names[2]) =
                              (uk.table_schema, uk.unique_constraint)))
                   OR EXISTS (
                        SELECT 1
                        FROM sql_saga.updatable_view AS v
                        WHERE (v.table_schema, v.table_name) = (b.table_schema, b.table_name)
                          AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name))
                   OR EXISTS (
                        SELECT 1
                        FROM sql_saga.system_versioning AS sv
                        WHERE (sv.table_schema, sv.table_name) = (b.table_schema, b.table_name)
                          AND ((d.address_names[1], d.address_names[2]) IN (
                                  (sv.history_schema_name, sv.history_table_name),
                                  (sv.view_schema_name, sv.view_table_name),
                                  (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)))
                               OR d.object_identity = ANY (ARRAY[
                                  sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to])))
            )),
            ARRAY[]::jsonb[])
        INTO v_refusal_objects
        FROM dropped AS d;
        END IF;

        IF cardinality(v_refusal_objects) > 0 THEN
        IF NOT EXISTS (
            SELECT 1
            FROM unnest(v_refusal_objects) AS u(data)
            WHERE u.data->>'object_type' <> 'trigger'
        ) THEN
        WITH dropped AS (
            SELECT u.ordinality AS dropped_ordinality, d.*
            FROM unnest(v_refusal_objects) WITH ORDINALITY AS u(data, ordinality)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, address_names text[])
        ), candidates AS (
            SELECT CASE d.address_names[3]
                       WHEN ste.generated_always_trigger THEN 40
                       WHEN ste.write_history_trigger THEN 50 ELSE 60 END AS priority,
                   d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in SYSTEM_TIME period',
                          d.address_names[3],
                          sql_saga.__internal_display_name(ste.table_schema, ste.table_name)) AS message
            FROM dropped AS d JOIN sql_saga.system_time_era AS ste
             ON (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
             AND d.address_names[3] IN (ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger)
            UNION ALL
            SELECT 90, d.dropped_ordinality,
                   format('cannot drop trigger "%s" on view "%s" because it is part of an updatable view for era "%s" on table "%s"',
                          v.trigger_name,
                          sql_saga.__internal_display_name(d.address_names[1]::name, d.address_names[2]::name),
                          v.era_name,
                          sql_saga.__internal_display_name(v.table_schema, v.table_name))
            FROM dropped AS d JOIN sql_saga.updatable_view AS v
             ON (d.address_names[1], d.address_names[2], d.address_names[3]) = (v.view_schema, v.view_name, v.trigger_name)
            UNION ALL
            SELECT CASE WHEN d.address_names[3] = fk.uk_update_trigger THEN 120 ELSE 130 END,
                   d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in era foreign key "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(uk.table_schema, uk.table_name),
                          fk.foreign_key_name)
            FROM dropped AS d
            JOIN sql_saga.foreign_keys AS fk ON d.address_names[3] IN (fk.uk_update_trigger, fk.uk_delete_trigger)
            JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
            WHERE (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
        )
        SELECT c.message INTO v_candidate
        FROM candidates AS c
        ORDER BY c.priority, c.dropped_ordinality
        LIMIT 1;
        ELSE
        WITH dropped AS (
            SELECT u.ordinality AS dropped_ordinality, d.*
            FROM unnest(v_refusal_objects) WITH ORDINALITY AS u(data, ordinality)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, object_identity text, address_names text[], objid oid)
        ), candidates AS (
            SELECT 10 AS priority, d.dropped_ordinality,
                   format('cannot drop column "%s" because it is part of the period "%s"', d.object_identity, e.era_name) AS message
            FROM dropped AS d JOIN sql_saga.era AS e
             ON d.object_type = 'table column'
             AND (d.address_names[1], d.address_names[2]) = (e.table_schema, e.table_name)
             AND d.address_names[3] IN (e.valid_from_column_name, e.valid_until_column_name)
            UNION ALL
            SELECT 20, d.dropped_ordinality,
                   format('cannot drop rangetype "%s" because it is used in period "%s" on table "%s"',
                          d.object_identity, e.era_name,
                          sql_saga.__internal_display_name(e.table_schema, e.table_name))
            FROM dropped AS d JOIN sql_saga.era AS e ON d.objid = e.range_type::oid
            UNION ALL
            SELECT 30, d.dropped_ordinality,
                   format('cannot drop constraint "%s" on table "%s" because it is used in SYSTEM_TIME period',
                          ste.infinity_check_constraint,
                          sql_saga.__internal_display_name(ste.table_schema, ste.table_name))
            FROM dropped AS d JOIN sql_saga.system_time_era AS ste
             ON d.object_type = 'table constraint'
             AND (d.address_names[1], d.address_names[2], d.address_names[3]) = (ste.table_schema, ste.table_name, ste.infinity_check_constraint)
            UNION ALL
            SELECT CASE d.address_names[3]
                       WHEN ste.generated_always_trigger THEN 40
                       WHEN ste.write_history_trigger THEN 50 ELSE 60 END,
                   d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in SYSTEM_TIME period',
                          d.address_names[3],
                          sql_saga.__internal_display_name(ste.table_schema, ste.table_name))
            FROM dropped AS d JOIN sql_saga.system_time_era AS ste
             ON d.object_type = 'trigger'
             AND (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
             AND d.address_names[3] IN (ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger)
            UNION ALL
            SELECT 70, d.dropped_ordinality,
                   format('cannot drop or rename column "%s" on table "%s" because it is excluded from system_time era',
                          d.address_names[3],
                          sql_saga.__internal_display_name(ste.table_schema, ste.table_name))
            FROM dropped AS d JOIN sql_saga.system_time_era AS ste
             ON d.object_type = 'table column'
             AND (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
             AND d.address_names[3] = ANY (ste.excluded_column_names)
            UNION ALL
            SELECT 80, d.dropped_ordinality,
                   format('cannot drop view "%s", call "sql_saga.drop_%s_view()" instead',
                          sql_saga.__internal_display_name(d.object_identity), v.view_type)
            FROM dropped AS d JOIN sql_saga.updatable_view AS v
             ON d.object_type = 'view'
             AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name)
            UNION ALL
            SELECT 90, d.dropped_ordinality,
                   format('cannot drop trigger "%s" on view "%s" because it is part of an updatable view for era "%s" on table "%s"',
                          v.trigger_name,
                          sql_saga.__internal_display_name(d.address_names[1]::name, d.address_names[2]::name),
                          v.era_name,
                          sql_saga.__internal_display_name(v.table_schema, v.table_name))
            FROM dropped AS d JOIN sql_saga.updatable_view AS v
             ON d.object_type = 'trigger'
             AND (d.address_names[1], d.address_names[2], d.address_names[3]) = (v.view_schema, v.view_name, v.trigger_name)
            UNION ALL
            SELECT 100, d.dropped_ordinality,
                   format('table "%s" must have a primary key or a single-column temporal unique key to support its updatable view for era "%s"',
                          sql_saga.__internal_display_name(v.table_schema, v.table_name), v.era_name)
            FROM dropped AS d JOIN sql_saga.updatable_view AS v
             ON d.object_type IN ('table column', 'table constraint', 'index')
             AND (d.address_names[1], d.address_names[2]) = (v.table_schema, v.table_name)
            WHERE NOT EXISTS (
                      SELECT 1 FROM pg_catalog.pg_constraint AS c
                      WHERE (c.conrelid, c.contype) =
                            (to_regclass(format('%I.%I', v.table_schema, v.table_name)), 'p'))
              AND NOT EXISTS (
                      SELECT 1 FROM sql_saga.unique_keys AS identifier_uk
                      WHERE (identifier_uk.table_schema, identifier_uk.table_name, identifier_uk.era_name) =
                            (v.table_schema, v.table_name, v.era_name)
                        AND array_length(identifier_uk.column_names, 1) = 1
                        AND NOT EXISTS (
                                SELECT 1
                                FROM dropped AS identifier_drop
                                WHERE (identifier_drop.object_type = 'table constraint'
                                       AND (identifier_drop.address_names[1], identifier_drop.address_names[2], identifier_drop.address_names[3]) =
                                           (identifier_uk.table_schema, identifier_uk.table_name, identifier_uk.unique_constraint))
                                   OR (identifier_drop.object_type = 'index'
                                       AND (identifier_drop.address_names[1], identifier_drop.address_names[2]) =
                                           (identifier_uk.table_schema, identifier_uk.unique_constraint))))
            UNION ALL
            SELECT 110, d.dropped_ordinality,
                   format('cannot drop constraint "%s" on table "%s" because it is used in era unique key "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(uk.table_schema, uk.table_name),
                          uk.unique_key_name)
            FROM dropped AS d JOIN sql_saga.unique_keys AS uk
             ON d.object_type = 'table constraint'
             AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
             AND (d.address_names[3] IN (uk.unique_constraint, uk.exclude_constraint, uk.check_constraint)
                  OR d.address_names[3] = ANY (coalesce(uk.pk_consistency_constraint_names, '{}')))
            UNION ALL
            SELECT 110, d.dropped_ordinality,
                   format('cannot drop index "%s" on table "%s" because it is used in era unique key "%s"',
                          uk.unique_constraint,
                          sql_saga.__internal_display_name(uk.table_schema, uk.table_name),
                          uk.unique_key_name)
            FROM dropped AS d JOIN sql_saga.unique_keys AS uk
             ON d.object_type = 'index'
             AND (d.address_names[1], d.address_names[2]) =
                 (uk.table_schema, uk.unique_constraint)
            UNION ALL
            SELECT CASE WHEN d.address_names[3] = fk.uk_update_trigger THEN 120 ELSE 130 END,
                   d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in era foreign key "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(uk.table_schema, uk.table_name),
                          fk.foreign_key_name)
            FROM dropped AS d
            JOIN sql_saga.foreign_keys AS fk ON d.address_names[3] IN (fk.uk_update_trigger, fk.uk_delete_trigger)
            JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
            WHERE d.object_type = 'trigger'
              AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
            UNION ALL
            SELECT 140, d.dropped_ordinality,
                   format('cannot drop table "%s" because it is used in SYSTEM VERSIONING for table "%s"',
                          sql_saga.__internal_display_name(d.object_identity),
                          sql_saga.__internal_display_name(sv.table_schema, sv.table_name))
            FROM dropped AS d JOIN sql_saga.system_versioning AS sv
              ON d.object_type = 'table'
             AND (d.address_names[1], d.address_names[2]) = (sv.history_schema_name, sv.history_table_name)
            UNION ALL
            SELECT 150, d.dropped_ordinality,
                   format('cannot drop view "%s" because it is used in SYSTEM VERSIONING for table "%s"',
                          sql_saga.__internal_display_name(d.object_identity),
                          sql_saga.__internal_display_name(sv.table_schema, sv.table_name))
            FROM dropped AS d JOIN sql_saga.system_versioning AS sv
              ON d.object_type = 'view'
             AND (d.address_names[1], d.address_names[2]) IN (
                    (sv.view_schema_name, sv.view_table_name),
                    (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)))
             AND sql_saga.__internal_rebuilding_system_versioning_views(
                     to_regclass(format('%I.%I', sv.table_schema, sv.table_name))::oid) IS NOT TRUE
            UNION ALL
            SELECT 160, d.dropped_ordinality,
                   format('cannot drop function "%s" because it is used in SYSTEM VERSIONING for table "%s"',
                          sql_saga.__internal_display_function_name(d.object_identity),
                          sql_saga.__internal_display_name(sv.table_schema, sv.table_name))
            FROM dropped AS d JOIN sql_saga.system_versioning AS sv
              ON d.object_type = 'function'
             AND d.object_identity = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to])
             AND sql_saga.__internal_rebuilding_system_versioning_views(
                     to_regclass(format('%I.%I', sv.table_schema, sv.table_name))::oid) IS NOT TRUE
            UNION ALL
            SELECT 170, d.dropped_ordinality,
                   format('cannot drop or alter policy "%s" on "%s" because it is managed by sql_saga for table "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(f.history_oid::regclass),
                          sql_saga.__internal_display_name(f.base_oid::regclass))
            FROM dropped AS d
            CROSS JOIN LATERAL sql_saga.__internal_history_family(
                to_regclass(format('%I.%I', d.address_names[1], d.address_names[2]))) AS f
            WHERE d.object_type = 'policy'
              AND to_regclass(format('%I.%I', d.address_names[1], d.address_names[2]))::oid = f.history_oid
              AND sql_saga.__internal_is_managed_history_policy(d.address_names[3]::name)
              AND NOT sql_saga.__internal_history_policy_sync_active(f.base_oid)
        )
        SELECT c.message INTO v_candidate
        FROM candidates AS c
        ORDER BY c.priority, c.dropped_ordinality
        LIMIT 1;
        END IF;

        IF FOUND THEN
            RAISE EXCEPTION '%', v_candidate.message;
        END IF;
        END IF;
    END IF;

    ---
    --- periods
    ---

    /*
     * Teardown by pg_restore --clean (sql_saga.__internal_is_clean_teardown, tmp/design_restore_clean.md).
     * The checks below that complain about missing objects are skipped, and so is cleanup of objects
     * pg_restore drops itself. One thing pg_restore cannot do: drop the managed history policies,
     * whose names carry this installation's token while the dump carries another. They depend on the
     * base table, so they are dropped here, as soon as any object of their family goes (DROP TABLE of
     * the base would fail otherwise when the base is dropped before its history). Not on pg_restore's
     * own DROP POLICY statements: those come first, and a later DROP POLICY (without IF EXISTS) of a
     * dumped name already gone would fail. Restore plumbing the caller asked for: reported at DEBUG.
     * The nested DROP POLICY statements below drop only policies, which the family lookup ignores, so
     * they never re-enter this cleanup.
     */
    IF v_teardown THEN
        DECLARE
            family_row record;
            policy_row record;
        BEGIN
            FOR family_row IN
                SELECT DISTINCT to_regclass(format('%I.%I', sv.history_schema_name /* %I */, sv.history_table_name /* %I */)) AS history_oid
                FROM unnest(v_dropped_objects) AS u(data)
                CROSS JOIN LATERAL jsonb_to_record(u.data) AS dobj(
                    object_type text, address_names text[])
                JOIN sql_saga.system_versioning AS sv
                  ON (dobj.address_names[1], dobj.address_names[2]) IN ((sv.table_schema, sv.table_name), (sv.history_schema_name, sv.history_table_name))
                WHERE dobj.object_type <> 'policy'
            LOOP
                CONTINUE WHEN family_row.history_oid IS NULL;
                FOR policy_row IN
                    SELECT p.polname
                    FROM pg_catalog.pg_policy AS p
                    WHERE p.polrelid = family_row.history_oid
                      AND sql_saga.__internal_is_managed_history_policy(p.polname)
                    ORDER BY p.polname
                LOOP
                    EXECUTE format('DROP POLICY IF EXISTS %I ON %s', policy_row.polname /* %I */, family_row.history_oid::regclass /* %s */);
                    RAISE DEBUG 'sql_saga: Dropped policy % on %, part of the teardown by pg_restore --clean',
                        policy_row.polname, family_row.history_oid::regclass;
                END LOOP;
            END LOOP;
        END;
    END IF;

    /* If one of our tables is being dropped, remove references to it */
    FOR r IN
        SELECT dobj.schema_name, dobj.object_name
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS dobj(
            object_type text, schema_name text, object_name text)
        WHERE dobj.object_type = 'table'
          AND EXISTS (SELECT 1 FROM sql_saga.era e WHERE (e.table_schema, e.table_name) = (dobj.schema_name, dobj.object_name))
    LOOP
        -- This table was dropped. Cascade-delete its metadata and any dependent triggers on other tables.
        -- This logic mirrors the metadata-first era drop for a dropped table.

        -- 1. For FKs that reference the dropped table, drop their triggers.
        -- The metadata will be cleaned up through the unique key's cascading foreign key.
        DECLARE
            fk_row record;
            fk_table_oid regclass;
        BEGIN
            FOR fk_row IN
                SELECT fk.*
                FROM sql_saga.foreign_keys fk
                JOIN sql_saga.unique_keys uk ON fk.unique_key_name = uk.unique_key_name
                WHERE (uk.table_schema, uk.table_name) = (r.schema_name, r.object_name)
            LOOP
                fk_table_oid := pg_catalog.to_regclass(format('%I.%I', fk_row.table_schema /* %I */, fk_row.table_name /* %I */));
                -- The referencing table might have been dropped in the same cascading statement. If it still exists, drop its triggers/constraints.
                -- Not during pg_restore --clean: it drops the CHECK (with the table) and the helper function itself.
                IF fk_table_oid IS NOT NULL AND NOT v_teardown THEN
                    IF fk_row.type = 'regular_to_temporal' THEN
                        IF fk_row.fk_check_constraint IS NOT NULL AND EXISTS (SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid = fk_table_oid AND conname = fk_row.fk_check_constraint) THEN
                            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk_table_oid /* %s */, fk_row.fk_check_constraint /* %I */);
                        END IF;
                        IF fk_row.fk_helper_function IS NOT NULL THEN
                           EXECUTE format('DROP FUNCTION IF EXISTS %s', fk_row.fk_helper_function /* %s */);
                        END IF;
                    END IF;
                END IF;
            END LOOP;
        END;

        -- 2. For FKs ON the dropped table, remove triggers from the referenced UK table.
        DECLARE
            fk_row record;
            uk_table_oid regclass;
        BEGIN
            FOR fk_row IN
                SELECT fk.*, uk.table_schema AS uk_schema, uk.table_name AS uk_table
                FROM sql_saga.foreign_keys fk JOIN sql_saga.unique_keys uk ON fk.unique_key_name = uk.unique_key_name
                WHERE (fk.table_schema, fk.table_name) = (r.schema_name, r.object_name)
            LOOP
                uk_table_oid := pg_catalog.to_regclass(format('%I.%I', fk_row.uk_schema /* %I */, fk_row.uk_table /* %I */));
                -- Use DROP IF EXISTS because the UK table might have been dropped in the same command.
                IF uk_table_oid IS NOT NULL THEN
                    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = uk_table_oid AND tgname = fk_row.uk_update_trigger) THEN
                        EXECUTE format('DROP TRIGGER %I ON %s', fk_row.uk_update_trigger /* %I */, uk_table_oid /* %s */);
                    END IF;
                    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = uk_table_oid AND tgname = fk_row.uk_delete_trigger) THEN
                        EXECUTE format('DROP TRIGGER %I ON %s', fk_row.uk_delete_trigger /* %I */, uk_table_oid /* %s */);
                    END IF;
                END IF;
            END LOOP;
        END;

        -- 3. Clean up template sync triggers/functions for each era on this table BEFORE deleting metadata
        DECLARE
            era_row record;
            func_name name;
        BEGIN
            -- Table has been dropped, but we still need to clean up the functions
            FOR era_row IN
                SELECT e.era_name, e.sync_temporal_trg_function_name
                FROM sql_saga.era e
                WHERE (e.table_schema, e.table_name) = (r.schema_name, r.object_name)
                  AND e.sync_temporal_trg_function_name IS NOT NULL  -- Only eras with sync triggers
                  -- Not during pg_restore --clean: it drops the (dumped) function itself, without IF EXISTS.
                  AND NOT v_teardown
            LOOP
                -- Drop the function directly (trigger already gone with table)
                func_name := era_row.sync_temporal_trg_function_name;
                IF to_regprocedure(format('sql_saga.%I()', func_name)) IS NOT NULL THEN
                    EXECUTE format($$DROP FUNCTION sql_saga.%I() %s$$, func_name, 'CAS' || 'CADE');
                END IF;
            END LOOP;
        END;

        -- 4. Delete all metadata for the dropped table.
        DELETE FROM sql_saga.updatable_view WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
        DELETE FROM sql_saga.foreign_keys WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
        DELETE FROM sql_saga.unique_keys WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
        DELETE FROM sql_saga.era WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
    END LOOP;

    /*
     * Policies (tmp/design_final.md sections 8, 13). A dropped policy on a versioned base table
     * drops its history copy. Policies sql_saga manages on a history table cannot be dropped
     * directly. address_names of a policy are {schema, table, policy}.
     */
    DECLARE
    BEGIN
    FOR r IN
        SELECT f.base_oid, f.history_oid,
               to_regclass(format('%I.%I', dobj.address_names[1], dobj.address_names[2]))::oid AS rel_oid,
               array_agg(dobj.address_names[3]::name ORDER BY dobj.ordinality) AS policy_names
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS dobj(
            object_type text, address_names text[], ordinality bigint)
        CROSS JOIN LATERAL sql_saga.__internal_history_family(
            to_regclass(format('%I.%I', dobj.address_names[1], dobj.address_names[2]))) AS f
        WHERE dobj.object_type = 'policy'
        GROUP BY 1, 2, 3
    LOOP
        -- Not during pg_restore --clean: it drops the dumped copies itself, without IF EXISTS.
        IF r.rel_oid = r.base_oid AND NOT v_teardown
           AND NOT sql_saga.__internal_history_policy_sync_active(r.base_oid) THEN
            PERFORM sql_saga.__internal_sync_history_policies(r.base_oid, r.policy_names);
        END IF;
    END LOOP;
    END;
END;
$function$;

COMMENT ON FUNCTION sql_saga.drop_protection() IS
'An event trigger function that prevents accidental dropping of sql_saga-managed objects. During the teardown phase of pg_restore --clean (see sql_saga.__internal_is_clean_teardown) it lets the drops through and removes the managed history policies the dump cannot name.';
CREATE OR REPLACE FUNCTION sql_saga.__internal_sync_history_policies(table_oid oid, dropped_base_policies name[] DEFAULT '{}')
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, public
AS $__internal_sync_history_policies$
DECLARE
    fam record;
    d record;
    p record;
    missing_column name;
    stmt text;
    force_mode text;
    reader_qual text;
    base_select_qual text;
BEGIN
    SELECT * INTO fam FROM sql_saga.__internal_history_family(table_oid) AS f WHERE f.base_oid = table_oid;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    IF NOT pg_catalog.pg_has_role(session_user,
            (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid = fam.base_oid), 'USAGE') THEN
        RAISE EXCEPTION 'must be owner of table %', fam.base_oid::regclass
            USING ERRCODE = 'insufficient_privilege';
    END IF;

    INSERT INTO sql_saga.history_policy_sync_guard AS g
        (backend_pid, transaction_id, base_schema, base_table, nesting_depth)
    SELECT pg_catalog.pg_backend_pid(), pg_catalog.pg_current_xact_id()::text::bigint,
           n.nspname, c.relname, 1
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = fam.base_oid
    ON CONFLICT (backend_pid, transaction_id, base_schema, base_table)
    DO UPDATE SET nesting_depth = g.nesting_depth + 1;

    -- A policy sql_saga does not manage blocks synchronization: sql_saga never drops a user's policy
    -- (decision 7); the user moves it to the base table.
    FOR p IN
        SELECT hp.polname FROM pg_catalog.pg_policy AS hp
        WHERE hp.polrelid = fam.history_oid AND NOT sql_saga.__internal_is_managed_history_policy(hp.polname)
        ORDER BY hp.polname
    LOOP
        RAISE EXCEPTION 'sql_saga: policies on history table "%" are managed by sql_saga', sql_saga.__internal_display_name(fam.history_oid::regclass)
            USING HINT = format('Drop policy "%s" on "%s" and create it on "%s"; sql_saga copies its SELECT and ALL policies to the history.',
                p.polname, sql_saga.__internal_display_name(fam.history_oid::regclass), sql_saga.__internal_display_name(fam.base_oid::regclass));
    END LOOP;

    -- Validate every desired copy before changing anything, so that a refusal reports only the
    -- refusal: no NOTICE describes a change that the error then undoes.
    FOR d IN
        SELECT c.base_policy, bp.oid AS base_policy_oid
        FROM sql_saga.__internal_history_policy_copies(fam.base_oid) AS c
        JOIN pg_catalog.pg_policy AS bp ON bp.polrelid = fam.base_oid AND bp.polname = c.base_policy
        ORDER BY c.base_policy
    LOOP
        missing_column := sql_saga.__internal_policy_missing_history_column(d.base_policy_oid, fam.history_oid);
        IF missing_column IS NOT NULL THEN
            RAISE EXCEPTION 'sql_saga: policy % on % references column %, which % lacks',
                d.base_policy, fam.base_oid::regclass, missing_column, fam.history_oid::regclass;
        END IF;
    END LOOP;

    -- The policy DDL below is the synchronizer's own: the event triggers recognize it by the guard
    -- row inserted above (sql_saga.__internal_history_policy_sync_active), neither refusing it nor
    -- starting a nested synchronization. The row is removed at the end, or with the (sub)transaction
    -- on error; nested calls only change its depth, so they cannot end it for their caller.

    -- RLS enabled, FORCE mirrored from the base.
    IF NOT (SELECT c.relrowsecurity FROM pg_catalog.pg_class AS c WHERE c.oid = fam.history_oid) THEN
        stmt := format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', fam.history_oid::regclass);
        EXECUTE stmt;
        RAISE DEBUG 'sql_saga: %', stmt;
    END IF;
    IF (SELECT c.relforcerowsecurity FROM pg_catalog.pg_class AS c WHERE c.oid = fam.history_oid)
       IS DISTINCT FROM (SELECT c.relforcerowsecurity FROM pg_catalog.pg_class AS c WHERE c.oid = fam.base_oid) THEN
        force_mode := CASE WHEN (SELECT c.relforcerowsecurity FROM pg_catalog.pg_class AS c WHERE c.oid = fam.base_oid)
                           THEN 'FORCE' ELSE 'NO FORCE' END;
        EXECUTE format('ALTER TABLE %s %s ROW LEVEL SECURITY', fam.history_oid::regclass, force_mode);
        RAISE NOTICE 'sql_saga: Set row level security on % to %', fam.history_oid::regclass, force_mode;
    END IF;

    -- The two fixed policies.
    -- regclass constants: PostgreSQL records a dependency on the base and pg_dump writes them by name.
    reader_qual := format('(has_table_privilege(%1$L::regclass::oid, ''SELECT''::text) AND (NOT row_security_active(%1$L::regclass::oid)))', sql_saga.__internal_qualified_name(fam.base_oid));
    base_select_qual := format('has_table_privilege(%L::regclass::oid, ''SELECT''::text)', sql_saga.__internal_qualified_name(fam.base_oid));
    FOR p IN
        SELECT sql_saga.__internal_history_reader_policy_name() AS name, true AS permissive, reader_qual AS qual
        UNION ALL
        SELECT sql_saga.__internal_history_base_select_policy_name(), false, base_select_qual
    LOOP
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_policy AS hp WHERE hp.polrelid = fam.history_oid AND hp.polname = p.name) THEN
            stmt := format('CREATE POLICY %I ON %s AS %s FOR SELECT TO PUBLIC USING (%s)',
                p.name, fam.history_oid::regclass, CASE WHEN p.permissive THEN 'PERMISSIVE' ELSE 'RESTRICTIVE' END, p.qual);
            EXECUTE stmt;
            -- Part of creating the history (add_system_versioning), or a repair: no NOTICE.
            RAISE DEBUG 'sql_saga: %', stmt;
        END IF;
    END LOOP;

    -- Drop fixed policies of another installation (replayed from a dump).
    FOR p IN
        SELECT hp.polname
        FROM pg_catalog.pg_policy AS hp
        WHERE hp.polrelid = fam.history_oid
          AND hp.polname ~ '^sql_saga_history_(reader|base_select)_'
          AND hp.polname NOT IN (sql_saga.__internal_history_reader_policy_name(),
                                 sql_saga.__internal_history_base_select_policy_name())
        ORDER BY hp.polname
    LOOP
        EXECUTE format('DROP POLICY %I ON %s', p.polname, fam.history_oid::regclass);
        RAISE DEBUG 'sql_saga: dropped policy % on %, a fixed policy of another installation', p.polname, fam.history_oid::regclass;
    END LOOP;

    -- Drop copies that are orphaned or stale. The base policy of a copy is found by its hash, among
    -- the current base policies and those just dropped (passed by the sql_drop event trigger).
    FOR p IN
        SELECT hp.polname,
               coalesce(c.base_policy,
                        (SELECT dp FROM unnest(dropped_base_policies) AS dp
                         WHERE sql_saga.__internal_history_copy_name(dp) = hp.polname LIMIT 1)) AS base_policy
        FROM pg_catalog.pg_policy AS hp
        LEFT JOIN sql_saga.__internal_history_policy_copies(fam.base_oid) AS c ON c.copy_name = hp.polname
        WHERE hp.polrelid = fam.history_oid
          AND hp.polname ~ '^sql_saga_[0-9a-f]{16}$'
          AND (c.copy_name IS NULL
               OR hp.polcmd <> 'r' OR hp.polpermissive <> c.permissive OR hp.polroles <> c.roles
               OR pg_catalog.pg_get_expr(hp.polqual, hp.polrelid) IS DISTINCT FROM c.qual)
        ORDER BY hp.polname
    LOOP
        EXECUTE format('DROP POLICY %I ON %s', p.polname, fam.history_oid::regclass);
        IF p.base_policy IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: Dropped policy % on %, the history copy of policy % on %',
                p.polname, fam.history_oid::regclass, p.base_policy, fam.base_oid::regclass;
        ELSE
            RAISE NOTICE 'sql_saga: Dropped policy % on %, which copies no current policy of %',
                p.polname, fam.history_oid::regclass, fam.base_oid::regclass;
        END IF;
    END LOOP;

    -- Create missing copies.
    FOR d IN
        SELECT c.*, bp.oid AS base_policy_oid
        FROM sql_saga.__internal_history_policy_copies(fam.base_oid) AS c
        JOIN pg_catalog.pg_policy AS bp ON bp.polrelid = fam.base_oid AND bp.polname = c.base_policy
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_policy AS hp WHERE hp.polrelid = fam.history_oid AND hp.polname = c.copy_name)
        ORDER BY c.base_policy
    LOOP
        missing_column := sql_saga.__internal_policy_missing_history_column(d.base_policy_oid, fam.history_oid);
        IF missing_column IS NOT NULL THEN
            RAISE EXCEPTION 'sql_saga: policy % on % references column %, which % lacks',
                d.base_policy, fam.base_oid::regclass, missing_column, fam.history_oid::regclass;
        END IF;
        stmt := format('CREATE POLICY %I ON %s AS %s FOR SELECT TO %s USING (%s)',
            d.copy_name, fam.history_oid::regclass,
            CASE WHEN d.permissive THEN 'PERMISSIVE' ELSE 'RESTRICTIVE' END,
            sql_saga.__internal_policy_role_list(d.roles), coalesce(d.qual, 'true'));
        BEGIN
            EXECUTE stmt;
        EXCEPTION WHEN undefined_table OR undefined_column THEN
            -- A self-reference by the base table's name inside a subquery cannot be resolved on history.
            RAISE EXCEPTION 'sql_saga: policy % on % cannot be applied to %: %',
                d.base_policy, fam.base_oid::regclass, fam.history_oid::regclass, SQLERRM;
        END;
        RAISE NOTICE 'sql_saga: Created policy % on % as the history copy of policy % on %',
            d.copy_name, fam.history_oid::regclass, d.base_policy, fam.base_oid::regclass;
    END LOOP;

    UPDATE sql_saga.history_policy_sync_guard AS g
    SET nesting_depth = g.nesting_depth - 1
    WHERE g.backend_pid = pg_catalog.pg_backend_pid()
      AND g.transaction_id = pg_catalog.pg_current_xact_id()::text::bigint
      AND (g.base_schema, g.base_table) = (
          SELECT n.nspname, c.relname
          FROM pg_catalog.pg_class AS c
          JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
          WHERE c.oid = fam.base_oid)
      AND g.nesting_depth > 1;
    IF NOT FOUND THEN
        DELETE FROM sql_saga.history_policy_sync_guard AS g
        WHERE g.backend_pid = pg_catalog.pg_backend_pid()
          AND g.transaction_id = pg_catalog.pg_current_xact_id()::text::bigint
          AND (g.base_schema, g.base_table) = (
              SELECT n.nspname, c.relname
              FROM pg_catalog.pg_class AS c
              JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
              WHERE c.oid = fam.base_oid);
    END IF;
END;
$__internal_sync_history_policies$;

ALTER TABLE sql_saga.system_time_era
ADD COLUMN table_columns_snapshot name[];

UPDATE sql_saga.system_time_era AS ste
SET table_columns_snapshot = (
    SELECT pg_catalog.array_agg(a.attname ORDER BY a.attnum)
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
    WHERE (n.nspname, c.relname) = (ste.table_schema, ste.table_name)
      AND a.attnum > 0
      AND NOT a.attisdropped
);

ALTER TABLE sql_saga.system_time_era
ALTER COLUMN table_columns_snapshot SET NOT NULL;

CREATE OR REPLACE FUNCTION sql_saga.__internal_alter_table_changes_column_shape(command pg_ddl_command)
RETURNS boolean
AS 'sql_saga', 'alter_table_changes_column_shape'
LANGUAGE c STRICT;

REVOKE ALL ON FUNCTION sql_saga.__internal_alter_table_changes_column_shape(pg_ddl_command) FROM PUBLIC;

CREATE FUNCTION sql_saga.__internal_refresh_system_time_columns_snapshot(table_oid oid)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog
AS $__internal_refresh_system_time_columns_snapshot$
    UPDATE sql_saga.system_time_era AS ste
    SET table_columns_snapshot = live.column_names
    FROM (
        SELECT pg_catalog.array_agg(a.attname ORDER BY a.attnum) AS column_names
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = table_oid
          AND a.attnum > 0
          AND NOT a.attisdropped
    ) AS live
    WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = table_oid;
$__internal_refresh_system_time_columns_snapshot$;

REVOKE ALL ON FUNCTION sql_saga.__internal_refresh_system_time_columns_snapshot(oid) FROM PUBLIC;

DO $upgrade_snapshot_routines$
DECLARE
    definition text;
    changed text;
BEGIN
    definition := pg_catalog.pg_get_functiondef(
        'sql_saga.__internal_add_system_time_era(regclass,name,name,name,name,name,name[])'::regprocedure);
    changed := pg_catalog.replace(definition,
$old_add$
    INSERT INTO sql_saga.system_time_era (table_schema, table_name, era_name, infinity_check_constraint, generated_always_trigger, write_history_trigger, truncate_trigger, excluded_column_names)
    VALUES (table_schema, table_name, era_name, infinity_check_constraint, generated_always_trigger, write_history_trigger, truncate_trigger, excluded_column_names);
$old_add$,
$new_add$
    INSERT INTO sql_saga.system_time_era (
        table_schema, table_name, era_name, infinity_check_constraint,
        generated_always_trigger, write_history_trigger, truncate_trigger,
        excluded_column_names, table_columns_snapshot)
    SELECT table_schema, table_name, era_name, infinity_check_constraint,
           generated_always_trigger, write_history_trigger, truncate_trigger,
           excluded_column_names,
           pg_catalog.array_agg(a.attname ORDER BY a.attnum)
    FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = table_oid
      AND a.attnum > 0
      AND NOT a.attisdropped
    GROUP BY table_schema, table_name, era_name, infinity_check_constraint,
             generated_always_trigger, write_history_trigger, truncate_trigger,
             excluded_column_names;
$new_add$);
    IF changed = definition THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade __internal_add_system_time_era snapshot lifecycle';
    END IF;
    EXECUTE changed;

    definition := pg_catalog.pg_get_functiondef(
        'sql_saga.__internal_reconcile_system_versioning_family(regclass,name[])'::regprocedure);
    changed := pg_catalog.replace(definition,
$old_reconcile_entry$
    END IF;
    history_table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name, sv.history_table_name))::oid;
$old_reconcile_entry$,
$new_reconcile_entry$
    END IF;
    IF current_setting('sql_saga.rebuilding_system_versioning_views', true) OPERATOR(pg_catalog.=) table_oid::oid::text THEN
        PERFORM sql_saga.__internal_sync_history_policies(table_oid);
        PERFORM sql_saga.__internal_refresh_system_time_columns_snapshot(table_oid);
        RETURN;
    END IF;
    history_table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name, sv.history_table_name))::oid;
$new_reconcile_entry$);
    changed := pg_catalog.replace(changed,
$old_reconcile_rebuild$
      IF needs_rebuild THEN
          PERFORM sql_saga.__internal_rebuild_system_versioning_views(table_oid, coalesce(without_columns, '{}'));
      END IF;
$old_reconcile_rebuild$,
$new_reconcile_rebuild$
      IF needs_rebuild THEN
          PERFORM sql_saga.__internal_rebuild_system_versioning_views(table_oid, coalesce(without_columns, '{}'));
          PERFORM sql_saga.__internal_refresh_system_time_columns_snapshot(table_oid);
      END IF;
$new_reconcile_rebuild$);
    IF changed = definition THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade __internal_reconcile_system_versioning_family snapshot lifecycle';
    END IF;
    EXECUTE changed;

    definition := pg_catalog.pg_get_functiondef(
        'sql_saga.rebuild_system_versioning_views(regclass,name[])'::regprocedure);
    changed := pg_catalog.replace(definition,
$old_public_declare$
    range_column name;
    excluded name;
$old_public_declare$,
$new_public_declare$
    range_column name;
    excluded name;
    previous_rebuild_flag text;
$new_public_declare$);
    changed := pg_catalog.replace(changed,
$old_public_rebuild$
    PERFORM sql_saga.__internal_rebuild_system_versioning_views(table_oid, without_columns);

    -- Copies of B's policies refer to columns by name: renames on both tables, or policy changes
    -- made while event triggers were off, are synchronized here (refused if a column is missing).
    PERFORM sql_saga.__internal_sync_history_policies(table_oid);
$old_public_rebuild$,
$new_public_rebuild$
    PERFORM sql_saga.__internal_rebuild_system_versioning_views(table_oid, without_columns);

    -- Use the existing authorized reconciler as the private refresh gateway.
    previous_rebuild_flag := current_setting('sql_saga.rebuilding_system_versioning_views', true);
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', table_oid::oid::text, true);
    BEGIN
        PERFORM sql_saga.__internal_reconcile_system_versioning_family(table_oid, without_columns);
    EXCEPTION WHEN OTHERS THEN
        PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_rebuild_flag, ''), true);
        RAISE;
    END;
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_rebuild_flag, ''), true);
$new_public_rebuild$);
    IF changed = definition THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade rebuild_system_versioning_views snapshot lifecycle';
    END IF;
    EXECUTE changed;
END;
$upgrade_snapshot_routines$;

/*
 * Names in the NOTICEs that announce what sql_saga created or dropped: quoted identifiers
 * (quote_ident / %I.%I), so names containing dots, quotes or parentheses stay unambiguous, with the
 * schema left out for public. Fixed rather than search_path dependent, because the announcing
 * functions run with their own search_path. The function form is read from the catalog by OID (so it
 * never parses identifier text) and must be computed while the function exists: schema.name and the
 * argument types, as regprocedure prints them.
 */
CREATE FUNCTION sql_saga.__internal_notice_name(schema_name name, object_name text)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog
AS $__internal_notice_name$
    SELECT CASE WHEN schema_name = 'public' THEN quote_ident(object_name) ELSE format('%I.%I', schema_name, object_name) END;
$__internal_notice_name$;

CREATE FUNCTION sql_saga.__internal_notice_function_name(function_oid oid)
RETURNS text
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $__internal_notice_function_name$
    SELECT CASE WHEN n.nspname = 'public' THEN quote_ident(p.proname) ELSE format('%I.%I', n.nspname, p.proname) END
        || '(' || coalesce((SELECT string_agg(format_type(a.type_oid, NULL), ',' ORDER BY a.ordinality)
                            FROM unnest(p.proargtypes::oid[]) WITH ORDINALITY AS a (type_oid, ordinality)), '') || ')'
    FROM pg_proc AS p
    JOIN pg_namespace AS n ON n.oid = p.pronamespace
    WHERE p.oid = function_oid;
$__internal_notice_function_name$;

REVOKE ALL ON FUNCTION sql_saga.__internal_notice_name(name, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_notice_function_name(oid) FROM PUBLIC;

-- The system time era announces the column, constraint and triggers it creates.
DO $upgrade_add_system_time_era_notices$
DECLARE
    definition text;
    anchor record;
BEGIN
    definition := pg_catalog.pg_get_functiondef(
        'sql_saga.__internal_add_system_time_era(regclass,name,name,name,name,name,name[])'::regprocedure);
    -- Fail closed: the body must be the released shape. No creation NOTICE yet (so a rerun or an
    -- already patched body is refused), and every anchor exactly once (replace() would patch each copy).
    IF pg_catalog.strpos(definition, 'sql_saga: Added column') > 0
       OR pg_catalog.strpos(definition, 'sql_saga: Created constraint') > 0
       OR pg_catalog.strpos(definition, 'sql_saga: Created trigger') > 0 THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade __internal_add_system_time_era creation notices: the body already announces creations';
    END IF;
    FOR anchor IN
        SELECT a.label, a.old_text, a.new_text
        FROM (VALUES
            ('ALTER TABLE', $old_era_alter$
        EXECUTE format('ALTER TABLE %I.%I %s', table_schema /* %I */, table_name /* %I */, array_to_string(alter_commands, ', ') /* %s */);
    END IF;
$old_era_alter$, $new_era_alter$
        EXECUTE format('ALTER TABLE %I.%I %s', table_schema /* %I */, table_name /* %I */, array_to_string(alter_commands, ', ') /* %s */);
    END IF;
    IF range_attnum = 0 THEN
        RAISE NOTICE 'sql_saga: Added column "%" to table "%" for system time era', quote_ident(range_column_name), sql_saga.__internal_notice_name(table_schema, table_name);
    END IF;
    RAISE NOTICE 'sql_saga: Created constraint "%" on table "%" for system time era', quote_ident(infinity_check_constraint), sql_saga.__internal_notice_name(table_schema, table_name);
$new_era_alter$),
            ('generated always trigger', $old_era_generated$
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.generated_always_as_row_start_end(%L)', generated_always_trigger, table_oid, range_column_name);
$old_era_generated$, $new_era_generated$
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.generated_always_as_row_start_end(%L)', generated_always_trigger, table_oid, range_column_name);
    RAISE NOTICE 'sql_saga: Created trigger "%" on table "%" for system time era', quote_ident(generated_always_trigger), sql_saga.__internal_notice_name(table_schema, table_name);
$new_era_generated$),
            ('write history trigger', $old_era_write$
    EXECUTE format('CREATE TRIGGER %I AFTER UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.write_history(%L)', write_history_trigger, table_oid, range_column_name);
$old_era_write$, $new_era_write$
    EXECUTE format('CREATE TRIGGER %I AFTER UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.write_history(%L)', write_history_trigger, table_oid, range_column_name);
    RAISE NOTICE 'sql_saga: Created trigger "%" on table "%" for system time era', quote_ident(write_history_trigger), sql_saga.__internal_notice_name(table_schema, table_name);
$new_era_write$),
            ('truncate trigger', $old_era_truncate$
    EXECUTE format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE PROCEDURE sql_saga.truncate_system_versioning()', truncate_trigger /* %I */, table_oid /* %s */);
$old_era_truncate$, $new_era_truncate$
    EXECUTE format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE PROCEDURE sql_saga.truncate_system_versioning()', truncate_trigger /* %I */, table_oid /* %s */);
    RAISE NOTICE 'sql_saga: Created trigger "%" on table "%" for system time era', quote_ident(truncate_trigger), sql_saga.__internal_notice_name(table_schema, table_name);
$new_era_truncate$)
        ) AS a (label, old_text, new_text)
    LOOP
        IF (pg_catalog.length(definition) - pg_catalog.length(pg_catalog.replace(definition, anchor.old_text, ''))) / pg_catalog.length(anchor.old_text) <> 1 THEN
            RAISE EXCEPTION 'sql_saga: cannot upgrade __internal_add_system_time_era creation notices (%): the anchor is not present exactly once', anchor.label;
        END IF;
        definition := pg_catalog.replace(definition, anchor.old_text, anchor.new_text);
        IF (pg_catalog.length(definition) - pg_catalog.length(pg_catalog.replace(definition, anchor.new_text, ''))) / pg_catalog.length(anchor.new_text) <> 1 THEN
            RAISE EXCEPTION 'sql_saga: cannot upgrade __internal_add_system_time_era creation notices (%): the new text is not present exactly once', anchor.label;
        END IF;
    END LOOP;
    -- Postcheck: each creation NOTICE exactly as often as the patch adds it.
    IF (SELECT count(*) FROM pg_catalog.regexp_matches(definition, 'RAISE NOTICE ''sql_saga: Added column', 'g')) <> 1
       OR (SELECT count(*) FROM pg_catalog.regexp_matches(definition, 'RAISE NOTICE ''sql_saga: Created constraint', 'g')) <> 1
       OR (SELECT count(*) FROM pg_catalog.regexp_matches(definition, 'RAISE NOTICE ''sql_saga: Created trigger', 'g')) <> 3 THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade __internal_add_system_time_era creation notices: unexpected NOTICE count after patching';
    END IF;
    EXECUTE definition;
END;
$upgrade_add_system_time_era_notices$;

/*
 * Re-create the referenced-side triggers of a regular foreign key, under their stored names, with
 * arguments taken from the current sql_saga.foreign_keys row (used after a referencing column rename).
 * drop_protection refuses dropping a trigger that sql_saga.foreign_keys names, so the names are
 * withdrawn from the row for the duration of the DROP and restored afterwards (the metadata-first
 * pattern of drop_foreign_key_by_name). Everything runs in the caller's transaction; no setting or
 * argument lets a caller skip the protection.
 */
CREATE FUNCTION sql_saga.__internal_recreate_regular_fk_uk_triggers(foreign_key_name name)
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_recreate_regular_fk_uk_triggers$
#variable_conflict use_variable
DECLARE
    fk sql_saga.foreign_keys;
    uk sql_saga.unique_keys;
    uk_era sql_saga.era;
    uk_table_oid regclass;
    update_columns text;
    update_enabled "char";
    delete_enabled "char";
BEGIN
    SELECT * INTO fk FROM sql_saga.foreign_keys AS f
    WHERE f.foreign_key_name = foreign_key_name AND f.type = 'regular_to_temporal';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'sql_saga: regular foreign key "%" not found', foreign_key_name;
    END IF;
    SELECT * INTO uk FROM sql_saga.unique_keys AS u WHERE u.unique_key_name = fk.unique_key_name;
    SELECT * INTO uk_era FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name);
    uk_table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name));
    IF uk_table_oid IS NULL OR uk_era.era_name IS NULL THEN
        RAISE EXCEPTION 'sql_saga: referenced table of foreign key "%" not found', foreign_key_name;
    END IF;

    SELECT pg_catalog.string_agg(pg_catalog.quote_ident(u.column_name), ', ' ORDER BY u.ordinality)
    INTO update_columns
    FROM pg_catalog.unnest(uk.column_names || uk_era.valid_from_column_name || uk_era.valid_until_column_name)
         WITH ORDINALITY AS u (column_name, ordinality);

    /* The firing mode (DISABLE / ENABLE / ENABLE REPLICA / ENABLE ALWAYS) of each trigger survives the rebuild. */
    SELECT t.tgenabled INTO update_enabled FROM pg_catalog.pg_trigger AS t
    WHERE t.tgrelid = uk_table_oid AND t.tgname = fk.uk_update_trigger;
    SELECT t.tgenabled INTO delete_enabled FROM pg_catalog.pg_trigger AS t
    WHERE t.tgrelid = uk_table_oid AND t.tgname = fk.uk_delete_trigger;

    UPDATE sql_saga.foreign_keys AS f
    SET uk_update_trigger = NULL, uk_delete_trigger = NULL
    WHERE f.foreign_key_name = fk.foreign_key_name;

    IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = uk_table_oid AND t.tgname = fk.uk_update_trigger) THEN
        EXECUTE pg_catalog.format('DROP TRIGGER %I ON %s', fk.uk_update_trigger, uk_table_oid);
    END IF;
    IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = uk_table_oid AND t.tgname = fk.uk_delete_trigger) THEN
        EXECUTE pg_catalog.format('DROP TRIGGER %I ON %s', fk.uk_delete_trigger, uk_table_oid);
    END IF;

    /* Same definitions as add_regular_foreign_key. */
    EXECUTE pg_catalog.format($$
        CREATE CONSTRAINT TRIGGER %1$I AFTER UPDATE OF %2$s ON %3$I.%4$I FROM %5$I.%6$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
        sql_saga.uk_update_check_c(%7$L, %5$L, %6$L, %8$L, %9$L, %10$L, %11$L, %3$L, %4$L, %12$L, %13$L, %14$L, %15$L, %16$L, %17$L, %18$L, 'regular_to_temporal');
        $$
        , /* %1$I */ fk.uk_update_trigger
        , /* %2$s */ update_columns
        , /* %3$I */ uk.table_schema
        , /* %4$I */ uk.table_name
        , /* %5$I */ fk.table_schema
        , /* %6$I */ fk.table_name
        , /* %7$L */ fk.foreign_key_name
        , /* %8$L */ fk.column_names::text
        , /* %9$L */ ''
        , /* %10$L*/ ''
        , /* %11$L*/ ''
        , /* %12$L*/ uk.column_names::text
        , /* %13$L*/ uk_era.era_name
        , /* %14$L*/ uk_era.valid_from_column_name
        , /* %15$L*/ uk_era.valid_until_column_name
        , /* %16$L*/ fk.match_type
        , /* %17$L*/ fk.update_action
        , /* %18$L*/ fk.delete_action
    );
    EXECUTE pg_catalog.format($$
        CREATE CONSTRAINT TRIGGER %1$I AFTER DELETE ON %2$I.%3$I FROM %4$I.%5$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
        sql_saga.uk_delete_check_c(%6$L, %4$L, %5$L, %7$L, %8$L, %9$L, %10$L, %2$L, %3$L, %11$L, %12$L, %13$L, %14$L, %15$L, %16$L, %17$L, 'regular_to_temporal');
        $$
        , /* %1$I */ fk.uk_delete_trigger
        , /* %2$I */ uk.table_schema
        , /* %3$I */ uk.table_name
        , /* %4$I */ fk.table_schema
        , /* %5$I */ fk.table_name
        , /* %6$L */ fk.foreign_key_name
        , /* %7$L */ fk.column_names::text
        , /* %8$L */ ''
        , /* %9$L */ ''
        , /* %10$L*/ ''
        , /* %11$L*/ uk.column_names::text
        , /* %12$L*/ uk_era.era_name
        , /* %13$L*/ uk_era.valid_from_column_name
        , /* %14$L*/ uk_era.valid_until_column_name
        , /* %15$L*/ fk.match_type
        , /* %16$L*/ fk.update_action
        , /* %17$L*/ fk.delete_action
    );

    /* A new trigger fires in origin mode ('O'); reapply each trigger's previous mode. */
    IF update_enabled IS DISTINCT FROM 'O' AND update_enabled IS NOT NULL THEN
        EXECUTE pg_catalog.format('ALTER TABLE %s %s TRIGGER %I', uk_table_oid,
            CASE update_enabled WHEN 'D' THEN 'DISABLE' WHEN 'R' THEN 'ENABLE REPLICA' WHEN 'A' THEN 'ENABLE ALWAYS' END,
            fk.uk_update_trigger);
    END IF;
    IF delete_enabled IS DISTINCT FROM 'O' AND delete_enabled IS NOT NULL THEN
        EXECUTE pg_catalog.format('ALTER TABLE %s %s TRIGGER %I', uk_table_oid,
            CASE delete_enabled WHEN 'D' THEN 'DISABLE' WHEN 'R' THEN 'ENABLE REPLICA' WHEN 'A' THEN 'ENABLE ALWAYS' END,
            fk.uk_delete_trigger);
    END IF;

    UPDATE sql_saga.foreign_keys AS f
    SET uk_update_trigger = fk.uk_update_trigger, uk_delete_trigger = fk.uk_delete_trigger
    WHERE f.foreign_key_name = fk.foreign_key_name;
END;
$__internal_recreate_regular_fk_uk_triggers$;

REVOKE ALL ON FUNCTION sql_saga.__internal_recreate_regular_fk_uk_triggers(name) FROM PUBLIC;

/*
 * The canonical body of a regular foreign key's helper function, for the current columns of the
 * referenced unique key: exactly what add_regular_foreign_key generates. NULL when a key column is
 * missing or has no default btree equality operator.
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_helper_body(foreign_key_name name)
RETURNS text
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_helper_body$
    SELECT CASE WHEN w.missing OR w.found_columns IS DISTINCT FROM w.stable_columns THEN NULL
           ELSE pg_catalog.format($body$
                SELECT EXISTS (SELECT 1 FROM %I.%I AS uk WHERE %s);
            $body$, uk.table_schema, uk.table_name, w.where_clause)
           END
    FROM sql_saga.foreign_keys AS fk
    JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
    JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
    CROSS JOIN LATERAL (
        SELECT pg_catalog.string_agg(pg_catalog.format('uk.%I OPERATOR(%I.=) $%s',
                                                       u.name, coalesce(eq.nspname, ''), u.ordinality),
                                     ' AND ' ORDER BY u.ordinality) AS where_clause,
               pg_catalog.bool_or(eq.oid IS NULL) AS missing,
               pg_catalog.count(*) AS found_columns,
               (SELECT pg_catalog.count(*) FROM pg_catalog.unnest(uk.column_names) AS c(name)
                WHERE c.name OPERATOR(pg_catalog.<>) e.range_column_name) AS stable_columns
        FROM pg_catalog.unnest(ARRAY(
                 SELECT c.name FROM pg_catalog.unnest(uk.column_names) WITH ORDINALITY AS c(name, n)
                 WHERE c.name OPERATOR(pg_catalog.<>) e.range_column_name ORDER BY c.n))
             WITH ORDINALITY AS u(name, ordinality)
        JOIN pg_catalog.pg_attribute AS ua
          ON ua.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name))
         AND ua.attname = u.name AND ua.attnum > 0 AND NOT ua.attisdropped
        LEFT JOIN LATERAL (
            SELECT o.oid, n.nspname
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
        ) AS eq ON true
    ) AS w
    WHERE fk.foreign_key_name = $1 AND fk.type = 'regular_to_temporal';
$__internal_regular_fk_helper_body$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_helper_body(name) FROM PUBLIC;

/*
 * The canonical arguments (pg_trigger.tgargs) of a regular foreign key's referenced-side triggers,
 * built from the catalog exactly as add_regular_foreign_key passes them.
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_trigger_args(foreign_key_name name)
RETURNS bytea
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_trigger_args$
    SELECT pg_catalog.string_agg(
               pg_catalog.convert_to(a.arg, pg_catalog.getdatabaseencoding()) OPERATOR(pg_catalog.||) '\x00'::bytea,
               ''::bytea ORDER BY a.n)
    FROM sql_saga.foreign_keys AS fk
    JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
    JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
    CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[
        fk.foreign_key_name::text, fk.table_schema::text, fk.table_name::text, fk.column_names::text,
        '', '', '',
        uk.table_schema::text, uk.table_name::text, uk.column_names::text, e.era_name::text,
        coalesce(e.valid_from_column_name::text, 'null'), coalesce(e.valid_until_column_name::text, 'null'),
        fk.match_type::text, fk.update_action::text, fk.delete_action::text, 'regular_to_temporal'
    ]) WITH ORDINALITY AS a(arg, n)
    WHERE fk.foreign_key_name = $1 AND fk.type = 'regular_to_temporal';
$__internal_regular_fk_trigger_args$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_trigger_args(name) FROM PUBLIC;

/*
 * Whether a regular foreign key's helper function body and both referenced-side trigger argument
 * lists are the canonical ones for the current catalog (used by check_integrity).
 */
CREATE FUNCTION sql_saga.__internal_regular_foreign_key_is_attributable(foreign_key_name name)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_foreign_key_is_attributable$
    SELECT coalesce((
        SELECT pg_catalog.btrim(p.prosrc, E' \t\r\n')
               OPERATOR(pg_catalog.=) pg_catalog.btrim(sql_saga.__internal_regular_fk_helper_body(fk.foreign_key_name), E' \t\r\n')
        FROM pg_catalog.pg_proc AS p
        WHERE p.oid = pg_catalog.to_regprocedure(fk.fk_helper_function)
    ) AND NOT EXISTS (
        SELECT FROM pg_catalog.pg_trigger AS t
        WHERE t.tgrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name))
          AND t.tgname IN (fk.uk_update_trigger, fk.uk_delete_trigger)
          AND t.tgargs IS DISTINCT FROM sql_saga.__internal_regular_fk_trigger_args(fk.foreign_key_name)
    ), false)
    FROM sql_saga.foreign_keys AS fk
    JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
    WHERE fk.foreign_key_name = $1 AND fk.type = 'regular_to_temporal';
$__internal_regular_foreign_key_is_attributable$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_foreign_key_is_attributable(name) FROM PUBLIC;

/*
 * The canonical body of a regular foreign key helper function for the given referenced key columns
 * (stable columns, in key order). Each column is compared with the default btree equality operator
 * of the helper's argument type at the same position; add_regular_foreign_key requires equal
 * referencing and referenced column types, so this equals the body generated from the referenced
 * table, and it does not need the named columns to exist (it also describes the helper as it was
 * before a referenced column rename). NULL when an argument type has no such operator or the column
 * count differs from the argument count.
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_helper_body_for(helper regprocedure, uk_schema name, uk_table name, uk_columns name[])
RETURNS text
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_helper_body_for$
    SELECT CASE WHEN w.missing OR w.found_columns IS DISTINCT FROM p.pronargs::bigint THEN NULL
           ELSE pg_catalog.format($body$
                SELECT EXISTS (SELECT 1 FROM %I.%I AS uk WHERE %s);
            $body$, uk_schema, uk_table, w.where_clause)
           END
    FROM pg_catalog.pg_proc AS p
    CROSS JOIN LATERAL (
        SELECT pg_catalog.string_agg(pg_catalog.format('uk.%I OPERATOR(%I.=) $%s',
                                                       u.name, coalesce(eq.nspname, ''), u.ordinality),
                                     ' AND ' ORDER BY u.ordinality) AS where_clause,
               pg_catalog.bool_or(eq.oid IS NULL) AS missing,
               pg_catalog.count(*) AS found_columns
        FROM pg_catalog.unnest(uk_columns) WITH ORDINALITY AS u(name, ordinality)
        LEFT JOIN LATERAL (
            SELECT o.oid, n.nspname
            FROM pg_catalog.pg_opclass AS opc
            JOIN pg_catalog.pg_am AS am ON am.oid = opc.opcmethod AND am.amname = 'btree'
            JOIN pg_catalog.pg_amop AS amop ON amop.amopfamily = opc.opcfamily
                 AND amop.amoplefttype = opc.opcintype AND amop.amoprighttype = opc.opcintype
                 AND amop.amopstrategy = 3
            JOIN pg_catalog.pg_operator AS o ON o.oid = amop.amopopr
            JOIN pg_catalog.pg_namespace AS n ON n.oid = o.oprnamespace
            WHERE opc.opcdefault
              AND opc.opcintype = (SELECT coalesce(nullif(t.typbasetype, 0), t.oid)
                                   FROM pg_catalog.pg_type AS t
                                   WHERE t.oid = p.proargtypes[(u.ordinality - 1)::integer])
            LIMIT 1
        ) AS eq ON true
    ) AS w
    WHERE p.oid = helper;
$__internal_regular_fk_helper_body_for$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_helper_body_for(regprocedure, name, name, name[]) FROM PUBLIC;

/*
 * Whether a function is exactly the helper add_regular_foreign_key generates for the given
 * referenced key columns: an SQL, STABLE STRICT, SECURITY DEFINER boolean function with only IN
 * arguments, the canonical body, and search_path = pg_catalog as its only search_path setting
 * (other per-function settings are the owner's choice and are kept).
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_helper_is_canonical(helper regprocedure, uk_schema name, uk_table name, uk_columns name[])
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_helper_is_canonical$
    SELECT coalesce((
        SELECT p.prolang OPERATOR(pg_catalog.=) (SELECT l.oid FROM pg_catalog.pg_language AS l WHERE l.lanname OPERATOR(pg_catalog.=) 'sql')
           AND p.prorettype OPERATOR(pg_catalog.=) 'pg_catalog.bool'::pg_catalog.regtype
           AND p.prokind OPERATOR(pg_catalog.=) 'f' AND NOT p.proretset
           AND p.proisstrict AND p.provolatile OPERATOR(pg_catalog.=) 's'
           AND p.prosecdef
           AND p.proargmodes IS NULL
           AND 'search_path=pg_catalog' OPERATOR(pg_catalog.=) ANY (p.proconfig)
           AND NOT EXISTS (
               SELECT FROM pg_catalog.unnest(p.proconfig) AS c(setting)
               WHERE c.setting LIKE 'search\_path=%' AND c.setting OPERATOR(pg_catalog.<>) 'search_path=pg_catalog')
           AND pg_catalog.btrim(p.prosrc, E' \t\r\n') OPERATOR(pg_catalog.=)
               pg_catalog.btrim(sql_saga.__internal_regular_fk_helper_body_for(helper, uk_schema, uk_table, uk_columns), E' \t\r\n')
        FROM pg_catalog.pg_proc AS p
        WHERE p.oid OPERATOR(pg_catalog.=) helper
    ), false);
$__internal_regular_fk_helper_is_canonical$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_helper_is_canonical(regprocedure, name, name, name[]) FROM PUBLIC;

/*
 * Regenerate, in place, the helper function of a regular foreign key with the canonical body for
 * the current referenced key columns (after a referenced-column rename). Only the body changes:
 * the function is re-created from its own pg_get_functiondef header, so its identity, owner,
 * privileges, SECURITY DEFINER and every per-function setting (proconfig) are kept, and the CHECK
 * constraints that call it stay valid. The caller has attributed the helper beforehand.
 */
CREATE FUNCTION sql_saga.__internal_regenerate_regular_fk_helper(foreign_key_name name)
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_regenerate_regular_fk_helper$
#variable_conflict use_variable
DECLARE
    helper regprocedure;
    body text;
    definition text;
    old_source text;
    dollar_tag text := '$function';
    old_suffix text;
BEGIN
    SELECT pg_catalog.to_regprocedure(fk.fk_helper_function) INTO helper
    FROM sql_saga.foreign_keys AS fk
    WHERE fk.foreign_key_name = foreign_key_name AND fk.type = 'regular_to_temporal';
    body := sql_saga.__internal_regular_fk_helper_body(foreign_key_name);
    IF helper IS NULL OR body IS NULL THEN
        RAISE EXCEPTION 'sql_saga: cannot regenerate the helper function of foreign key "%"', foreign_key_name;
    END IF;
    definition := pg_catalog.pg_get_functiondef(helper);
    SELECT p.prosrc INTO old_source FROM pg_catalog.pg_proc AS p WHERE p.oid = helper;
    -- pg_get_functiondef quotes the body with "$function", extended by "x" while the body contains
    -- that prefix, then "$". Derive the same tag, so the expected suffix is exact for any body.
    WHILE pg_catalog.strpos(old_source, dollar_tag) > 0 LOOP
        dollar_tag := dollar_tag || 'x';
    END LOOP;
    dollar_tag := dollar_tag || '$';
    old_suffix := 'AS ' || dollar_tag || old_source || dollar_tag || E'\n';
    IF pg_catalog.left(definition, 27) IS DISTINCT FROM 'CREATE OR REPLACE FUNCTION '
       OR pg_catalog.right(definition, pg_catalog.length(old_suffix)) IS DISTINCT FROM old_suffix THEN
        RAISE EXCEPTION 'sql_saga: cannot regenerate the helper function of foreign key "%"', foreign_key_name;
    END IF;
    EXECUTE pg_catalog.left(definition, pg_catalog.length(definition) - pg_catalog.length(old_suffix))
        || pg_catalog.format('AS %L', body);
END;
$__internal_regenerate_regular_fk_helper$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regenerate_regular_fk_helper(name) FROM PUBLIC;

/*
 * Before a referenced key column rename changes anything: for the unique keys on the given
 * relations (those touched by the ALTER TABLE) that are referenced by a regular foreign key and
 * have a catalogued column missing from the table (a renamed key column), require that every such
 * foreign key's helper function and referenced-side triggers are exactly what sql_saga generated for
 * the columns before the rename. Refuses otherwise, so nothing a user changed is overwritten.
 * Returns the key columns before the rename, by unique key name.
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_key_rename_prepare(table_oids oid[])
RETURNS jsonb
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_key_rename_prepare$
#variable_conflict use_variable
DECLARE
    uk_rec record;
    fk_rec record;
    stable_columns name[];
    columns_before jsonb := '{}'::jsonb;
BEGIN
    FOR uk_rec IN
        SELECT uk.unique_key_name, uk.table_schema, uk.table_name, uk.column_names, e.range_column_name, c.oid AS uk_table_oid
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        JOIN sql_saga.unique_keys AS uk ON (uk.table_schema, uk.table_name) = (n.nspname, c.relname)
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        WHERE c.oid = ANY (table_oids)
          AND EXISTS (SELECT FROM sql_saga.foreign_keys AS fk
                      WHERE fk.unique_key_name = uk.unique_key_name AND fk.type = 'regular_to_temporal')
          AND EXISTS (SELECT FROM pg_catalog.unnest(uk.column_names) AS col(name)
                      WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                                        WHERE a.attrelid = c.oid AND a.attname = col.name
                                          AND a.attnum > 0 AND NOT a.attisdropped))
        ORDER BY uk.unique_key_name COLLATE "C"
    LOOP
        stable_columns := ARRAY(
            SELECT col.name FROM pg_catalog.unnest(uk_rec.column_names) WITH ORDINALITY AS col(name, n)
            WHERE col.name <> uk_rec.range_column_name ORDER BY col.n);
        FOR fk_rec IN
            SELECT fk.foreign_key_name, fk.fk_helper_function, fk.uk_update_trigger, fk.uk_delete_trigger
            FROM sql_saga.foreign_keys AS fk
            WHERE fk.unique_key_name = uk_rec.unique_key_name AND fk.type = 'regular_to_temporal'
            ORDER BY fk.foreign_key_name COLLATE "C"
        LOOP
            IF NOT sql_saga.__internal_regular_fk_helper_is_canonical(
                    pg_catalog.to_regprocedure(fk_rec.fk_helper_function),
                    uk_rec.table_schema, uk_rec.table_name, stable_columns) THEN
                RAISE EXCEPTION 'sql_saga: cannot rename a column of unique key "%" on table "%.%": helper function % of regular foreign key "%" is not the canonical sql_saga helper; restore it, or drop the foreign key with drop_foreign_key first',
                    uk_rec.unique_key_name, uk_rec.table_schema, uk_rec.table_name, fk_rec.fk_helper_function, fk_rec.foreign_key_name;
            END IF;
            IF EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = uk_rec.uk_table_oid
                  AND t.tgname IN (fk_rec.uk_update_trigger, fk_rec.uk_delete_trigger)
                  AND (t.tgfoid NOT IN ('sql_saga.uk_update_check_c()'::regprocedure, 'sql_saga.uk_delete_check_c()'::regprocedure)
                       OR t.tgargs IS DISTINCT FROM sql_saga.__internal_regular_fk_trigger_args(fk_rec.foreign_key_name))) THEN
                RAISE EXCEPTION 'sql_saga: cannot rename a column of unique key "%" on table "%.%": a referenced-side trigger of regular foreign key "%" is not the canonical sql_saga trigger; repair with drop_foreign_key and add_regular_foreign_key first',
                    uk_rec.unique_key_name, uk_rec.table_schema, uk_rec.table_name, fk_rec.foreign_key_name;
            END IF;
        END LOOP;
        columns_before := columns_before || pg_catalog.jsonb_build_object(uk_rec.unique_key_name, uk_rec.column_names);
    END LOOP;
    RETURN columns_before;
END;
$__internal_regular_fk_key_rename_prepare$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_key_rename_prepare(oid[]) FROM PUBLIC;

/*
 * After the unique key columns followed a rename: regenerate, in place, the helper function and the
 * referenced-side triggers of every regular foreign key on a key whose columns changed (same names,
 * owner, privileges, settings and firing modes; the helper body and the trigger arguments name the
 * current columns). columns_before comes from __internal_regular_fk_key_rename_prepare.
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_key_rename_apply(columns_before jsonb)
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_key_rename_apply$
#variable_conflict use_variable
DECLARE
    fk_rec record;
BEGIN
    FOR fk_rec IN
        SELECT fk.foreign_key_name
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        WHERE fk.type = 'regular_to_temporal'
          AND columns_before ? uk.unique_key_name::text
          AND (columns_before -> uk.unique_key_name::text) IS DISTINCT FROM pg_catalog.to_jsonb(uk.column_names)
        ORDER BY fk.foreign_key_name COLLATE "C"
    LOOP
        PERFORM sql_saga.__internal_regenerate_regular_fk_helper(fk_rec.foreign_key_name);
        PERFORM sql_saga.__internal_recreate_regular_fk_uk_triggers(fk_rec.foreign_key_name);
    END LOOP;
END;
$__internal_regular_fk_key_rename_apply$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_key_rename_apply(jsonb) FROM PUBLIC;


DO $upgrade_rename_following$
DECLARE
    definition text := pg_catalog.pg_get_functiondef('sql_saga.rename_following()'::regprocedure);
    changed text;
BEGIN
    changed := pg_catalog.replace(definition,
$old_declare$    cmd record;
$old_declare$,
$new_declare$    cmd record;
    v_command_changes_shape boolean;
    v_shape_changed_table_oids oid[] := ARRAY[]::oid[];
    v_table_oid oid;
    v_current_columns name[];
    v_removed_columns name[];
    v_added_columns name[];
    v_missing_excluded name;
$new_declare$);
    changed := pg_catalog.replace(changed,
$old_collect$    FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF cmd.command_tag = 'ALTER TABLE' THEN
            is_alter_table := true;
            EXIT;
        END IF;
    END LOOP;
$old_collect$,
$new_collect$    FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF cmd.command_tag = 'ALTER TABLE' THEN
            is_alter_table := true;
            IF cmd.classid = 'pg_catalog.pg_class'::regclass THEN
                v_command_changes_shape := cmd.objsubid > 0
                    OR sql_saga.__internal_alter_table_changes_column_shape(cmd.command);

                IF v_command_changes_shape THEN
                    IF EXISTS (
                        SELECT 1
                        FROM sql_saga.system_time_era AS ste
                        JOIN pg_catalog.pg_class AS c
                          ON c.oid = cmd.objid
                        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                        WHERE (ste.table_schema, ste.table_name) = (n.nspname, c.relname))
                       AND NOT cmd.objid = ANY (v_shape_changed_table_oids) THEN
                        v_shape_changed_table_oids := pg_catalog.array_append(v_shape_changed_table_oids, cmd.objid);
                    END IF;

                    -- PostgreSQL propagates column-shape changes through table inheritance. Avoid all
                    -- pg_inherits work for the overwhelmingly common no-children case.
                    IF (SELECT c.relhassubclass FROM pg_catalog.pg_class AS c WHERE c.oid = cmd.objid) THEN
                        FOR v_table_oid IN
                            WITH RECURSIVE descendants(table_oid) AS (
                                SELECT i.inhrelid
                                FROM pg_catalog.pg_inherits AS i
                                WHERE i.inhparent = cmd.objid
                                UNION
                                SELECT i.inhrelid
                                FROM descendants AS d
                                JOIN pg_catalog.pg_inherits AS i ON i.inhparent = d.table_oid
                            )
                            SELECT c.oid
                            FROM descendants AS d
                            JOIN pg_catalog.pg_class AS c ON c.oid = d.table_oid
                            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                            JOIN sql_saga.system_time_era AS ste
                              ON (ste.table_schema, ste.table_name) = (n.nspname, c.relname)
                            JOIN LATERAL (
                                SELECT pg_catalog.array_agg(a.attname ORDER BY a.attname COLLATE "C") AS column_names
                                FROM pg_catalog.pg_attribute AS a
                                WHERE a.attrelid = c.oid
                                  AND a.attnum > 0
                                  AND NOT a.attisdropped
                            ) AS live ON true
                            -- Set comparison: only added/removed columns are shape changes.
                            WHERE (SELECT pg_catalog.array_agg(s.attname ORDER BY s.attname COLLATE "C")
                                   FROM pg_catalog.unnest(ste.table_columns_snapshot) AS s(attname))
                                  IS DISTINCT FROM live.column_names
                        LOOP
                            IF NOT v_table_oid = ANY (v_shape_changed_table_oids) THEN
                                v_shape_changed_table_oids := pg_catalog.array_append(
                                    v_shape_changed_table_oids, v_table_oid);
                            END IF;
                        END LOOP;
                    END IF;
                END IF;
            END IF;
        END IF;
    END LOOP;
$new_collect$);
    changed := pg_catalog.replace(changed,
$old_fast_exit$    IF NOT v_is_alter_trigger AND NOT sql_saga.__internal_ddl_command_affects_managed_object() THEN
$old_fast_exit$,
$new_fast_exit$    IF NOT v_is_alter_trigger
       AND cardinality(v_shape_changed_table_oids) = 0
       AND NOT sql_saga.__internal_ddl_command_affects_managed_object() THEN
$new_fast_exit$);
    changed := pg_catalog.replace(changed,
$old_excluded$    /*
     * We can't reliably find out what a column was renamed to, so just error
     * out in this case. Note: This is also checked in drop_protection.sql.
     */
    FOR r IN
        SELECT format('%I.%I', ste.table_schema, ste.table_name)::regclass AS table_oid, u.column_name
        FROM sql_saga.system_time_era AS ste
        CROSS JOIN LATERAL unnest(ste.excluded_column_names) AS u (column_name)
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (to_regclass(format('%I.%I', ste.table_schema, ste.table_name)), u.column_name))
    LOOP
        RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is excluded from system_time era',
            r.column_name, sql_saga.__internal_display_name(r.table_oid);
    END LOOP;
$old_excluded$,
$new_excluded$    /*
     * Attribute base-column changes only to registered base relations present in this ALTER TABLE
     * invocation. The persisted snapshot supplies the pre-command names without storing OIDs.
     */
    FOREACH v_table_oid IN ARRAY v_shape_changed_table_oids
    LOOP
        SELECT pg_catalog.array_agg(a.attname ORDER BY a.attnum)
        INTO v_current_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = v_table_oid
          AND a.attnum > 0
          AND NOT a.attisdropped;

        SELECT pg_catalog.array_agg(s.column_name ORDER BY s.ordinality)
        INTO v_removed_columns
        FROM sql_saga.system_time_era AS ste
        CROSS JOIN LATERAL pg_catalog.unnest(ste.table_columns_snapshot) WITH ORDINALITY AS s(column_name, ordinality)
        WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = v_table_oid
          AND NOT s.column_name = ANY (v_current_columns);

        SELECT pg_catalog.array_agg(c.column_name ORDER BY c.ordinality)
        INTO v_added_columns
        FROM pg_catalog.unnest(v_current_columns) WITH ORDINALITY AS c(column_name, ordinality)
        JOIN sql_saga.system_time_era AS ste
          ON pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = v_table_oid
        WHERE NOT c.column_name = ANY (ste.table_columns_snapshot);

        SELECT removed.column_name
        INTO v_missing_excluded
        FROM pg_catalog.unnest(coalesce(v_removed_columns, ARRAY[]::name[])) AS removed(column_name)
        JOIN sql_saga.system_time_era AS ste
          ON pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = v_table_oid
        WHERE removed.column_name = ANY (ste.excluded_column_names)
        ORDER BY removed.column_name
        LIMIT 1;

        IF v_missing_excluded IS NOT NULL THEN
            IF cardinality(v_removed_columns) = 1 AND cardinality(v_added_columns) = 1 THEN
                RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is excluded from system_time era',
                    v_missing_excluded, sql_saga.__internal_display_name(v_table_oid);
            END IF;
            RAISE EXCEPTION 'cannot determine whether excluded column "%" on table "%" was renamed because ALTER TABLE changed multiple columns; split column-shape changes into separate ALTER TABLE statements',
                v_missing_excluded, sql_saga.__internal_display_name(v_table_oid);
        END IF;
    END LOOP;
$new_excluded$);
    changed := pg_catalog.replace(changed,
$old_tail$    END IF;

END;
$old_tail$,
$new_tail$    END IF;

    IF cardinality(v_shape_changed_table_oids) > 0 THEN
        UPDATE sql_saga.system_time_era AS ste
        SET table_columns_snapshot = live.column_names
        FROM (
            SELECT c.oid AS table_oid,
                   pg_catalog.array_agg(a.attname ORDER BY a.attnum) AS column_names
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
            WHERE c.oid = ANY (v_shape_changed_table_oids)
              AND a.attnum > 0
              AND NOT a.attisdropped
            GROUP BY c.oid
        ) AS live
        WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = live.table_oid;
    END IF;

END;
$new_tail$);
    changed := pg_catalog.replace(changed,
$old_begin$BEGIN
    FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
$old_begin$,
$new_begin$BEGIN
    /* DROP statements cannot rename or reshape surviving managed objects. */
    IF tg_tag LIKE 'DROP %' THEN
        RETURN;
    END IF;

    FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
$new_begin$);
    changed := pg_catalog.replace(changed,
$old_fk_snapshot$                -- If columns are unchanged, we are done with this table
                IF snapshot = current_columns THEN
                    CONTINUE;
                END IF;
$old_fk_snapshot$,
$new_fk_snapshot$                -- If columns are unchanged, we are done with this table
                -- Set comparison: a restore can recreate the table with another column order.
                IF (SELECT array_agg(u.c ORDER BY u.c COLLATE "C") FROM unnest(snapshot) AS u(c))
                   IS NOT DISTINCT FROM
                   (SELECT array_agg(u.c ORDER BY u.c COLLATE "C") FROM unnest(current_columns) AS u(c)) THEN
                    CONTINUE;
                END IF;
$new_fk_snapshot$);
    changed := pg_catalog.replace(changed,
$old_fk_rename$                    -- We have a rename. Find all affected FKs, rename their triggers and update their metadata.
                    DECLARE
                        fk_rec RECORD;
                        uk_rec RECORD;
                        new_column_names name[];
                        new_foreign_key_name name;
                        new_uk_update_trigger name;
                        new_uk_delete_trigger name;
                    BEGIN
                        FOR fk_rec IN
                            SELECT * FROM sql_saga.foreign_keys
                            WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name)
                              AND old_col_name = ANY(column_names)
                        LOOP
                            -- Get UK info for uk-side trigger renames
                            SELECT * INTO uk_rec FROM sql_saga.unique_keys WHERE unique_key_name = fk_rec.unique_key_name;

                            -- 1. Calculate all the new names
                            DECLARE
                                old_base_foreign_key_name name;
                                new_base_foreign_key_name name;
                                name_suffix text;
                            BEGIN
                                new_column_names := array_replace(fk_rec.column_names, old_col_name, new_col_name);

                                -- To correctly rename, we must preserve any suffix that was added
                                -- to the original foreign key name to ensure uniqueness.
                                old_base_foreign_key_name := sql_saga.__internal_make_name(
                                    ARRAY[fk_rec.table_name] || fk_rec.column_names || ARRAY[fk_rec.fk_era_name]
                                );
                                name_suffix := regexp_replace(fk_rec.foreign_key_name, '^' || old_base_foreign_key_name, '');

                                new_base_foreign_key_name := sql_saga.__internal_make_name(
                                    ARRAY[fk_rec.table_name] || new_column_names || ARRAY[fk_rec.fk_era_name]
                                );
                                new_foreign_key_name := new_base_foreign_key_name || name_suffix;

                                -- Regenerate trigger names with the full new name
                                new_uk_update_trigger := sql_saga.__internal_make_name(ARRAY[new_foreign_key_name], 'uk_update');
                                new_uk_delete_trigger := sql_saga.__internal_make_name(ARRAY[new_foreign_key_name], 'uk_delete');
                            END;

                            -- It appears that Postgres automatically renames triggers when a column that is part
                            -- of the trigger's name is renamed. Therefore, we do not need to rename them
                            -- manually; we only need to update our metadata to reflect the new names.
                            UPDATE sql_saga.foreign_keys
                            SET
                                foreign_key_name = new_foreign_key_name,
                                column_names = new_column_names,
                                uk_update_trigger = new_uk_update_trigger,
                                uk_delete_trigger = new_uk_delete_trigger
                            WHERE foreign_key_name = fk_rec.foreign_key_name;
                        END LOOP;
                    END;
                END IF;

$old_fk_rename$,
$new_fk_rename$                    -- We have a rename. PostgreSQL renames neither constraints, indexes nor triggers when a
                    -- column is renamed, so every stored object name stays unchanged; only column_names and
                    -- the snapshot follow. A regular FK's referenced-side triggers carry the referencing
                    -- column list in their arguments, so they are re-created with the same names.
                    DECLARE
                        fk_rec RECORD;
                    BEGIN
                        -- Snapshot first: the trigger DDL below re-enters this event trigger, which must
                        -- then see no pending rename.
                        UPDATE sql_saga.foreign_keys
                        SET fk_table_columns_snapshot = current_columns
                        WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name);

                        FOR fk_rec IN
                            SELECT * FROM sql_saga.foreign_keys
                            WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name)
                              AND old_col_name = ANY(column_names)
                            ORDER BY foreign_key_name
                        LOOP
                            UPDATE sql_saga.foreign_keys
                            SET column_names = array_replace(fk_rec.column_names, old_col_name, new_col_name)
                            WHERE foreign_key_name = fk_rec.foreign_key_name;

                            IF fk_rec.type = 'regular_to_temporal' THEN
                                PERFORM sql_saga.__internal_recreate_regular_fk_uk_triggers(fk_rec.foreign_key_name);
                            END IF;
                        END LOOP;
                    END;
                END IF;

$new_fk_rename$);
    IF pg_catalog.strpos(changed, 'It appears that Postgres automatically renames triggers') > 0
       OR pg_catalog.strpos(changed, 'PERFORM sql_saga.__internal_recreate_regular_fk_uk_triggers(fk_rec.foreign_key_name);') = 0 THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade rename_following foreign key column rename';
    END IF;
    IF pg_catalog.strpos(changed, $old_rk0$    v_missing_excluded name;
$old_rk0$) = 0 OR pg_catalog.strpos(changed, $mark_rk0$    v_unique_key_columns_before jsonb;
$mark_rk0$) > 0 THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade rename_following referenced key column rename';
    END IF;
    changed := pg_catalog.replace(changed, $old_rk0$    v_missing_excluded name;
$old_rk0$, $new_rk0$    v_missing_excluded name;
    v_unique_key_columns_before jsonb;
    v_altered_table_oids oid[];
$new_rk0$);
    IF pg_catalog.strpos(changed, $old_rk1$    ---
    --- unique_keys
    ---
$old_rk1$) = 0 OR pg_catalog.strpos(changed, $mark_rk1$-- A renamed column of a unique key referenced by a regular foreign key: attribute the dependent
$mark_rk1$) > 0 THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade rename_following referenced key column rename';
    END IF;
    changed := pg_catalog.replace(changed, $old_rk1$    ---
    --- unique_keys
    ---
$old_rk1$, $new_rk1$    -- A renamed column of a unique key referenced by a regular foreign key: attribute the dependent
    -- helper functions and referenced-side triggers before anything changes, and remember the key
    -- columns, so that they can be regenerated below. Only relations touched by this ALTER TABLE
    -- are considered; any other ALTER TABLE skips this work.
    IF is_alter_table THEN
        v_altered_table_oids := ARRAY(
            SELECT DISTINCT c.objid
            FROM pg_catalog.pg_event_trigger_ddl_commands() AS c
            WHERE c.command_tag = 'ALTER TABLE' AND c.classid = 'pg_catalog.pg_class'::regclass);
        IF EXISTS (
            SELECT FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            JOIN sql_saga.unique_keys AS uk ON (uk.table_schema, uk.table_name) = (n.nspname, c.relname)
            JOIN sql_saga.foreign_keys AS fk ON fk.unique_key_name = uk.unique_key_name AND fk.type = 'regular_to_temporal'
            WHERE c.oid = ANY (v_altered_table_oids)) THEN
            v_unique_key_columns_before := sql_saga.__internal_regular_fk_key_rename_prepare(v_altered_table_oids);
        END IF;
    END IF;

    ---
    --- unique_keys
    ---
$new_rk1$);
    IF pg_catalog.strpos(changed, $old_rk2$    ---
    --- foreign_keys
    ---
$old_rk2$) = 0 OR pg_catalog.strpos(changed, $mark_rk2$PERFORM sql_saga.__internal_regular_fk_key_rename_apply(v_unique_key_columns_before);$mark_rk2$) > 0 THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade rename_following referenced key column rename';
    END IF;
    changed := pg_catalog.replace(changed, $old_rk2$    ---
    --- foreign_keys
    ---
$old_rk2$, $new_rk2$    -- A renamed referenced key column: regenerate the dependent regular foreign key objects.
    IF v_unique_key_columns_before IS DISTINCT FROM '{}'::jsonb AND v_unique_key_columns_before IS NOT NULL THEN
        PERFORM sql_saga.__internal_regular_fk_key_rename_apply(v_unique_key_columns_before);
    END IF;

    ---
    --- foreign_keys
    ---
$new_rk2$);
    IF pg_catalog.strpos(changed, 'v_unique_key_columns_before jsonb;') = 0
       OR pg_catalog.strpos(changed, 'v_altered_table_oids oid[];') = 0
       OR pg_catalog.strpos(changed, 'PERFORM sql_saga.__internal_regular_fk_key_rename_apply(v_unique_key_columns_before);') = 0
       OR pg_catalog.strpos(changed, 'v_unique_key_columns_before := sql_saga.__internal_regular_fk_key_rename_prepare(v_altered_table_oids);') = 0 THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade rename_following referenced key column rename';
    END IF;
    IF pg_catalog.strpos(changed, 'IF snapshot = current_columns THEN') > 0
       OR pg_catalog.strpos(changed, '-- Set comparison: a restore can recreate the table with another column order.') = 0 THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade rename_following foreign key snapshot comparison';
    END IF;
    IF changed = definition THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade rename_following snapshot attribution';
    END IF;
    EXECUTE changed;
END;
$upgrade_rename_following$;

/* ITEM 5 (era column renames): BEGIN. Follow era range/boundary/valid_to column renames: the era catalog
 * follows, the synchronization function is replaced in place (CREATE OR REPLACE, same OID, owner, privileges;
 * its trigger keeps OID and firing mode), the referenced-side triggers of regular foreign keys are re-created
 * (same names and firing modes), and the boundary CHECK is recognized as add_era creates it. Before anything
 * changes, the synchronization function and trigger and the referenced-side triggers must be exactly what
 * sql_saga generated, and the function must have no other dependent; otherwise the rename is refused.
 * Byte-identical to src/3e_synchronize_temporal_columns_trigger.sql (generator, procedure) and
 * src/2f_rename_following.sql. Exact replacements in rename_following; each fails closed when the released
 * fragment is absent or the new one is already present.
 */
/*
 * The SQL add_synchronize_temporal_columns_trigger executes for an era, generated from the catalog
 * (pure: reads only): the CREATE OR REPLACE FUNCTION statement of the table-specific synchronization
 * function, the CREATE TRIGGER statement of its trigger, and their names. Also used to attribute an
 * existing synchronization function and trigger, and to regenerate the function in place.
 */
CREATE FUNCTION sql_saga.__internal_synchronize_temporal_columns_sql(
    table_oid regclass, era_name name,
    OUT function_name name, OUT trigger_name name, OUT function_sql text, OUT trigger_sql text)
LANGUAGE plpgsql STABLE
SET search_path = sql_saga, pg_catalog, public
AS $__internal_synchronize_temporal_columns_sql$
DECLARE
    v_table_schema name;
    v_table_name name;

    v_range_col name;
    v_from_col name;
    v_until_col name;
    v_to_col name;

    v_range_type regtype;
    v_range_subtype regtype;
    v_apply_defaults boolean;

    v_func_name name;
    v_trg_name name;

    v_update_of_cols name[];
    v_update_of_cols_sql text;

    v_range_col_ident text;
    v_from_col_ident text;
    v_until_col_ident text;
    v_to_col_ident text;

    v_range_col_lit text;
    v_from_col_lit text;
    v_until_col_lit text;
    v_to_col_lit text;

    v_range_type_sql text;
    v_range_subtype_sql text;

    v_to_decl text := '';
    v_to_changed_decl text := '';
    v_to_changed_detect text := '';
    v_to_extract text := '';
    v_to_collect_end text := '';
    v_to_error_end_src text := '';
    v_to_populate text := '';

    v_defaults_logic text := '';
    v_validation_error text;

    specialized_trigger_template text;
    specialized_trigger_sql text;
    specialized_trigger_create_trigger_sql text;
BEGIN
    SELECT n.nspname, c.relname
    INTO v_table_schema, v_table_name
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = __internal_synchronize_temporal_columns_sql.table_oid;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table % does not exist', table_oid;
    END IF;

    SELECT
        e.range_column_name,
        e.valid_from_column_name,
        e.valid_until_column_name,
        e.valid_to_column_name,
        e.range_type,
        e.range_subtype,
        e.trigger_applies_defaults
    INTO
        v_range_col,
        v_from_col,
        v_until_col,
        v_to_col,
        v_range_type,
        v_range_subtype,
        v_apply_defaults
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (v_table_schema, v_table_name, __internal_synchronize_temporal_columns_sql.era_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Era "%" not found on table %', era_name, table_oid;
    END IF;

    IF v_from_col IS NULL OR v_until_col IS NULL THEN
        RAISE EXCEPTION 'Template sync trigger requires valid_from_column_name and valid_until_column_name (era "%" on %)', era_name, table_oid;
    END IF;

    IF v_to_col IS NOT NULL AND v_from_col IS NULL THEN
        RAISE EXCEPTION 'valid_to_column_name requires valid_from_column_name and valid_until_column_name (era "%" on %)', era_name, table_oid;
    END IF;

    v_func_name := sql_saga.__internal_make_name(ARRAY[v_table_schema, v_table_name, era_name], 'template_sync');
    v_trg_name := sql_saga.__internal_make_name(ARRAY[v_table_name, era_name], 'sync_temporal_trg');

    v_range_col_ident := quote_ident(v_range_col);
    v_from_col_ident := quote_ident(v_from_col);
    v_until_col_ident := quote_ident(v_until_col);

    v_range_col_lit := quote_literal(v_range_col);
    v_from_col_lit := quote_literal(v_from_col);
    v_until_col_lit := quote_literal(v_until_col);

    v_range_type_sql := v_range_type::text;
    v_range_subtype_sql := v_range_subtype::text;

    v_update_of_cols := ARRAY[v_range_col];

    IF v_to_col IS NOT NULL THEN
        v_to_col_ident := quote_ident(v_to_col);
        v_to_col_lit := quote_literal(v_to_col);
        v_update_of_cols := v_update_of_cols || v_to_col;

        v_to_decl := $to_decl$
    until_via_to text;
$to_decl$;

        v_to_changed_decl := $to_changed_decl$
    is_to_changed boolean;
$to_changed_decl$;

        v_to_changed_detect := $to_changed_detect$
    is_to_changed := (TG_OP = 'INSERT' AND NEW.__TO_COL_IDENT__ IS NOT NULL)
        OR (TG_OP = 'UPDATE' AND NEW.__TO_COL_IDENT__ IS DISTINCT FROM OLD.__TO_COL_IDENT__);
$to_changed_detect$;

        v_to_extract := $to_extract$
    IF is_to_changed THEN
        until_via_to := (NEW.__TO_COL_IDENT__::__SUBTYPE_TYPE__ + 1)::text;
    END IF;
$to_extract$;

        v_to_collect_end := $to_collect_end$
    IF is_to_changed THEN
        IF final_from IS NULL THEN
            RAISE EXCEPTION 'When setting "%", the start of the period must also be provided via "%" or "%".', __TO_COL_LITERAL__, __FROM_COL_LITERAL__, __RANGE_COL_LITERAL__;
        END IF;
        until_sources := until_sources || until_via_to;
    END IF;
$to_collect_end$;

        v_to_error_end_src := $to_error_end_src$
                UNION ALL SELECT __TO_COL_LITERAL__ AS key, until_via_to AS val WHERE is_to_changed
$to_error_end_src$;

        v_to_populate := $to_populate$
    IF final_until = 'infinity' THEN
        NEW.__TO_COL_IDENT__ := 'infinity'::__SUBTYPE_TYPE__;
    ELSE
        NEW.__TO_COL_IDENT__ := (final_until::__SUBTYPE_TYPE__ - 1);
    END IF;
$to_populate$;

        v_validation_error := $validation_error$
        RAISE EXCEPTION 'The temporal period could not be determined. At least one of "%", "%" (with "%"), or the pair "%"/"%" must be provided.', __RANGE_COL_LITERAL__, __TO_COL_LITERAL__, __FROM_COL_LITERAL__, __FROM_COL_LITERAL__, __UNTIL_COL_LITERAL__;
$validation_error$;
    ELSE
        v_validation_error := $validation_error$
        RAISE EXCEPTION 'The temporal period could not be determined. At least one of "%" or the pair "%"/"%" must be provided.', __RANGE_COL_LITERAL__, __FROM_COL_LITERAL__, __UNTIL_COL_LITERAL__;
$validation_error$;
    END IF;

    v_update_of_cols := v_update_of_cols || v_from_col || v_until_col;

    IF v_apply_defaults THEN
        v_defaults_logic := $defaults$
    IF final_until IS NULL THEN
        final_until := 'infinity';
    END IF;
$defaults$;
    END IF;

    SELECT string_agg(quote_ident(c), ', ')
    INTO v_update_of_cols_sql
    FROM unnest(v_update_of_cols) AS c;

    specialized_trigger_template := $template$
CREATE OR REPLACE FUNCTION sql_saga.__FUNC_NAME__()
RETURNS trigger
LANGUAGE plpgsql AS $template_sync_trigger$
DECLARE
    final_from text;
    final_until text;

    from_via_bounds text;
    until_via_bounds text;
    from_via_range text;
    until_via_range text;

__TO_DECL__

    from_sources text[] := ARRAY[]::text[];
    until_sources text[] := ARRAY[]::text[];
    distinct_from text[];
    distinct_until text[];

    is_range_changed boolean;
    is_from_changed boolean;
    is_until_changed boolean;
    is_to_changed boolean := false;

    has_from_input boolean;
    has_until_input boolean;
BEGIN
    IF NEW.__RANGE_COL_IDENT__ = 'empty'::__RANGE_TYPE__ THEN
        RAISE EXCEPTION 'Cannot use an empty range for temporal column "%"', __RANGE_COL_LITERAL__;
    END IF;

    is_range_changed := (TG_OP = 'INSERT' AND NEW.__RANGE_COL_IDENT__ IS NOT NULL)
        OR (TG_OP = 'UPDATE' AND NEW.__RANGE_COL_IDENT__ IS DISTINCT FROM OLD.__RANGE_COL_IDENT__);

    is_from_changed := (TG_OP = 'INSERT' AND NEW.__FROM_COL_IDENT__ IS NOT NULL)
        OR (TG_OP = 'UPDATE' AND NEW.__FROM_COL_IDENT__ IS DISTINCT FROM OLD.__FROM_COL_IDENT__);

    is_until_changed := (TG_OP = 'INSERT' AND NEW.__UNTIL_COL_IDENT__ IS NOT NULL)
        OR (TG_OP = 'UPDATE' AND NEW.__UNTIL_COL_IDENT__ IS DISTINCT FROM OLD.__UNTIL_COL_IDENT__);

__TO_CHANGED_DETECT__

    has_from_input := is_range_changed OR is_from_changed;
    has_until_input := is_range_changed OR is_until_changed OR is_to_changed;

    IF is_range_changed THEN
        from_via_range := lower(NEW.__RANGE_COL_IDENT__)::text;
        until_via_range := upper(NEW.__RANGE_COL_IDENT__)::text;
    END IF;

    IF is_from_changed THEN
        from_via_bounds := NEW.__FROM_COL_IDENT__::text;
    END IF;

    IF is_until_changed THEN
        until_via_bounds := NEW.__UNTIL_COL_IDENT__::text;
    END IF;

__TO_EXTRACT__

    IF is_range_changed AND from_via_range IS NOT NULL THEN
        from_sources := from_sources || from_via_range;
    END IF;
    IF is_from_changed AND from_via_bounds IS NOT NULL THEN
        from_sources := from_sources || from_via_bounds;
    END IF;

    SELECT array_agg(DISTINCT v) INTO distinct_from FROM unnest(from_sources) AS v;

    IF cardinality(distinct_from) > 1 THEN
        RAISE EXCEPTION 'Inconsistent start of period provided. Sources: %',
            (SELECT jsonb_object_agg(key, val)
             FROM (
                SELECT __RANGE_COL_LITERAL__ AS key, from_via_range AS val WHERE is_range_changed AND from_via_range IS NOT NULL
                UNION ALL SELECT __FROM_COL_LITERAL__ AS key, from_via_bounds AS val WHERE is_from_changed AND from_via_bounds IS NOT NULL
             ) AS s);
    END IF;

    IF cardinality(distinct_from) = 1 THEN
        final_from := distinct_from[1];
    ELSIF TG_OP = 'UPDATE' AND NOT has_from_input THEN
        final_from := OLD.__FROM_COL_IDENT__::text;
    END IF;

    IF is_range_changed AND until_via_range IS NOT NULL THEN
        until_sources := until_sources || until_via_range;
    END IF;
    IF is_until_changed AND until_via_bounds IS NOT NULL THEN
        until_sources := until_sources || until_via_bounds;
    END IF;

__TO_COLLECT_END__

    SELECT array_agg(DISTINCT v) INTO distinct_until FROM unnest(until_sources) AS v;

    IF cardinality(distinct_until) > 1 THEN
        RAISE EXCEPTION 'Inconsistent end of period provided. Sources: %',
            (SELECT jsonb_object_agg(key, val)
             FROM (
                SELECT __RANGE_COL_LITERAL__ AS key, until_via_range AS val WHERE is_range_changed AND until_via_range IS NOT NULL
                UNION ALL SELECT __UNTIL_COL_LITERAL__ AS key, until_via_bounds AS val WHERE is_until_changed AND until_via_bounds IS NOT NULL
__TO_ERROR_END_SRC__
             ) AS s);
    END IF;

    IF cardinality(distinct_until) = 1 THEN
        final_until := distinct_until[1];
    ELSIF TG_OP = 'UPDATE' AND NOT has_until_input THEN
        final_until := OLD.__UNTIL_COL_IDENT__::text;
    END IF;

__DEFAULTS__

    IF final_from IS NULL OR final_until IS NULL THEN
__VALIDATION_ERROR__
    END IF;

    NEW.__FROM_COL_IDENT__ := final_from::__SUBTYPE_TYPE__;
    NEW.__UNTIL_COL_IDENT__ := final_until::__SUBTYPE_TYPE__;

__TO_POPULATE__

    BEGIN
        NEW.__RANGE_COL_IDENT__ := __RANGE_TYPE__(final_from::__SUBTYPE_TYPE__, final_until::__SUBTYPE_TYPE__, '[)');
    EXCEPTION WHEN data_exception THEN
        -- Let the table's CHECK constraint handle invalid bounds (e.g., from >= until).
    END;

    RETURN NEW;
END;
$template_sync_trigger$;
$template$;

    specialized_trigger_sql := specialized_trigger_template;

    -- First insert all optional blocks, then do identifier/type replacement.
    specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_DECL__', v_to_decl);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_CHANGED_DETECT__', v_to_changed_detect);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_EXTRACT__', v_to_extract);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_COLLECT_END__', v_to_collect_end);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_ERROR_END_SRC__', v_to_error_end_src);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_POPULATE__', v_to_populate);

    specialized_trigger_sql := replace(specialized_trigger_sql, '__DEFAULTS__', v_defaults_logic);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__VALIDATION_ERROR__', v_validation_error);

    IF v_to_col IS NOT NULL THEN
        specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_COL_IDENT__', v_to_col_ident);
        specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_COL_LITERAL__', v_to_col_lit);
    ELSE
        specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_COL_IDENT__', 'NULL');
        specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_COL_LITERAL__', 'NULL');
    END IF;

    specialized_trigger_sql := replace(specialized_trigger_sql, '__FUNC_NAME__', quote_ident(v_func_name));

    specialized_trigger_sql := replace(specialized_trigger_sql, '__RANGE_COL_IDENT__', v_range_col_ident);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__FROM_COL_IDENT__', v_from_col_ident);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__UNTIL_COL_IDENT__', v_until_col_ident);

    specialized_trigger_sql := replace(specialized_trigger_sql, '__RANGE_COL_LITERAL__', v_range_col_lit);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__FROM_COL_LITERAL__', v_from_col_lit);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__UNTIL_COL_LITERAL__', v_until_col_lit);

    specialized_trigger_sql := replace(specialized_trigger_sql, '__RANGE_TYPE__', v_range_type_sql);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__SUBTYPE_TYPE__', v_range_subtype_sql);

    specialized_trigger_create_trigger_sql := format($$CREATE TRIGGER %1$I BEFORE INSERT OR UPDATE OF %2$s ON %3$s FOR EACH ROW EXECUTE FUNCTION sql_saga.%4$I()$$,
        v_trg_name /* %1$I */,
        v_update_of_cols_sql /* %2$s */,
        table_oid /* %3$s */,
        v_func_name /* %4$I */
    );

    function_name := v_func_name;
    trigger_name := v_trg_name;
    function_sql := specialized_trigger_sql;
    trigger_sql := specialized_trigger_create_trigger_sql;
END;
$__internal_synchronize_temporal_columns_sql$;

REVOKE ALL ON FUNCTION sql_saga.__internal_synchronize_temporal_columns_sql(regclass, name) FROM PUBLIC;

/*
 * add_synchronize_temporal_columns_trigger: the released 1.1 (and 1.0) definition, which generates inline,
 * is replaced by the one that uses __internal_synchronize_temporal_columns_sql (same generated objects).
 * Fails closed unless the installed definition is the released one.
 */
DO $upgrade_add_synchronize_temporal_columns_trigger$
BEGIN
    IF pg_catalog.md5(pg_catalog.pg_get_functiondef('sql_saga.add_synchronize_temporal_columns_trigger(regclass,name)'::regprocedure))
       <> '99c1db7fdeb02d24a00967dcdc9e9d70' THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade add_synchronize_temporal_columns_trigger';
    END IF;
END;
$upgrade_add_synchronize_temporal_columns_trigger$;

CREATE OR REPLACE PROCEDURE sql_saga.add_synchronize_temporal_columns_trigger(table_oid regclass, era_name name)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, public
AS $add_synchronize_temporal_columns_trigger$
DECLARE
    v_table_schema name;
    v_table_name name;
    generated record;
BEGIN
    SELECT n.nspname, c.relname
    INTO v_table_schema, v_table_name
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = add_synchronize_temporal_columns_trigger.table_oid;

    generated := sql_saga.__internal_synchronize_temporal_columns_sql(table_oid, era_name);

    RAISE DEBUG 'specialized_trigger_sql: %', generated.function_sql;
    RAISE DEBUG 'specialized_trigger_create_trigger_sql: %', generated.trigger_sql;

    CALL sql_saga.drop_synchronize_temporal_columns_trigger(table_oid, era_name);

    EXECUTE generated.function_sql;
    EXECUTE generated.trigger_sql;

    -- Update sql_saga.era with the trigger and function names
    UPDATE sql_saga.era AS e
    SET sync_temporal_trg_name = generated.trigger_name,
        sync_temporal_trg_function_name = generated.function_name
    WHERE (e.table_schema, e.table_name, e.era_name) = (v_table_schema, v_table_name, add_synchronize_temporal_columns_trigger.era_name);
END;
$add_synchronize_temporal_columns_trigger$;

/*
 * Whether a CHECK constraint definition (pg_get_constraintdef) is the boundary check add_era creates
 * for the given boundary columns: CHECK ((from < until)), or, for subtypes with infinity,
 * CHECK (((from < until) AND (from > '-infinity'::subtype))) (rendered per subtype, e.g.
 * '-infinity'::date, '-Infinity'::numeric, '-infinity'::timestamp with time zone).
 */
CREATE FUNCTION sql_saga.__internal_era_boundary_check_matches(definition text, valid_from_column_name name, valid_until_column_name name)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_era_boundary_check_matches$
    SELECT $1 OPERATOR(pg_catalog.=) pg_catalog.format('CHECK ((%I < %I))', $2, $3)
        OR (pg_catalog.starts_with($1, pg_catalog.format('CHECK (((%I < %I) AND (%I > ''-', $2, $3, $2))
            AND pg_catalog.substr($1, pg_catalog.length(pg_catalog.format('CHECK (((%I < %I) AND (%I > ''-', $2, $3, $2)) OPERATOR(pg_catalog.+) 1)
                OPERATOR(pg_catalog.~) '^[Ii]nfinity''::[a-z0-9 ]+\)\)\)$');
$__internal_era_boundary_check_matches$;

REVOKE ALL ON FUNCTION sql_saga.__internal_era_boundary_check_matches(text, name, name) FROM PUBLIC;

/*
 * Whether a regular foreign key's referenced-side trigger is exactly what add_regular_foreign_key
 * creates (see __internal_recreate_regular_fk_uk_triggers): a non-internal constraint trigger on the
 * referenced table FROM the referencing table, DEFERRABLE INITIALLY IMMEDIATE, row level, AFTER
 * UPDATE OF the key columns and bounds (update) or AFTER DELETE (delete), with the role's C
 * function, no WHEN qualifier, no transition tables, and the canonical arguments for the current
 * catalog. Only the firing mode (tgenabled) may differ. update_of_attnums: the expected UPDATE OF
 * columns of the update trigger (by attnum, so the caller can name them after a column rename).
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_uk_trigger_is_canonical(foreign_key_name name, trigger_oid oid, is_update boolean, update_of_attnums smallint[])
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_uk_trigger_is_canonical$
    SELECT coalesce((
        SELECT NOT t.tgisinternal
           AND t.tgfoid OPERATOR(pg_catalog.=) CASE WHEN $3 THEN 'sql_saga.uk_update_check_c()'::regprocedure
                                                   ELSE 'sql_saga.uk_delete_check_c()'::regprocedure END
           -- ROW (1) + AFTER + UPDATE (16) or DELETE (8)
           AND t.tgtype OPERATOR(pg_catalog.=) CASE WHEN $3 THEN 17 ELSE 9 END
           AND t.tgconstraint OPERATOR(pg_catalog.<>) 0
           AND t.tgconstrrelid OPERATOR(pg_catalog.=) pg_catalog.to_regclass(pg_catalog.format('%I.%I', fk.table_schema, fk.table_name))
           AND t.tgconstrindid OPERATOR(pg_catalog.=) 0
           AND t.tgdeferrable AND NOT t.tginitdeferred
           AND t.tgqual IS NULL
           AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL
           AND t.tgparentid OPERATOR(pg_catalog.=) 0
           AND t.tgargs OPERATOR(pg_catalog.=) sql_saga.__internal_regular_fk_trigger_args(fk.foreign_key_name)
           -- tgattr (int2vector) is zero-based: compare the elements, not the array bounds.
           AND ARRAY(SELECT pg_catalog.unnest(t.tgattr::pg_catalog.int2[]))::pg_catalog.int2[] OPERATOR(pg_catalog.=)
               CASE WHEN $3 THEN $4 ELSE '{}'::pg_catalog.int2[] END
        FROM pg_catalog.pg_trigger AS t
        WHERE t.oid = $2
          AND t.tgrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name))
          AND t.tgname = CASE WHEN $3 THEN fk.uk_update_trigger ELSE fk.uk_delete_trigger END
    ), false)
    FROM sql_saga.foreign_keys AS fk
    JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
    WHERE fk.foreign_key_name = $1 AND fk.type = 'regular_to_temporal';
$__internal_regular_fk_uk_trigger_is_canonical$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_uk_trigger_is_canonical(name, oid, boolean, smallint[]) FROM PUBLIC;

/*
 * Follow renames of the temporal columns of application-time eras (range, valid_from, valid_until,
 * valid_to) on the given relations (those touched by the ALTER TABLE). A catalogued column that no
 * longer exists is identified by attnum through an object that follows renames by itself:
 *   - the synchronization trigger: its UPDATE OF list is range, [valid_to,] valid_from, valid_until;
 *   - without one, the bounds CHECK (NOT isempty(range)) names the range column and the boundary
 *     CHECK (from < until ...) names the boundary columns.
 * Before anything changes, everything that will be regenerated must be exactly what sql_saga
 * generated for the columns before the rename, and must not be used by anything else; otherwise the
 * rename is refused, so nothing a user created or changed is overwritten or deleted:
 *   - the synchronization function: definition (body and properties) as generated, and no
 *     dependent other than its own trigger;
 *   - the synchronization trigger: definition as generated (only the firing mode may differ);
 *   - the referenced-side triggers of every regular foreign key referencing a unique key of the era
 *     (when a boundary column changes): see __internal_regular_fk_uk_trigger_is_canonical.
 * Then, in place: the era row follows; the synchronization function is replaced (CREATE OR REPLACE:
 * same OID, owner and privileges, its trigger keeps its OID and firing mode); the referenced-side
 * triggers are re-created with the same names and firing modes.
 * The system_time era is not followed here.
 */
CREATE FUNCTION sql_saga.__internal_era_column_rename_follow(table_oids oid[])
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_era_column_rename_follow$
#variable_conflict use_variable
DECLARE
    era_rec record;
    fk_rec record;
    sync_trigger record;
    sync_columns name[];
    sync_function oid;
    generated record;
    canonical_definition text;
    dependent record;
    new_range name;
    new_from name;
    new_until name;
    new_to name;
    boundaries_changed boolean;
    era_display text;
    uk_update_attnums smallint[];
BEGIN
    FOR era_rec IN
        SELECT e.*, c.oid AS table_oid
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name) = (n.nspname, c.relname)
        WHERE c.oid = ANY (table_oids)
          AND e.era_name <> 'system_time'
          AND EXISTS (
              SELECT FROM pg_catalog.unnest(ARRAY[e.range_column_name, e.valid_from_column_name,
                                                  e.valid_until_column_name, e.valid_to_column_name]) AS col(name)
              WHERE col.name IS NOT NULL
                AND NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                                WHERE a.attrelid = c.oid AND a.attname = col.name AND a.attnum > 0 AND NOT a.attisdropped))
        ORDER BY e.table_schema COLLATE "C", e.table_name COLLATE "C", e.era_name COLLATE "C"
    LOOP
        new_range := NULL;
        new_from := NULL;
        new_until := NULL;
        new_to := NULL;
        sync_function := NULL;
        era_display := sql_saga.__internal_display_name(era_rec.table_oid::regclass);

        SELECT t.oid, t.tgfoid INTO sync_trigger
        FROM pg_catalog.pg_trigger AS t
        WHERE t.tgrelid = era_rec.table_oid
          AND t.tgname = era_rec.sync_temporal_trg_name;
        IF FOUND THEN
            sync_function := pg_catalog.to_regprocedure(pg_catalog.format('sql_saga.%I()', era_rec.sync_temporal_trg_function_name));
            sync_columns := ARRAY(
                SELECT a.attname
                FROM pg_catalog.pg_trigger AS t
                CROSS JOIN LATERAL pg_catalog.unnest(t.tgattr::pg_catalog.int2[]) WITH ORDINALITY AS u(attnum, n)
                JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (era_rec.table_oid, u.attnum)
                WHERE t.oid = sync_trigger.oid
                ORDER BY u.n);
            IF era_rec.valid_to_column_name IS NULL AND pg_catalog.cardinality(sync_columns) = 3 THEN
                new_range := sync_columns[1];
                new_from := sync_columns[2];
                new_until := sync_columns[3];
            ELSIF era_rec.valid_to_column_name IS NOT NULL AND pg_catalog.cardinality(sync_columns) = 4 THEN
                new_range := sync_columns[1];
                new_to := sync_columns[2];
                new_from := sync_columns[3];
                new_until := sync_columns[4];
            ELSE
                RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": synchronization trigger "%" is not the canonical sql_saga trigger; restore it with sql_saga.add_synchronize_temporal_columns_trigger(table_oid regclass, era_name name) first',
                    era_rec.era_name, era_display, era_rec.sync_temporal_trg_name;
            END IF;
        ELSE
            SELECT a.attname INTO new_range
            FROM pg_catalog.pg_constraint AS c
            JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (c.conrelid, c.conkey[1])
            WHERE (c.conrelid, c.conname) = (era_rec.table_oid, era_rec.bounds_check_constraint)
              AND c.contype = 'c' AND pg_catalog.cardinality(c.conkey) = 1
              AND pg_catalog.pg_get_constraintdef(c.oid) = pg_catalog.format('CHECK ((NOT isempty(%I)))', a.attname);
            SELECT sa.attname, ea.attname INTO new_from, new_until
            FROM pg_catalog.pg_constraint AS c
            JOIN pg_catalog.pg_attribute AS sa ON sa.attrelid = c.conrelid AND sa.attnum = ANY (c.conkey)
            JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = c.conrelid AND ea.attnum = ANY (c.conkey)
            WHERE (c.conrelid, c.conname) = (era_rec.table_oid, era_rec.boundary_check_constraint)
              AND c.contype = 'c' AND sa.attnum <> ea.attnum
              AND sql_saga.__internal_era_boundary_check_matches(pg_catalog.pg_get_constraintdef(c.oid), sa.attname, ea.attname);
        END IF;

        /* Only a catalogued column that is gone follows, and only to a column of the era's types. */
        IF new_range IS NULL OR EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = era_rec.range_column_name AND a.attnum > 0 AND NOT a.attisdropped)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = new_range AND a.atttypid = era_rec.range_type) THEN
            new_range := era_rec.range_column_name;
        END IF;
        IF new_from IS NULL OR era_rec.valid_from_column_name IS NULL
           OR EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = era_rec.valid_from_column_name AND a.attnum > 0 AND NOT a.attisdropped)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = new_from AND a.atttypid = era_rec.range_subtype) THEN
            new_from := era_rec.valid_from_column_name;
        END IF;
        IF new_until IS NULL OR era_rec.valid_until_column_name IS NULL
           OR EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = era_rec.valid_until_column_name AND a.attnum > 0 AND NOT a.attisdropped)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = new_until AND a.atttypid = era_rec.range_subtype) THEN
            new_until := era_rec.valid_until_column_name;
        END IF;
        IF new_to IS NULL OR era_rec.valid_to_column_name IS NULL
           OR EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = era_rec.valid_to_column_name AND a.attnum > 0 AND NOT a.attisdropped)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = new_to AND a.atttypid = era_rec.range_subtype) THEN
            new_to := era_rec.valid_to_column_name;
        END IF;

        IF (new_range, new_from, new_until, new_to) IS NOT DISTINCT FROM
           (era_rec.range_column_name, era_rec.valid_from_column_name, era_rec.valid_until_column_name, era_rec.valid_to_column_name) THEN
            CONTINUE;
        END IF;
        boundaries_changed := (new_from, new_until) IS DISTINCT FROM (era_rec.valid_from_column_name, era_rec.valid_until_column_name);

        /*
         * Attribute before anything changes. The synchronization objects are compared with what
         * sql_saga generates from the catalog as it was before the rename (era row not yet updated);
         * their column references are by name, so the generated text uses the old names.
         */
        IF sync_trigger.oid IS NOT NULL THEN
            generated := sql_saga.__internal_synchronize_temporal_columns_sql(era_rec.table_oid::regclass, era_rec.era_name);

            IF sync_function IS NULL OR sync_trigger.tgfoid IS DISTINCT FROM sync_function
               OR generated.function_name IS DISTINCT FROM era_rec.sync_temporal_trg_function_name THEN
                RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": synchronization trigger "%" is not the canonical sql_saga trigger; restore it with sql_saga.add_synchronize_temporal_columns_trigger(table_oid regclass, era_name name) first',
                    era_rec.era_name, era_display, era_rec.sync_temporal_trg_name;
            END IF;

            FOR dependent IN
                SELECT t.tgname, t.tgrelid
                FROM pg_catalog.pg_trigger AS t
                WHERE t.tgfoid = sync_function AND t.oid <> sync_trigger.oid
                ORDER BY t.tgrelid, t.tgname COLLATE "C"
            LOOP
                RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": synchronization function %() is also used by trigger "%" on table "%"; drop that trigger first',
                    era_rec.era_name, era_display, pg_catalog.format('sql_saga.%I', era_rec.sync_temporal_trg_function_name), dependent.tgname,
                    sql_saga.__internal_display_name(dependent.tgrelid::regclass);
            END LOOP;
            IF EXISTS (
                SELECT FROM pg_catalog.pg_depend AS d
                WHERE d.refclassid = 'pg_catalog.pg_proc'::regclass AND d.refobjid = sync_function
                  AND d.deptype IN ('n', 'a')
                  AND NOT (d.classid = 'pg_catalog.pg_trigger'::regclass AND d.objid = sync_trigger.oid)) THEN
                RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": synchronization function %() has dependent objects other than its trigger; drop them first',
                    era_rec.era_name, era_display, pg_catalog.format('sql_saga.%I', era_rec.sync_temporal_trg_function_name);
            END IF;

            /*
             * The function: exactly the generated body (prosrc) and the properties CREATE FUNCTION
             * gives it there (plpgsql, RETURNS trigger, no arguments, VOLATILE, not STRICT, not
             * SECURITY DEFINER, not LEAKPROOF, PARALLEL UNSAFE, COST 100, no SET clauses).
             */
            canonical_definition := pg_catalog.substr(generated.function_sql,
                pg_catalog.strpos(generated.function_sql, '$template_sync_trigger$') + pg_catalog.length('$template_sync_trigger$'));
            canonical_definition := pg_catalog.substr(canonical_definition, 1,
                pg_catalog.strpos(canonical_definition, '$template_sync_trigger$') - 1);
            IF NOT EXISTS (
                SELECT FROM pg_catalog.pg_proc AS p
                WHERE p.oid = sync_function
                  AND p.prosrc = canonical_definition
                  AND p.prolang = (SELECT l.oid FROM pg_catalog.pg_language AS l WHERE l.lanname = 'plpgsql')
                  AND p.prorettype = 'pg_catalog.trigger'::regtype AND p.pronargs = 0 AND p.prokind = 'f'
                  AND p.provolatile = 'v' AND NOT p.proisstrict AND NOT p.prosecdef AND NOT p.proleakproof
                  AND p.proparallel = 'u' AND p.procost = 100 AND p.proconfig IS NULL AND p.probin IS NULL
                  AND p.prosqlbody IS NULL AND p.protrftypes IS NULL AND p.prosupport = 0) THEN
                RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": synchronization function %() is not the canonical sql_saga function; restore it with sql_saga.add_synchronize_temporal_columns_trigger(table_oid regclass, era_name name) first',
                    era_rec.era_name, era_display, pg_catalog.format('sql_saga.%I', era_rec.sync_temporal_trg_function_name);
            END IF;

            /*
             * The trigger: BEFORE INSERT OR UPDATE OF range, [valid_to,] valid_from, valid_until (by
             * attnum: the columns as they are named now, after the rename), FOR EACH ROW, the
             * function, no WHEN, no arguments, not a constraint trigger. Only the firing mode may differ.
             */
            IF generated.trigger_name IS DISTINCT FROM era_rec.sync_temporal_trg_name
               OR NOT EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE t.oid = sync_trigger.oid
                  -- ROW (1) + BEFORE (2) + INSERT (4) + UPDATE (16)
                  AND t.tgtype = 23 AND NOT t.tgisinternal AND t.tgconstraint = 0 AND t.tgconstrrelid = 0
                  AND t.tgqual IS NULL AND t.tgnargs = 0 AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL
                  AND t.tgparentid = 0
                  -- tgattr (int2vector) is zero-based: compare the elements, not the array bounds.
                  AND ARRAY(SELECT pg_catalog.unnest(t.tgattr::pg_catalog.int2[]))::pg_catalog.int2[] = ARRAY(
                      SELECT a.attnum
                      FROM pg_catalog.unnest(ARRAY[new_range, new_to, new_from, new_until])
                           WITH ORDINALITY AS u(column_name, n)
                      JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (era_rec.table_oid, u.column_name)
                      WHERE u.column_name IS NOT NULL
                      ORDER BY u.n)::pg_catalog.int2[]) THEN
                RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": synchronization trigger "%" is not the canonical sql_saga trigger; restore it with sql_saga.add_synchronize_temporal_columns_trigger(table_oid regclass, era_name name) first',
                    era_rec.era_name, era_display, era_rec.sync_temporal_trg_name;
            END IF;
        END IF;

        IF boundaries_changed THEN
            FOR fk_rec IN
                SELECT fk.foreign_key_name, fk.uk_update_trigger, fk.uk_delete_trigger, uk.column_names AS uk_column_names
                FROM sql_saga.foreign_keys AS fk
                JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                WHERE fk.type = 'regular_to_temporal'
                  AND (uk.table_schema, uk.table_name, uk.era_name) = (era_rec.table_schema, era_rec.table_name, era_rec.era_name)
                ORDER BY fk.foreign_key_name COLLATE "C"
            LOOP
                /* UPDATE OF key columns and bounds, by attnum: the bounds are named as they are now. */
                uk_update_attnums := ARRAY(
                    SELECT a.attnum
                    FROM pg_catalog.unnest(fk_rec.uk_column_names || new_from || new_until) WITH ORDINALITY AS u(column_name, n)
                    JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (era_rec.table_oid, u.column_name)
                    ORDER BY u.n);
                IF NOT coalesce(sql_saga.__internal_regular_fk_uk_trigger_is_canonical(fk_rec.foreign_key_name,
                           (SELECT t.oid FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = era_rec.table_oid AND t.tgname = fk_rec.uk_update_trigger), true, uk_update_attnums), false)
                   OR NOT coalesce(sql_saga.__internal_regular_fk_uk_trigger_is_canonical(fk_rec.foreign_key_name,
                           (SELECT t.oid FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = era_rec.table_oid AND t.tgname = fk_rec.uk_delete_trigger), false, NULL), false) THEN
                    RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": a referenced-side trigger of regular foreign key "%" is not the canonical sql_saga trigger; repair with drop_foreign_key and add_regular_foreign_key first',
                        era_rec.era_name, era_display, fk_rec.foreign_key_name;
                END IF;
            END LOOP;
        END IF;

        UPDATE sql_saga.era AS e
        SET range_column_name = new_range,
            valid_from_column_name = new_from,
            valid_until_column_name = new_until,
            valid_to_column_name = new_to
        WHERE (e.table_schema, e.table_name, e.era_name) = (era_rec.table_schema, era_rec.table_name, era_rec.era_name);

        /*
         * The synchronization function names the columns: replace it in place (same OID, owner and
         * privileges). Its trigger references the columns by attnum and keeps its OID and firing mode.
         */
        IF sync_trigger.oid IS NOT NULL THEN
            generated := sql_saga.__internal_synchronize_temporal_columns_sql(era_rec.table_oid::regclass, era_rec.era_name);
            EXECUTE generated.function_sql;
        END IF;

        /* The referenced-side triggers of regular foreign keys carry the bounds in their arguments. */
        IF boundaries_changed THEN
            FOR fk_rec IN
                SELECT fk.foreign_key_name
                FROM sql_saga.foreign_keys AS fk
                JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                WHERE fk.type = 'regular_to_temporal'
                  AND (uk.table_schema, uk.table_name, uk.era_name) = (era_rec.table_schema, era_rec.table_name, era_rec.era_name)
                ORDER BY fk.foreign_key_name COLLATE "C"
            LOOP
                PERFORM sql_saga.__internal_recreate_regular_fk_uk_triggers(fk_rec.foreign_key_name);
            END LOOP;
        END IF;
    END LOOP;
END;
$__internal_era_column_rename_follow$;

REVOKE ALL ON FUNCTION sql_saga.__internal_era_column_rename_follow(oid[]) FROM PUBLIC;

DO $upgrade_rename_following_era_columns$
DECLARE
    definition text := pg_catalog.pg_get_functiondef('sql_saga.rename_following()'::regprocedure);
    changed text;
BEGIN
    IF (pg_catalog.length(definition) - pg_catalog.length(pg_catalog.replace(definition, $old_era_columns$    /*
     * Follow renames for the era's temporal columns. This logic is broken into
     * three parts to handle each type of temporal column independently.
     *
     * 1. The primary `range_column_name` is identified by its data type via
     *    the synchronization trigger's metadata.
     * 2. The optional `valid_from_column_name` and `valid_until_column_name`
     *    are identified by inspecting the bounds check constraint.
     * 3. The optional `valid_to_column_name` is identified as the "other"
     *    column in the synchronization trigger's metadata.
     */

    -- Follow rename of the primary range_column_name for synchronized eras.
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET range_column_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            a.attname, e.table_schema, e.table_name, e.era_name
        )
        FROM sql_saga.era e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_trigger t ON t.tgrelid = pc.oid
        JOIN LATERAL (
            -- Find the single column attached to the trigger that has the era's range type.
            SELECT att.attname
            FROM unnest(t.tgattr) AS tg(attnum)
            JOIN pg_attribute att ON (att.attrelid, att.attnum) = (pc.oid, tg.attnum)
            WHERE att.atttypid = e.range_type
            LIMIT 1
        ) AS a ON true
        WHERE e.trigger_applies_defaults -- This flag indicates a synchronization trigger exists.
          AND t.tgfoid = 'sql_saga.synchronize_temporal_columns'::regproc
          AND e.range_column_name <> a.attname
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames for valid_from and valid_until columns using the boundary check constraint.
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET valid_from_column_name = %L, valid_until_column_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            sa.attname, ea.attname,
            e.table_schema, e.table_name, e.era_name
        )
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (pc.oid, e.boundary_check_constraint)
        JOIN pg_catalog.pg_attribute AS sa ON sa.attrelid = pc.oid
        JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = pc.oid
        WHERE e.boundary_check_constraint IS NOT NULL
          AND (e.valid_from_column_name, e.valid_until_column_name) <> (sa.attname, ea.attname)
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', sa.attname, ea.attname)
    LOOP
        RAISE DEBUG 'rename_following: Executing boundary column rename: %', sql;
        EXECUTE sql;
    END LOOP;

    -- Follow renames for the optional valid_to_column_name for synchronized eras.
    FOR sql IN
        WITH new_valid_to AS (
            SELECT
                e.table_schema, e.table_name, e.era_name,
                (
                    -- The valid_to column is the one left in the sync trigger's metadata after
                    -- excluding all other known temporal columns.
                    SELECT a.attname
                    FROM unnest(t.tgattr) AS att(num)
                    JOIN pg_attribute a ON (a.attrelid, a.attnum) = (t.tgrelid, att.num)
                    WHERE a.attname IS DISTINCT FROM e.range_column_name
                      AND a.attname IS DISTINCT FROM e.valid_from_column_name
                      AND a.attname IS DISTINCT FROM e.valid_until_column_name
                    LIMIT 1 -- There should be at most one left.
                ) AS candidate_name
            FROM sql_saga.era e
            JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
            JOIN pg_trigger t ON t.tgrelid = pc.oid
            WHERE e.trigger_applies_defaults -- Sync trigger exists.
              AND e.valid_to_column_name IS NOT NULL -- Only for eras that have a valid_to column.
              AND t.tgfoid = 'sql_saga.synchronize_temporal_columns'::regproc
        )
        SELECT format('UPDATE sql_saga.era SET valid_to_column_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            n.candidate_name,
            e.table_schema, e.table_name, e.era_name
        )
        FROM sql_saga.era e
        JOIN new_valid_to n ON (e.table_schema, e.table_name, e.era_name) = (n.table_schema, n.table_name, n.era_name)
        WHERE e.valid_to_column_name IS DISTINCT FROM n.candidate_name
          AND n.candidate_name IS NOT NULL
    LOOP
        EXECUTE sql;
    END LOOP;

$old_era_columns$, ''))) <> pg_catalog.length($old_era_columns_len$    /*
     * Follow renames for the era's temporal columns. This logic is broken into
     * three parts to handle each type of temporal column independently.
     *
     * 1. The primary `range_column_name` is identified by its data type via
     *    the synchronization trigger's metadata.
     * 2. The optional `valid_from_column_name` and `valid_until_column_name`
     *    are identified by inspecting the bounds check constraint.
     * 3. The optional `valid_to_column_name` is identified as the "other"
     *    column in the synchronization trigger's metadata.
     */

    -- Follow rename of the primary range_column_name for synchronized eras.
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET range_column_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            a.attname, e.table_schema, e.table_name, e.era_name
        )
        FROM sql_saga.era e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_trigger t ON t.tgrelid = pc.oid
        JOIN LATERAL (
            -- Find the single column attached to the trigger that has the era's range type.
            SELECT att.attname
            FROM unnest(t.tgattr) AS tg(attnum)
            JOIN pg_attribute att ON (att.attrelid, att.attnum) = (pc.oid, tg.attnum)
            WHERE att.atttypid = e.range_type
            LIMIT 1
        ) AS a ON true
        WHERE e.trigger_applies_defaults -- This flag indicates a synchronization trigger exists.
          AND t.tgfoid = 'sql_saga.synchronize_temporal_columns'::regproc
          AND e.range_column_name <> a.attname
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames for valid_from and valid_until columns using the boundary check constraint.
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET valid_from_column_name = %L, valid_until_column_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            sa.attname, ea.attname,
            e.table_schema, e.table_name, e.era_name
        )
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (pc.oid, e.boundary_check_constraint)
        JOIN pg_catalog.pg_attribute AS sa ON sa.attrelid = pc.oid
        JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = pc.oid
        WHERE e.boundary_check_constraint IS NOT NULL
          AND (e.valid_from_column_name, e.valid_until_column_name) <> (sa.attname, ea.attname)
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', sa.attname, ea.attname)
    LOOP
        RAISE DEBUG 'rename_following: Executing boundary column rename: %', sql;
        EXECUTE sql;
    END LOOP;

    -- Follow renames for the optional valid_to_column_name for synchronized eras.
    FOR sql IN
        WITH new_valid_to AS (
            SELECT
                e.table_schema, e.table_name, e.era_name,
                (
                    -- The valid_to column is the one left in the sync trigger's metadata after
                    -- excluding all other known temporal columns.
                    SELECT a.attname
                    FROM unnest(t.tgattr) AS att(num)
                    JOIN pg_attribute a ON (a.attrelid, a.attnum) = (t.tgrelid, att.num)
                    WHERE a.attname IS DISTINCT FROM e.range_column_name
                      AND a.attname IS DISTINCT FROM e.valid_from_column_name
                      AND a.attname IS DISTINCT FROM e.valid_until_column_name
                    LIMIT 1 -- There should be at most one left.
                ) AS candidate_name
            FROM sql_saga.era e
            JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
            JOIN pg_trigger t ON t.tgrelid = pc.oid
            WHERE e.trigger_applies_defaults -- Sync trigger exists.
              AND e.valid_to_column_name IS NOT NULL -- Only for eras that have a valid_to column.
              AND t.tgfoid = 'sql_saga.synchronize_temporal_columns'::regproc
        )
        SELECT format('UPDATE sql_saga.era SET valid_to_column_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            n.candidate_name,
            e.table_schema, e.table_name, e.era_name
        )
        FROM sql_saga.era e
        JOIN new_valid_to n ON (e.table_schema, e.table_name, e.era_name) = (n.table_schema, n.table_name, n.era_name)
        WHERE e.valid_to_column_name IS DISTINCT FROM n.candidate_name
          AND n.candidate_name IS NOT NULL
    LOOP
        EXECUTE sql;
    END LOOP;

$old_era_columns_len$)
       OR pg_catalog.strpos(definition, 'sql_saga.__internal_era_column_rename_follow(') > 0 THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade rename_following era column rename';
    END IF;
    changed := pg_catalog.replace(definition, $old_era_columns$    /*
     * Follow renames for the era's temporal columns. This logic is broken into
     * three parts to handle each type of temporal column independently.
     *
     * 1. The primary `range_column_name` is identified by its data type via
     *    the synchronization trigger's metadata.
     * 2. The optional `valid_from_column_name` and `valid_until_column_name`
     *    are identified by inspecting the bounds check constraint.
     * 3. The optional `valid_to_column_name` is identified as the "other"
     *    column in the synchronization trigger's metadata.
     */

    -- Follow rename of the primary range_column_name for synchronized eras.
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET range_column_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            a.attname, e.table_schema, e.table_name, e.era_name
        )
        FROM sql_saga.era e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_trigger t ON t.tgrelid = pc.oid
        JOIN LATERAL (
            -- Find the single column attached to the trigger that has the era's range type.
            SELECT att.attname
            FROM unnest(t.tgattr) AS tg(attnum)
            JOIN pg_attribute att ON (att.attrelid, att.attnum) = (pc.oid, tg.attnum)
            WHERE att.atttypid = e.range_type
            LIMIT 1
        ) AS a ON true
        WHERE e.trigger_applies_defaults -- This flag indicates a synchronization trigger exists.
          AND t.tgfoid = 'sql_saga.synchronize_temporal_columns'::regproc
          AND e.range_column_name <> a.attname
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames for valid_from and valid_until columns using the boundary check constraint.
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET valid_from_column_name = %L, valid_until_column_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            sa.attname, ea.attname,
            e.table_schema, e.table_name, e.era_name
        )
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (pc.oid, e.boundary_check_constraint)
        JOIN pg_catalog.pg_attribute AS sa ON sa.attrelid = pc.oid
        JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = pc.oid
        WHERE e.boundary_check_constraint IS NOT NULL
          AND (e.valid_from_column_name, e.valid_until_column_name) <> (sa.attname, ea.attname)
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', sa.attname, ea.attname)
    LOOP
        RAISE DEBUG 'rename_following: Executing boundary column rename: %', sql;
        EXECUTE sql;
    END LOOP;

    -- Follow renames for the optional valid_to_column_name for synchronized eras.
    FOR sql IN
        WITH new_valid_to AS (
            SELECT
                e.table_schema, e.table_name, e.era_name,
                (
                    -- The valid_to column is the one left in the sync trigger's metadata after
                    -- excluding all other known temporal columns.
                    SELECT a.attname
                    FROM unnest(t.tgattr) AS att(num)
                    JOIN pg_attribute a ON (a.attrelid, a.attnum) = (t.tgrelid, att.num)
                    WHERE a.attname IS DISTINCT FROM e.range_column_name
                      AND a.attname IS DISTINCT FROM e.valid_from_column_name
                      AND a.attname IS DISTINCT FROM e.valid_until_column_name
                    LIMIT 1 -- There should be at most one left.
                ) AS candidate_name
            FROM sql_saga.era e
            JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
            JOIN pg_trigger t ON t.tgrelid = pc.oid
            WHERE e.trigger_applies_defaults -- Sync trigger exists.
              AND e.valid_to_column_name IS NOT NULL -- Only for eras that have a valid_to column.
              AND t.tgfoid = 'sql_saga.synchronize_temporal_columns'::regproc
        )
        SELECT format('UPDATE sql_saga.era SET valid_to_column_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            n.candidate_name,
            e.table_schema, e.table_name, e.era_name
        )
        FROM sql_saga.era e
        JOIN new_valid_to n ON (e.table_schema, e.table_name, e.era_name) = (n.table_schema, n.table_name, n.era_name)
        WHERE e.valid_to_column_name IS DISTINCT FROM n.candidate_name
          AND n.candidate_name IS NOT NULL
    LOOP
        EXECUTE sql;
    END LOOP;

$old_era_columns$, $new_era_columns$    /*
     * Follow renames of the era's temporal columns (range, valid_from, valid_until, valid_to) on the
     * relations touched by this ALTER TABLE, and regenerate what names them (the synchronization
     * function, the referenced-side triggers of regular foreign keys). Runs before the unique keys
     * are followed, which exclude the era's range column by name.
     */
    IF is_alter_table THEN
        PERFORM sql_saga.__internal_era_column_rename_follow(ARRAY(
            SELECT DISTINCT c.objid
            FROM pg_catalog.pg_event_trigger_ddl_commands() AS c
            WHERE c.command_tag = 'ALTER TABLE' AND c.classid = 'pg_catalog.pg_class'::regclass));
    END IF;

$new_era_columns$);
    IF (pg_catalog.length(changed) - pg_catalog.length(pg_catalog.replace(changed, $old_boundary$          AND (pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', e.valid_from_column_name, e.valid_until_column_name)
               OR pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I) AND (%I > ''-infinity''::integer))', e.valid_from_column_name, e.valid_until_column_name, e.valid_from_column_name))
$old_boundary$, ''))) <> pg_catalog.length($old_boundary_len$          AND (pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', e.valid_from_column_name, e.valid_until_column_name)
               OR pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I) AND (%I > ''-infinity''::integer))', e.valid_from_column_name, e.valid_until_column_name, e.valid_from_column_name))
$old_boundary_len$)
       OR pg_catalog.strpos(changed, 'sql_saga.__internal_era_boundary_check_matches(') > 0 THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade rename_following era boundary check';
    END IF;
    changed := pg_catalog.replace(changed, $old_boundary$          AND (pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', e.valid_from_column_name, e.valid_until_column_name)
               OR pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I) AND (%I > ''-infinity''::integer))', e.valid_from_column_name, e.valid_until_column_name, e.valid_from_column_name))
$old_boundary$, $new_boundary$          AND sql_saga.__internal_era_boundary_check_matches(pg_catalog.pg_get_constraintdef(c.oid), e.valid_from_column_name, e.valid_until_column_name)
$new_boundary$);
    IF pg_catalog.strpos(changed, 'PERFORM sql_saga.__internal_era_column_rename_follow(ARRAY(') = 0
       OR pg_catalog.strpos(changed, 'AND sql_saga.__internal_era_boundary_check_matches(pg_catalog.pg_get_constraintdef(c.oid), e.valid_from_column_name, e.valid_until_column_name)') = 0
       OR pg_catalog.strpos(changed, 'sql_saga.synchronize_temporal_columns''::regproc') > 0
       OR pg_catalog.strpos(changed, '''-infinity''''::integer') > 0 THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade rename_following era column rename';
    END IF;
    EXECUTE changed;
END;
$upgrade_rename_following_era_columns$;
/* ITEM 5 (era column renames): END. */

/* SPLIT RESTORE (parallel pg_restore without deadlocks): BEGIN. See the README, "Backup and Restore". */

/*
 * Is this statement run by a parallel pg_restore (pg_restore -j N) that the caller has declared with
 * PGOPTIONS='-c sql_saga.parallel_restore=on' (README, "Backup and Restore")?
 *
 * Then health_checks defers the synchronization of history policies: each sync reads and alters both
 * tables of a family, while parallel workers already hold exclusive locks on one table each, so two
 * workers restoring the two tables of one family would deadlock. The documented final command,
 * sql_saga.sync_all_history_policies(), runs the deferred synchronization once the restore is done.
 * Recognized only in a pg_restore session (the settings pg_dump and pg_restore always set, and
 * application_name 'pg_restore'), and only with the explicit setting, which is meant for the -j
 * invocation alone. Serial and --single-transaction restores keep synchronizing as they go.
 * Not a security boundary: deferring only delays the copy of policies the restoring owner could
 * change anyway, and guarded history reads fail closed until the sync has run.
 * Called only by the SECURITY DEFINER event triggers.
 */
CREATE FUNCTION sql_saga.__internal_is_parallel_restore()
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $__internal_is_parallel_restore$
    SELECT current_setting('check_function_bodies') = 'off'
       AND current_setting('row_security') = 'off'
       AND current_setting('application_name') = 'pg_restore'
       -- '' (not NULL) once the session has seen the placeholder, so coalesce both
       AND coalesce(current_setting('sql_saga.parallel_restore', true), '') = 'on';
$__internal_is_parallel_restore$;

REVOKE EXECUTE ON FUNCTION sql_saga.__internal_is_parallel_restore() FROM PUBLIC;

/*
 * pg_get_constraintdef of a constraint of one of the given relations, NULL for any other constraint
 * (so it matches no expected definition). rename_following compares definitions only for the
 * relations its DDL command touched: deparsing a CHECK constraint locks the constraint's table, which
 * a concurrent session may hold exclusively (see rename_following). The CASE, unlike a WHERE
 * condition next to the deparse, guarantees that the definition of another relation is never read,
 * whatever order the planner evaluates the conditions in.
 */
CREATE FUNCTION sql_saga.__internal_constraintdef_if_touched(constraint_oid oid, table_oid oid, touched_relation_oids oid[])
RETURNS text
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $__internal_constraintdef_if_touched$
    SELECT CASE WHEN table_oid = ANY (touched_relation_oids) THEN pg_catalog.pg_get_constraintdef(constraint_oid) END;
$__internal_constraintdef_if_touched$;

REVOKE ALL ON FUNCTION sql_saga.__internal_constraintdef_if_touched(oid, oid, oid[]) FROM PUBLIC;

-- The final command of a parallel restore (README, "Backup and Restore"; see
-- sql_saga.__internal_is_parallel_restore), and the repair after policy DDL with event triggers off.
-- SECURITY INVOKER like sync_history_policies, with the same ownership rule. It refuses before any
-- change when the caller does not own every system-versioned table, rather than skipping those: a
-- restore that leaves some histories unsynchronized must fail visibly, not look finished.
CREATE FUNCTION sql_saga.sync_all_history_policies()
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $sync_all_history_policies$
DECLARE
    table_oid regclass;
BEGIN
    SELECT c.oid::regclass INTO table_oid
    FROM sql_saga.system_versioning AS sv
    JOIN pg_catalog.pg_class AS c ON c.oid = pg_catalog.format('%I.%I', sv.table_schema, sv.table_name)::regclass
    WHERE NOT pg_catalog.pg_has_role(c.relowner, 'USAGE')
    ORDER BY sv.table_schema, sv.table_name
    LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'must be owner of table %', table_oid USING ERRCODE = 'insufficient_privilege',
            HINT = 'Run sql_saga.sync_all_history_policies() as a role that owns every system-versioned table (for example the role that ran the restore), or sql_saga.sync_history_policies(table) per table as its owner.';
    END IF;
    FOR table_oid IN
        SELECT pg_catalog.format('%I.%I', sv.table_schema, sv.table_name)::regclass
        FROM sql_saga.system_versioning AS sv
        ORDER BY sv.table_schema, sv.table_name
    LOOP
        PERFORM sql_saga.__internal_sync_history_policies(table_oid);
    END LOOP;
END;
$sync_all_history_policies$;
COMMENT ON FUNCTION sql_saga.sync_all_history_policies() IS
'Runs sync_history_policies for every system-versioned table, in name order. The final command after a parallel pg_restore with sql_saga.parallel_restore = on, which defers that synchronization. Refuses before any change unless the caller owns every system-versioned table.';

/*
 * rename_following deparses constraint definitions only for the relations its DDL command touched.
 * Exact replacements in the function as upgraded above (ITEM 5); each released fragment must occur
 * exactly once and no new fragment may be present yet, otherwise the update fails closed.
 */
DO $upgrade_rename_following_touched_relations$
DECLARE
    definition text := pg_catalog.pg_get_functiondef('sql_saga.rename_following()'::regprocedure);
    replacements text[] := ARRAY[
        ARRAY[$old1$    v_altered_table_oids oid[];
$old1$, $new1$    v_altered_table_oids oid[];
    v_touched_relation_oids oid[];
$new1$],
        ARRAY[$old2$        RETURN;
    END IF;
    /*
     * Anything that is stored by reg* type$old2$, $new2$        RETURN;
    END IF;

    /*
     * The relations this command touched: tables, views and indexes it names directly, the tables of
     * the constraints, triggers, policies and indexes it names, and their inheritance descendants
     * (PostgreSQL propagates renames to them). Built once from the catalog only.
     *
     * The constraint renames below are found by comparing constraint definitions
     * (pg_get_constraintdef), and deparsing a CHECK constraint takes a lock on its table. They only
     * deparse constraints of these relations: a rename can only happen on a relation the command
     * touched, and a concurrent session (such as another pg_restore -j worker) may hold an exclusive
     * lock on any other table while it waits for one of ours, which would be a deadlock.
     */
    v_touched_relation_oids := ARRAY(
        WITH RECURSIVE touched(relation_oid) AS (
            SELECT CASE ev.classid
                       WHEN 'pg_catalog.pg_class'::regclass THEN
                           coalesce((SELECT i.indrelid FROM pg_catalog.pg_index AS i WHERE i.indexrelid = ev.objid), ev.objid)
                       WHEN 'pg_catalog.pg_constraint'::regclass THEN
                           (SELECT c.conrelid FROM pg_catalog.pg_constraint AS c WHERE c.oid = ev.objid)
                       WHEN 'pg_catalog.pg_trigger'::regclass THEN
                           (SELECT t.tgrelid FROM pg_catalog.pg_trigger AS t WHERE t.oid = ev.objid)
                       WHEN 'pg_catalog.pg_policy'::regclass THEN
                           (SELECT p.polrelid FROM pg_catalog.pg_policy AS p WHERE p.oid = ev.objid)
                   END
            FROM pg_catalog.pg_event_trigger_ddl_commands() AS ev
            UNION
            SELECT i.inhrelid
            FROM touched AS t
            JOIN pg_catalog.pg_inherits AS i ON i.inhparent = t.relation_oid
        )
        SELECT t.relation_oid FROM touched AS t WHERE t.relation_oid IS NOT NULL);

    /*
     * Anything that is stored by reg* type$new2$],
        ARRAY[$old3$          AND e.bounds_check_constraint <> c.conname
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((NOT isempty(%I)))', e.range_column_name)
$old3$, $new3$          AND e.bounds_check_constraint <> c.conname
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = format('CHECK ((NOT isempty(%I)))', e.range_column_name)
$new3$],
        ARRAY[$old4$          AND e.valid_until_column_name IS NOT NULL
          AND sql_saga.__internal_era_boundary_check_matches(pg_catalog.pg_get_constraintdef(c.oid), e.valid_from_column_name, e.valid_until_column_name)
$old4$, $new4$          AND e.valid_until_column_name IS NOT NULL
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_era_boundary_check_matches(sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids), e.valid_from_column_name, e.valid_until_column_name)
$new4$],
        ARRAY[$old5$        WHERE ste.infinity_check_constraint <> c.conname
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((upper(%I) = ''infinity''::timestamp with time zone))', e.range_column_name)
$old5$, $new5$        WHERE ste.infinity_check_constraint <> c.conname
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = format('CHECK ((upper(%I) = ''infinity''::timestamp with time zone))', e.range_column_name)
$new5$],
        ARRAY[$old6$          AND NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.unique_constraint))
$old6$, $new6$          AND NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.unique_constraint))
          AND pc.oid = ANY (v_touched_relation_oids)
$new6$],
        ARRAY[$old7$            -- Match traditional constraint format
            pg_get_constraintdef(c.oid) = (
$old7$, $new7$            -- Match traditional constraint format
            sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = (
$new7$],
        ARRAY[$old8$            -- Match NULLS NOT DISTINCT constraint format
            pg_get_constraintdef(c.oid) = (
$old8$, $new8$            -- Match NULLS NOT DISTINCT constraint format
            sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = (
$new8$],
        ARRAY[$old9$          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.exclude_constraint))
          AND pg_get_constraintdef(c.oid) = (
$old9$, $new9$          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.exclude_constraint))
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = (
$new9$],
        ARRAY[$old10$        AND NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.check_constraint))
        AND pg_get_constraintdef(c.oid) = format('CHECK (((%s) = 1))', format('num_nonnulls(%s)', (SELECT string_agg(format('%I', col), ', ') FROM unnest(uk.mutually_exclusive_columns) as col)))
$old10$, $new10$        AND NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.check_constraint))
        AND pc.oid = ANY (v_touched_relation_oids)
        AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = format('CHECK (((%s) = 1))', format('num_nonnulls(%s)', (SELECT string_agg(format('%I', col), ', ') FROM unnest(uk.mutually_exclusive_columns) as col)))
$new10$]
    ];
    i integer;
BEGIN
    IF pg_catalog.strpos(definition, 'v_touched_relation_oids') > 0
       OR pg_catalog.strpos(definition, '__internal_constraintdef_if_touched') > 0 THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade rename_following touched relations: already present';
    END IF;
    FOR i IN 1 .. pg_catalog.array_length(replacements, 1) LOOP
        IF (pg_catalog.length(definition) - pg_catalog.length(pg_catalog.replace(definition, replacements[i][1], '')))
           <> pg_catalog.length(replacements[i][1]) THEN
            RAISE EXCEPTION 'sql_saga: cannot upgrade rename_following touched relations: fragment % not found exactly once', i;
        END IF;
        definition := pg_catalog.replace(definition, replacements[i][1], replacements[i][2]);
    END LOOP;
    IF definition ~ 'pg_get_constraintdef\(c\.oid\)' THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade rename_following touched relations: a definition read is left unscoped';
    END IF;
    EXECUTE definition;
END;
$upgrade_rename_following_touched_relations$;
/* SPLIT RESTORE: END. */

/*
 * add_regular_foreign_key: find an existing (shared) helper by its parsed signature, so a helper whose
 * schema or name needs quoting is reused instead of re-created. Exact replacement of the single released
 * 1.1 lookup; fails closed when the released fragment is absent or the new one is already present.
 */
DO $upgrade_add_regular_foreign_key$
DECLARE
    definition text := pg_catalog.pg_get_functiondef(
        'sql_saga.add_regular_foreign_key(regclass, name[], name, sql_saga.fk_match_types, sql_saga.fk_actions, sql_saga.fk_actions, name, name, text, name, name, boolean)'::regprocedure);
    old_lookup text := $old_lookup$        -- Check if a function with this signature already exists.
        SELECT p.oid INTO existing_proc
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid
        WHERE p.proname = split_part(fk_helper_function, '.', 2)
          AND n.nspname = split_part(fk_helper_function, '.', 1)
          AND pg_catalog.pg_get_function_identity_arguments(p.oid) = fk_column_signatures;
$old_lookup$;
    new_lookup text := $new_lookup$        -- Check if a function with this signature already exists. The signature is parsed as SQL, so a
        -- schema or function name that needs quoting (format %I above) resolves too.
        existing_proc := pg_catalog.to_regprocedure(helper_signature);
$new_lookup$;
    changed text;
BEGIN
    IF (pg_catalog.length(definition) - pg_catalog.length(pg_catalog.replace(definition, old_lookup, '')))
           OPERATOR(pg_catalog.<>) pg_catalog.length(old_lookup)
       OR pg_catalog.strpos(definition, new_lookup) > 0 THEN
        RAISE EXCEPTION 'sql_saga: cannot upgrade add_regular_foreign_key shared helper lookup';
    END IF;
    changed := pg_catalog.replace(definition, old_lookup, new_lookup);
    EXECUTE changed;
END;
$upgrade_add_regular_foreign_key$;

CREATE OR REPLACE FUNCTION sql_saga.__internal_refuse_managed_relation_identity_change()
RETURNS event_trigger
AS 'sql_saga', 'refuse_managed_relation_identity_change'
LANGUAGE c;

REVOKE ALL ON FUNCTION sql_saga.__internal_refuse_managed_relation_identity_change() FROM PUBLIC;

CREATE FUNCTION sql_saga.__internal_refuse_managed_schema_rename(schema_name text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, public
AS $__internal_refuse_managed_schema_rename$
#variable_conflict use_variable
DECLARE
    managed record;
    object_display_name text;
    base_display_name text;
BEGIN
    SELECT candidate.managed_class, candidate.object_schema, candidate.object_name,
           candidate.base_schema, candidate.base_table, candidate.era_name, candidate.detail
    INTO managed
    FROM (
        SELECT 1 AS priority, 'system_versioning_base' AS managed_class,
               sv.table_schema AS object_schema, sv.table_name AS object_name,
               sv.table_schema AS base_schema, sv.table_name AS base_table,
               sv.era_name, NULL::text AS detail
        FROM sql_saga.system_versioning AS sv
        WHERE sv.table_schema = schema_name

        UNION ALL

        SELECT 2, 'era', e.table_schema, e.table_name,
               e.table_schema, e.table_name, e.era_name, NULL::text
        FROM sql_saga.era AS e
        WHERE e.table_schema = schema_name

        UNION ALL

        SELECT 3, 'foreign_key', fk.table_schema, fk.table_name,
               fk.table_schema, fk.table_name, fk.fk_era_name, fk.foreign_key_name::text
        FROM sql_saga.foreign_keys AS fk
        WHERE fk.table_schema = schema_name

        UNION ALL

        SELECT 4, 'history_table', sv.history_schema_name, sv.history_table_name,
               sv.table_schema, sv.table_name, sv.era_name, NULL::text
        FROM sql_saga.system_versioning AS sv
        WHERE sv.history_schema_name = schema_name

        UNION ALL

        SELECT 5, 'system_versioning_view', sv.view_schema_name, sv.view_table_name,
               sv.table_schema, sv.table_name, sv.era_name, NULL::text
        FROM sql_saga.system_versioning AS sv
        WHERE sv.view_schema_name = schema_name

        UNION ALL

        SELECT 5, 'system_versioning_view', sv.history_schema_name,
               sql_saga.__internal_history_gated_name(sv.history_table_name),
               sv.table_schema, sv.table_name, sv.era_name, NULL::text
        FROM sql_saga.system_versioning AS sv
        WHERE sv.history_schema_name = schema_name

        UNION ALL

        SELECT 6, 'system_versioning_function', n.nspname, p.proname,
               sv.table_schema, sv.table_name, sv.era_name, NULL::text
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL unnest(ARRAY[
            sv.func_as_of,
            sv.func_between,
            sv.func_between_symmetric,
            sv.func_from_to
        ]) AS f (signature)
        JOIN pg_catalog.pg_proc AS p ON p.oid = pg_catalog.to_regprocedure(f.signature)
        JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
        WHERE n.nspname = schema_name

        UNION ALL

        SELECT CASE uv.view_type WHEN 'current' THEN 7 ELSE 8 END,
               uv.view_type::text, uv.view_schema, uv.view_name,
               uv.table_schema, uv.table_name, uv.era_name, NULL::text
        FROM sql_saga.updatable_view AS uv
        WHERE uv.view_schema = schema_name
    ) AS candidate
    ORDER BY candidate.object_schema::text COLLATE "C",
             candidate.object_name::text COLLATE "C", candidate.priority,
             candidate.era_name::text COLLATE "C", candidate.detail COLLATE "C"
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    object_display_name := sql_saga.__internal_display_name(
        pg_catalog.format('%I.%I', managed.object_schema, managed.object_name));
    base_display_name := sql_saga.__internal_display_name(
        pg_catalog.format('%I.%I', managed.base_schema, managed.base_table));

    IF managed.managed_class = 'system_versioning_base' THEN
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed system-versioned table "%"; drop system versioning first with sql_saga.drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior, cleanup boolean)', schema_name, object_display_name
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'era' THEN
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed table "%" with era "%"; drop dependent keys and foreign keys, then drop the era first with sql_saga.drop_era(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior, cleanup boolean)', schema_name, object_display_name, managed.era_name
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'foreign_key' THEN
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed table "%" with foreign key "%"; drop it first with sql_saga.drop_foreign_key_by_name(table_oid regclass, key_name name, drop_index boolean)', schema_name, object_display_name, managed.detail
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'history_table' THEN
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed history table "%" for table "%"; drop system versioning first with sql_saga.drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior, cleanup boolean)', schema_name, object_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class IN ('system_versioning_view', 'system_versioning_function') THEN
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed % "%" for table "%"; drop system versioning first with sql_saga.drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior, cleanup boolean)', schema_name,
            CASE managed.managed_class WHEN 'system_versioning_view' THEN 'view' ELSE 'query function' END,
            object_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'current' THEN
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed view "%" for table "%"; drop it with sql_saga.drop_current_view(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior) before renaming the schema', schema_name, object_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    ELSE
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed view "%" for table "%"; drop it with sql_saga.drop_for_portion_of_view(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior, cleanup boolean) before renaming the schema', schema_name, object_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    END IF;
END;
$__internal_refuse_managed_schema_rename$;

REVOKE ALL ON FUNCTION sql_saga.__internal_refuse_managed_schema_rename(text) FROM PUBLIC;

/*
 * The supported way to move or rename a system-versioned table together with its history
 * (README, "Moving a system-versioned table"), named by the identity-change refusals.
 */
CREATE FUNCTION sql_saga.__internal_system_versioning_move_advice(
    base_schema name, base_name name, history_schema name, history_name name)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog
AS $__internal_system_versioning_move_advice$
    SELECT pg_catalog.format(
        'run sql_saga.drop_system_versioning(%1$L, cleanup => false) and sql_saga.drop_system_time_era(%1$L, cleanup => false), '
        'move or rename table "%2$s" and history table "%3$s" together, '
        'then run sql_saga.add_system_versioning(table_oid, history_table_name => name, history_table_must_exist => true)',
        pg_catalog.format('%I.%I', base_schema, base_name),
        pg_catalog.array_to_string(pg_catalog.parse_ident(pg_catalog.format('%I.%I', base_schema, base_name)), '.'),
        pg_catalog.array_to_string(pg_catalog.parse_ident(pg_catalog.format('%I.%I', history_schema, history_name)), '.'));
$__internal_system_versioning_move_advice$;

REVOKE ALL ON FUNCTION sql_saga.__internal_system_versioning_move_advice(name, name, name, name) FROM PUBLIC;

CREATE FUNCTION sql_saga.__internal_refuse_managed_relation_identity_change(relation_oid oid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, public
AS $__internal_refuse_managed_relation_identity_change$
#variable_conflict use_variable
DECLARE
    relation_schema name;
    relation_name name;
    relation_display_name text;
    base_display_name text;
    history_schema name;
    history_name name;
    managed record;
BEGIN
    SELECT n.nspname, c.relname,
           sql_saga.__internal_display_name(pg_catalog.format('%I.%I', n.nspname, c.relname))
    INTO relation_schema, relation_name, relation_display_name
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = relation_oid;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT candidate.managed_class, candidate.base_schema, candidate.base_table,
           candidate.era_name, candidate.detail
    INTO managed
    FROM (
        SELECT 1 AS priority, 'system_versioning_base' AS managed_class,
               sv.table_schema AS base_schema, sv.table_name AS base_table,
               sv.era_name, NULL::name AS detail
        FROM sql_saga.system_versioning AS sv
        WHERE (sv.table_schema, sv.table_name) = (relation_schema, relation_name)

        UNION ALL

        SELECT 2, 'era', e.table_schema, e.table_name, e.era_name, NULL::name
        FROM sql_saga.era AS e
        WHERE (e.table_schema, e.table_name) = (relation_schema, relation_name)

        UNION ALL

        SELECT 3, 'foreign_key', fk.table_schema, fk.table_name, fk.fk_era_name,
               fk.foreign_key_name
        FROM sql_saga.foreign_keys AS fk
        WHERE (fk.table_schema, fk.table_name) = (relation_schema, relation_name)

        UNION ALL

        SELECT 4, 'history_table', sv.table_schema, sv.table_name, sv.era_name, NULL::name
        FROM sql_saga.system_versioning AS sv
        WHERE (sv.history_schema_name, sv.history_table_name) = (relation_schema, relation_name)

        UNION ALL

        SELECT 5, 'system_versioning_view', sv.table_schema, sv.table_name, sv.era_name, NULL::name
        FROM sql_saga.system_versioning AS sv
        WHERE (sv.view_schema_name, sv.view_table_name) = (relation_schema, relation_name)
           OR (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)) =
              (relation_schema, relation_name)

        UNION ALL

        SELECT CASE uv.view_type WHEN 'current' THEN 6 ELSE 7 END,
               uv.view_type::text, uv.table_schema, uv.table_name, uv.era_name, NULL::name
        FROM sql_saga.updatable_view AS uv
        WHERE (uv.view_schema, uv.view_name) = (relation_schema, relation_name)
    ) AS candidate
    ORDER BY candidate.priority, candidate.era_name, candidate.detail
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    IF managed.managed_class = 'system_versioning_base' THEN
        SELECT sv.history_schema_name, sv.history_table_name INTO history_schema, history_name
        FROM sql_saga.system_versioning AS sv
        WHERE (sv.table_schema, sv.table_name) = (managed.base_schema, managed.base_table);
        RAISE EXCEPTION 'cannot rename or move system-versioned table "%"; to move it with its history, %', relation_display_name,
            sql_saga.__internal_system_versioning_move_advice(managed.base_schema, managed.base_table, history_schema, history_name)
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'era' THEN
        RAISE EXCEPTION 'cannot rename or move table "%" because it has era "%"; drop dependent keys and foreign keys, then drop the era first with sql_saga.drop_era(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior, cleanup boolean)', relation_display_name, managed.era_name
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'foreign_key' THEN
        RAISE EXCEPTION 'cannot rename or move table "%" because it has foreign key "%"; drop it first with sql_saga.drop_foreign_key_by_name(table_oid regclass, key_name name, drop_index boolean)', relation_display_name, managed.detail
            USING ERRCODE = 'object_in_use';
    END IF;

    base_display_name := sql_saga.__internal_display_name(
        pg_catalog.format('%I.%I', managed.base_schema, managed.base_table));

    IF managed.managed_class = 'history_table' THEN
        RAISE EXCEPTION 'history table "%" is managed by sql_saga for table "%"; to move it with its table, %', relation_display_name, base_display_name,
            sql_saga.__internal_system_versioning_move_advice(managed.base_schema, managed.base_table, relation_schema, relation_name)
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'system_versioning_view' THEN
        RAISE EXCEPTION 'view "%" is managed by sql_saga for table "%"; it cannot be renamed or moved; drop system versioning first with sql_saga.drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior, cleanup boolean)', relation_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'current' THEN
        RAISE EXCEPTION 'view "%" is managed by sql_saga for table "%"; drop it with sql_saga.drop_current_view(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior) before renaming or moving it', relation_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    ELSE
        RAISE EXCEPTION 'view "%" is managed by sql_saga for table "%"; drop it with sql_saga.drop_for_portion_of_view(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior, cleanup boolean) before renaming or moving it', relation_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    END IF;
END;
$__internal_refuse_managed_relation_identity_change$;

REVOKE ALL ON FUNCTION sql_saga.__internal_refuse_managed_relation_identity_change(oid) FROM PUBLIC;

CREATE EVENT TRIGGER sql_saga_refuse_managed_relation_identity_change ON ddl_command_start
    WHEN TAG IN ('ALTER TABLE', 'ALTER VIEW', 'ALTER SCHEMA')
    EXECUTE PROCEDURE sql_saga.__internal_refuse_managed_relation_identity_change();

CREATE OR REPLACE FUNCTION sql_saga.__internal_record_alter_function_resolved()
RETURNS event_trigger
AS 'sql_saga', 'record_alter_function_resolved'
LANGUAGE c;

REVOKE ALL ON FUNCTION sql_saga.__internal_record_alter_function_resolved() FROM PUBLIC;

CREATE OR REPLACE FUNCTION sql_saga.__internal_set_alter_function_resolved(function_identity text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
DECLARE
    resolved text;
BEGIN
    /* Every start invocation replaces any prior statement's consumed value. */
    IF function_identity IS NULL THEN
        PERFORM set_config('sql_saga.alter_function_resolved', '', true);
        RETURN;
    END IF;

    SELECT coalesce(jsonb_agg(jsonb_build_object(
            'table_schema', f.table_schema,
            'table_name', f.table_name,
            'role', f.role,
            'identity', f.identity)
        ORDER BY f.table_schema, f.table_name, f.role)::text, '[]')
    INTO resolved
    FROM (
        SELECT sv.table_schema, sv.table_name, 'as_of' AS role, sv.func_as_of AS identity
        FROM sql_saga.system_versioning AS sv
        WHERE sv.func_as_of = function_identity
        UNION ALL
        SELECT sv.table_schema, sv.table_name, 'between', sv.func_between
        FROM sql_saga.system_versioning AS sv
        WHERE sv.func_between = function_identity
        UNION ALL
        SELECT sv.table_schema, sv.table_name, 'between_symmetric', sv.func_between_symmetric
        FROM sql_saga.system_versioning AS sv
        WHERE sv.func_between_symmetric = function_identity
        UNION ALL
        SELECT sv.table_schema, sv.table_name, 'from_to', sv.func_from_to
        FROM sql_saga.system_versioning AS sv
        WHERE sv.func_from_to = function_identity
    ) AS f
    WHERE to_regprocedure(f.identity) IS NOT NULL;

    /* Catalog values only: never persist an OID or reg* value across the statement. */
    PERFORM set_config('sql_saga.alter_function_resolved', resolved, true);
END;
$function$;

REVOKE ALL ON FUNCTION sql_saga.__internal_set_alter_function_resolved(text) FROM PUBLIC;

COMMENT ON FUNCTION sql_saga.__internal_set_alter_function_resolved(text) IS
'Records a parsed ALTER FUNCTION or ALTER ROUTINE target when it is a resolving generated SYSTEM VERSIONING function identity.';

CREATE OR REPLACE FUNCTION sql_saga.health_checks()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 -- Set a safe search path for this SECURITY DEFINER function
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    cmd text;
    r record;
    managed_policy record;
    gated_columns text;
    expected_gate text;
    alter_function_resolved text;
BEGIN
    IF tg_tag IN ('ALTER FUNCTION', 'ALTER ROUTINE') THEN
        alter_function_resolved := current_setting('sql_saga.alter_function_resolved', true);
        /* Consume the statement snapshot before doing any comparison or raising. */
        PERFORM set_config('sql_saga.alter_function_resolved', '', true);

        IF alter_function_resolved IS NULL OR alter_function_resolved = '' THEN
            RETURN;
        END IF;

        FOR r IN
            SELECT to_regclass(format('%I.%I', x.table_schema, x.table_name)) AS table_oid,
                   x.identity
            FROM jsonb_to_recordset(alter_function_resolved::jsonb) AS x(
                table_schema name,
                table_name name,
                role text,
                identity text)
            WHERE to_regprocedure(x.identity) IS NULL
            ORDER BY x.table_schema, x.table_name, x.role
        LOOP
            RAISE EXCEPTION 'cannot drop or rename function "%" because it is used in SYSTEM VERSIONING for table "%"',
                sql_saga.__internal_display_function_name(r.identity), sql_saga.__internal_display_name(r.table_oid);
        END LOOP;
    END IF;

    /* sql_drop protects exact managed members before DROP reaches ddl_command_end. */
    IF tg_tag LIKE 'DROP %' THEN
        RETURN;
    END IF;

    -- Exit early if the DDL command does not affect a managed object.
    IF NOT sql_saga.__internal_ddl_command_affects_managed_object() THEN
        RETURN;
    END IF;

    /* Make sure that all of our tables are still persistent */
    FOR r IN
        SELECT to_regclass(format('%I.%I', e.table_schema /* %I */, e.table_name /* %I */)) AS table_oid
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_class AS c ON c.relname = e.table_name
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = e.table_schema
        WHERE c.relpersistence <> 'p'
    LOOP
        RAISE EXCEPTION 'table "%" must remain persistent because it has an era',
            sql_saga.__internal_display_name(r.table_oid);
    END LOOP;

    /* And the history tables, too */
    FOR r IN
        SELECT to_regclass(format('%I.%I', e.table_schema /* %I */, e.table_name /* %I */)) AS table_oid
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_class AS c ON c.relname = e.audit_table_name
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace AND n.nspname = e.audit_schema_name
        WHERE e.audit_table_name IS NOT NULL AND c.relpersistence <> 'p'
    LOOP
        RAISE EXCEPTION 'history table "%" must remain persistent because it has an era',
            sql_saga.__internal_display_name(r.table_oid);
    END LOOP;

    /* Fix up history and for-portion objects ownership */
    FOR cmd IN
        /* Fix ownership of history tables */
        SELECT format('ALTER TABLE %I.%I OWNER TO %I',
            sv.history_schema_name /* %I */, sv.history_table_name /* %I */, t.relowner::regrole /* %I */)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        JOIN pg_class AS ht ON ht.relname = sv.history_table_name
        JOIN pg_namespace AS htn ON htn.oid = ht.relnamespace AND htn.nspname = sv.history_schema_name
        WHERE t.relowner <> ht.relowner

        UNION ALL

        /* Fix ownership of system versioning views */
        SELECT format('ALTER VIEW %I.%I OWNER TO %I',
            sv.view_schema_name /* %I */, sv.view_table_name /* %I */, t.relowner::regrole /* %I */)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        JOIN pg_class AS vt ON vt.relname = sv.view_table_name
        JOIN pg_namespace AS vtn ON vtn.oid = vt.relnamespace AND vtn.nspname = sv.view_schema_name
        WHERE t.relowner <> vt.relowner

        UNION ALL

        /* Fix ownership of the gated history views */
        SELECT format('ALTER VIEW %s OWNER TO %I', f.gated_oid::regclass, t.relowner::regrole)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        CROSS JOIN LATERAL sql_saga.__internal_history_family(t.oid) AS f
        JOIN pg_class AS gv ON gv.oid = f.gated_oid
        WHERE f.base_oid = t.oid AND t.relowner <> gv.relowner

        UNION ALL

        /* Fix ownership of for-portion-of views */
        SELECT format('ALTER VIEW %I.%I OWNER TO %I', v.view_schema /* %I */, v.view_name /* %I */, t.relowner::regrole /* %I */)
        FROM sql_saga.updatable_view v
        JOIN pg_class t ON t.relname = v.table_name
        JOIN pg_namespace tn ON tn.oid = t.relnamespace AND tn.nspname = v.table_schema
        JOIN pg_class vt ON vt.relname = v.view_name
        JOIN pg_namespace vn ON vn.oid = vt.relnamespace AND vn.nspname = v.view_schema
        WHERE t.relowner <> vt.relowner

        UNION ALL

        /* Fix ownership of system versioning functions */
        SELECT format('ALTER FUNCTION %s OWNER TO %I', p.oid::regprocedure, t.relowner::regrole)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        JOIN pg_proc AS p ON p.oid IN (to_regprocedure(sv.func_as_of), to_regprocedure(sv.func_between),
                                       to_regprocedure(sv.func_between_symmetric), to_regprocedure(sv.func_from_to))
        WHERE t.relowner <> p.proowner
    LOOP
        EXECUTE cmd;
    END LOOP;

    /*
     * Row-level security of history tables (tmp/design_final.md sections 3, 8-10, 15).
     * Policy DDL on a versioned base table (or its history), and ENABLE/DISABLE/FORCE/NO FORCE on
     * either, re-synchronizes that family's history policies. Direct changes to the policies
     * sql_saga manages, and to the history's RLS settings, are refused. Only the synchronizer's own
     * nested DDL is exempt, recognized by its private guard row
     * (sql_saga.__internal_history_policy_sync_active), never by a setting a caller could forge.
     */
    FOR r IN
        SELECT DISTINCT f.base_oid, f.history_oid, ev.command_tag, ev.objid, ev.classid
        FROM pg_event_trigger_ddl_commands() AS ev
        LEFT JOIN pg_catalog.pg_policy AS pol
               ON ev.classid = 'pg_catalog.pg_policy'::regclass AND pol.oid = ev.objid
        CROSS JOIN LATERAL sql_saga.__internal_history_family(
            CASE WHEN ev.classid = 'pg_catalog.pg_policy'::regclass THEN pol.polrelid ELSE ev.objid END) AS f
        WHERE ev.command_tag IN ('CREATE POLICY', 'ALTER POLICY', 'ALTER TABLE')
    LOOP
        IF NOT sql_saga.__internal_history_policy_sync_active(r.base_oid) THEN
            -- Refuse direct DDL on managed history policies.
            -- CREATE POLICY of a managed name is what pg_restore does when it replays a dump: the sync
            -- below then keeps it (current installation) or drops it (another installation's token).
            IF r.classid = 'pg_catalog.pg_policy'::regclass THEN
                SELECT p.polname, p.polrelid INTO managed_policy
                FROM pg_catalog.pg_policy AS p WHERE p.oid = r.objid;
                IF managed_policy.polrelid = r.history_oid THEN
                    IF NOT sql_saga.__internal_is_managed_history_policy(managed_policy.polname) THEN
                        -- A policy of one's own on the history table (decision 7): it would be OR-ed
                        -- with the copies and could show more history than the base table allows.
                        -- ALTER POLICY ... RENAME TO a non-managed name lands here too.
                        RAISE EXCEPTION 'sql_saga: policies on history table "%" are managed by sql_saga', sql_saga.__internal_display_name(r.history_oid::regclass)
                            USING HINT = format('Create the policy on "%s"; sql_saga copies its SELECT and ALL policies to the history.',
                                                sql_saga.__internal_display_name(r.base_oid::regclass));
                    ELSIF r.command_tag = 'ALTER POLICY' THEN
                        RAISE EXCEPTION 'cannot drop or alter policy "%" on "%" because it is managed by sql_saga for table "%"',
                            managed_policy.polname, sql_saga.__internal_display_name(r.history_oid::regclass), sql_saga.__internal_display_name(r.base_oid::regclass);
                    END IF;
                END IF;
            END IF;
            -- Refuse direct changes to the history's RLS settings.
            IF r.command_tag = 'ALTER TABLE' AND r.objid = r.history_oid
               AND NOT EXISTS (SELECT FROM pg_catalog.pg_class AS h, pg_catalog.pg_class AS b
                               WHERE h.oid = r.history_oid AND b.oid = r.base_oid
                                 AND h.relrowsecurity AND h.relforcerowsecurity = b.relforcerowsecurity) THEN
                RAISE EXCEPTION 'history table "%" must keep row level security enabled, and forced exactly when its base table is, because it is used in SYSTEM VERSIONING for table "%"',
                    sql_saga.__internal_display_name(r.history_oid::regclass), sql_saga.__internal_display_name(r.base_oid::regclass);
            END IF;
        END IF;
        -- Not during the teardown by pg_restore --clean (its ALTER TABLE .. DROP CONSTRAINT on the base
        -- would recreate the history policies drop_protection has just dropped for the teardown).
        IF NOT sql_saga.__internal_history_policy_sync_active(r.base_oid)
           AND NOT sql_saga.__internal_is_clean_teardown()
           AND (r.objid <> r.history_oid OR r.classid = 'pg_catalog.pg_policy'::regclass) THEN
            PERFORM sql_saga.__internal_sync_history_policies(r.base_oid);
        END IF;
    END LOOP;

    /* The fixed shape of views sql_saga manages. */
    FOR r IN
        SELECT ev.objid AS view_oid, v.base_oid, v.kind
        FROM pg_event_trigger_ddl_commands() AS ev
        JOIN (
            SELECT format('%I.%I', sv.view_schema_name, sv.view_table_name)::regclass::oid AS view_oid,
                   format('%I.%I', sv.table_schema, sv.table_name)::regclass::oid AS base_oid,
                   'SYSTEM VERSIONING'::text AS kind
            FROM sql_saga.system_versioning AS sv
            WHERE to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name)) IS NOT NULL
            UNION ALL
            SELECT format('%I.%I', uv.view_schema, uv.view_name)::regclass::oid,
                   format('%I.%I', uv.table_schema, uv.table_name)::regclass::oid,
                   CASE uv.view_type WHEN 'for_portion_of' THEN 'FOR PORTION OF' ELSE 'a current view' END
            FROM sql_saga.updatable_view AS uv
            WHERE to_regclass(format('%I.%I', uv.view_schema, uv.view_name)) IS NOT NULL
        ) AS v ON v.view_oid = ev.objid
        WHERE ev.command_tag IN ('ALTER VIEW', 'CREATE VIEW', 'ALTER TABLE')
    LOOP
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c
                       WHERE c.oid = r.view_oid AND 'security_invoker=on' = ANY (c.reloptions)) THEN
            RAISE EXCEPTION 'view "%" must keep security_invoker because it is used in % for table "%"',
                sql_saga.__internal_display_name(r.view_oid::regclass), r.kind, sql_saga.__internal_display_name(r.base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT ev.objid AS view_oid, f.base_oid, f.history_oid
        FROM pg_event_trigger_ddl_commands() AS ev
        JOIN sql_saga.system_versioning AS sv
          ON to_regclass(format('%I.%I', sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)))::oid = ev.objid
        CROSS JOIN LATERAL sql_saga.__internal_history_family(format('%I.%I', sv.table_schema, sv.table_name)::regclass) AS f
        WHERE ev.command_tag IN ('ALTER VIEW', 'CREATE VIEW', 'ALTER TABLE')
    LOOP
        -- The gate must be exactly the generated definition (owner-rights, same body), not just
        -- contain its parts: e.g. "WHERE true OR (gate)" keeps every function reference but opens
        -- the view. The canonical body is rendered from the view's own column list, so the check
        -- holds whatever columns the history has, and both are deparsed the same way.
        gated_columns := (SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum)
                          FROM pg_catalog.pg_attribute AS a
                          WHERE a.attrelid = r.view_oid AND a.attnum > 0 AND NOT a.attisdropped);
        EXECUTE 'CREATE TEMPORARY VIEW sql_saga_expected_gate AS '
            || sql_saga.__internal_history_gated_view_sql(r.base_oid, r.history_oid, gated_columns);
        expected_gate := pg_catalog.pg_get_viewdef('pg_temp.sql_saga_expected_gate'::regclass);
        DROP VIEW pg_temp.sql_saga_expected_gate;
        IF EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = r.view_oid AND c.reloptions IS NOT NULL)
           OR pg_catalog.pg_get_viewdef(r.view_oid) IS DISTINCT FROM expected_gate THEN
            RAISE EXCEPTION 'view "%" must keep its access gate because it is used in SYSTEM VERSIONING for table "%"',
                sql_saga.__internal_display_name(r.view_oid::regclass), sql_saga.__internal_display_name(r.base_oid::regclass);
        END IF;
    END LOOP;

END;
$function$;

COMMENT ON FUNCTION sql_saga.health_checks() IS
'An event trigger function that runs after DDL commands to check for inconsistencies in sql_saga''s metadata catalogs.';

CREATE EVENT TRIGGER sql_saga_record_alter_function_resolved ON ddl_command_start
    WHEN TAG IN ('ALTER FUNCTION', 'ALTER ROUTINE')
    EXECUTE PROCEDURE sql_saga.__internal_record_alter_function_resolved();

/* Step 4: integrity checker, canonical SYSTEM VERSIONING query-function generator, and tolerant metadata-first drops. */
CREATE OR REPLACE FUNCTION sql_saga.drop_foreign_key_by_name(
    table_oid regclass,
    key_name name,
    drop_index boolean DEFAULT true)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    foreign_key_row record;
    unique_table_oid regclass;
    fk_table_oid regclass;
    fk_schema_name name;
    fk_table_name name;
    native_constraint_attributable boolean;
BEGIN
    IF table_oid IS NULL AND key_name IS NULL THEN
        RAISE EXCEPTION 'no table or key name specified';
    END IF;

    IF table_oid IS NOT NULL THEN
        SELECT n.nspname, c.relname
        INTO fk_schema_name, fk_table_name
        FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.oid = table_oid;
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    FOR foreign_key_row IN
        SELECT fk.*,
               fkc.oid AS fk_table_oid,
               ukc.oid AS unique_table_oid,
               coalesce(
                   fk.type <> 'temporal_to_temporal'
                   OR (
                       c.oid IS NOT NULL
                       AND c.contype = 'f'
                       AND c.conrelid = fkc.oid
                       AND c.confrelid = ukc.oid
                       AND c.conkey = fk_cols.attnums
                       AND c.confkey = uk_cols.attnums
                       AND c.conperiod
                       AND c.condeferrable
                       AND NOT c.condeferred
                       AND c.convalidated
                       AND c.conenforced
                       AND c.confmatchtype = CASE fk.match_type WHEN 'FULL' THEN 'f' WHEN 'PARTIAL' THEN 'p' ELSE 's' END
                       AND c.confupdtype = CASE fk.update_action::text WHEN 'RESTRICT' THEN 'r' WHEN 'CAS' || 'CADE' THEN 'c' WHEN 'SET NULL' THEN 'n' WHEN 'SET DEFAULT' THEN 'd' ELSE 'a' END
                       AND c.confdeltype = CASE fk.delete_action::text WHEN 'RESTRICT' THEN 'r' WHEN 'CAS' || 'CADE' THEN 'c' WHEN 'SET NULL' THEN 'n' WHEN 'SET DEFAULT' THEN 'd' ELSE 'a' END
                   ),
                   false) AS native_constraint_attributable
        FROM sql_saga.foreign_keys AS fk
        LEFT JOIN pg_catalog.pg_namespace AS fkn ON fkn.nspname = fk.table_schema
        LEFT JOIN pg_catalog.pg_class AS fkc
          ON fkc.relnamespace = fkn.oid AND fkc.relname = fk.table_name
        LEFT JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        LEFT JOIN pg_catalog.pg_namespace AS ukn ON ukn.nspname = uk.table_schema
        LEFT JOIN pg_catalog.pg_class AS ukc
          ON ukc.relnamespace = ukn.oid AND ukc.relname = uk.table_name
        LEFT JOIN sql_saga.era AS fk_era
          ON (fk_era.table_schema, fk_era.table_name, fk_era.era_name)
           = (fk.table_schema, fk.table_name, fk.fk_era_name)
        LEFT JOIN sql_saga.era AS uk_era
          ON (uk_era.table_schema, uk_era.table_name, uk_era.era_name)
           = (uk.table_schema, uk.table_name, uk.era_name)
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS attnums
            FROM pg_catalog.unnest(fk.column_names || fk_era.range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = fkc.oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS fk_cols ON fk.type = 'temporal_to_temporal'
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS attnums
            FROM pg_catalog.unnest(uk.column_names || uk_era.range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = ukc.oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS uk_cols ON fk.type = 'temporal_to_temporal'
        LEFT JOIN pg_catalog.pg_constraint AS c
          ON fk.type = 'temporal_to_temporal'
         AND c.conrelid = fkc.oid
         AND c.conname = fk.foreign_key_name
        WHERE (table_oid IS NULL OR (fk.table_schema, fk.table_name) = (fk_schema_name, fk_table_name))
          AND (fk.foreign_key_name = key_name OR key_name IS NULL)
    LOOP
        native_constraint_attributable := foreign_key_row.native_constraint_attributable;

        -- Delete the metadata record first, so the drop_protection trigger doesn't fire on the main objects.
        DELETE FROM sql_saga.foreign_keys AS fk
        WHERE fk.foreign_key_name = foreign_key_row.foreign_key_name;

        /*
         * Now that the metadata is gone, we can safely drop the database objects.
         */

        -- Drop the auto-created index if it exists and requested.
        IF drop_index AND foreign_key_row.fk_index_name IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: Dropping automatically created index "%" for foreign key "%"', foreign_key_row.fk_index_name, foreign_key_row.foreign_key_name;
            EXECUTE format('DROP INDEX IF EXISTS %I.%I', foreign_key_row.table_schema, foreign_key_row.fk_index_name);
        END IF;
        fk_table_oid := foreign_key_row.fk_table_oid;

        CASE foreign_key_row.type
            WHEN 'temporal_to_temporal' THEN
                -- For native temporal FKs, we drop the constraint directly.
                IF EXISTS (SELECT FROM pg_constraint WHERE conrelid = fk_table_oid AND conname = foreign_key_row.foreign_key_name) THEN
                    IF native_constraint_attributable THEN
                        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk_table_oid /* %s */, foreign_key_row.foreign_key_name /* %I */);
                    ELSE
                        RAISE NOTICE 'sql_saga: Left constraint "%" on table "%" in place because it is not the sql_saga-managed constraint',
                            foreign_key_row.foreign_key_name, sql_saga.__internal_display_name(fk_table_oid);
                    END IF;
                END IF;
            WHEN 'regular_to_temporal' THEN
                IF EXISTS (SELECT FROM pg_constraint WHERE conrelid = fk_table_oid AND conname = foreign_key_row.fk_check_constraint) THEN
                    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk_table_oid /* %s */, foreign_key_row.fk_check_constraint /* %I */);
                END IF;
                -- If no other foreign keys use the helper function, drop it.
                IF NOT EXISTS (
                    SELECT 1 FROM sql_saga.foreign_keys
                    WHERE fk_helper_function = foreign_key_row.fk_helper_function
                ) THEN
                    IF to_regprocedure(foreign_key_row.fk_helper_function) IS NOT NULL THEN
                        EXECUTE format('DROP FUNCTION %s', foreign_key_row.fk_helper_function);
                    END IF;
                END IF;
        END CASE;

        unique_table_oid := foreign_key_row.unique_table_oid;

        IF unique_table_oid IS NOT NULL AND pg_catalog.to_regclass(unique_table_oid::text) IS NOT NULL THEN
            IF EXISTS (SELECT FROM pg_trigger WHERE tgrelid = unique_table_oid AND tgname = foreign_key_row.uk_update_trigger) THEN
                EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.uk_update_trigger /* %I */, unique_table_oid /* %s */);
            END IF;
            IF EXISTS (SELECT FROM pg_trigger WHERE tgrelid = unique_table_oid AND tgname = foreign_key_row.uk_delete_trigger) THEN
                EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.uk_delete_trigger /* %I */, unique_table_oid /* %s */);
            END IF;
        END IF;
    END LOOP;

    RETURN true;
END;
$function$;
CREATE OR REPLACE FUNCTION sql_saga.drop_unique_key_by_name(
    table_oid regclass,
    key_name name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT',
    cleanup boolean DEFAULT true
 )
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    foreign_key_row sql_saga.foreign_keys;
    unique_key_row sql_saga.unique_keys;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    FOR unique_key_row IN
        SELECT uk.*
        FROM sql_saga.unique_keys AS uk
        WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name)
          AND (uk.unique_key_name = key_name OR key_name IS NULL)
    LOOP
        RAISE DEBUG 'drop_unique_key_by_name: Processing key: %', row_to_json(unique_key_row);
        /* Propagate the requested drop behavior to foreign keys, if desired */
        FOR foreign_key_row IN
            SELECT fk.*
            FROM sql_saga.foreign_keys AS fk
            WHERE fk.unique_key_name = unique_key_row.unique_key_name
        LOOP
            IF drop_behavior = 'RESTRICT' THEN
                RAISE EXCEPTION 'cannot drop unique key "%" because foreign key "%" on table "%" depends on it',
                    unique_key_row.unique_key_name, foreign_key_row.foreign_key_name,
                    sql_saga.__internal_display_name(format('%I.%I', foreign_key_row.table_schema, foreign_key_row.table_name)::regclass);
            END IF;

            PERFORM sql_saga.drop_foreign_key_by_name(NULL, foreign_key_row.foreign_key_name);
        END LOOP;

        DELETE FROM sql_saga.unique_keys AS uk
        WHERE uk.unique_key_name = unique_key_row.unique_key_name;

        /* If purging, drop the underlying constraints unless the table has been dropped */
        IF cleanup AND EXISTS (
            SELECT FROM pg_catalog.pg_class AS c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE (n.nspname, c.relname) = (unique_key_row.table_schema, unique_key_row.table_name))
        THEN
            IF unique_key_row.mutually_exclusive_columns IS NOT NULL THEN
                DECLARE
                    alter_parts text[];
                    object_name name;
                BEGIN
                    -- Build a list of constraints to drop in a single ALTER TABLE command.
                    alter_parts := ARRAY[]::text[];

                    -- 1. Add the CHECK constraint to the drop list.
                    IF unique_key_row.check_constraint IS NOT NULL THEN
                        alter_parts := alter_parts || format('DROP CONSTRAINT IF EXISTS %I', unique_key_row.check_constraint);
                    END IF;

                    -- 2. No partial constraints or indexes to drop for XOR keys using NULLS NOT DISTINCT

                    -- 3. Execute the ALTER TABLE command for all constraints.
                    IF cardinality(alter_parts) > 0 THEN
                        EXECUTE format('ALTER TABLE %I.%I %s',
                            unique_key_row.table_schema,
                            unique_key_row.table_name,
                            array_to_string(alter_parts, ', ')
                        );
                    END IF;
                END;
            ELSIF unique_key_row.key_type = 'predicated' THEN
                IF unique_key_row.check_constraint IS NOT NULL THEN
                    -- This is a unique index, drop it and the exclusion constraint
                    EXECUTE format('DROP INDEX IF EXISTS %I.%I; ALTER TABLE %I.%I DROP CONSTRAINT IF EXISTS %I;',
                        unique_key_row.table_schema, /* %I */
                        unique_key_row.unique_constraint, /* %I */
                        unique_key_row.table_schema, /* %I */
                        unique_key_row.table_name, /* %I */
                        unique_key_row.exclude_constraint /* %I */
                    );
                END IF;
            ELSE -- 'primary' or 'natural'
                DECLARE
                    alter_parts text[] := '{}';
                BEGIN
                    -- This is a unique/primary key constraint, drop both constraints if they exist.
                    IF unique_key_row.unique_constraint IS NOT NULL THEN
                        alter_parts := alter_parts || format('DROP CONSTRAINT IF EXISTS %I', unique_key_row.unique_constraint);
                    END IF;
                    IF unique_key_row.exclude_constraint IS NOT NULL THEN
                        alter_parts := alter_parts || format('DROP CONSTRAINT IF EXISTS %I', unique_key_row.exclude_constraint);
                    END IF;

                    IF cardinality(alter_parts) > 0 THEN
                        EXECUTE format('ALTER TABLE %I.%I %s',
                            unique_key_row.table_schema,
                            unique_key_row.table_name,
                            array_to_string(alter_parts, ', ')
                        );
                    END IF;
                END;
            END IF;
        END IF;
    END LOOP;

END;
$function$;

COMMENT ON FUNCTION sql_saga.drop_unique_key_by_name(regclass, name, sql_saga.drop_behavior, boolean) IS
'Drops a temporal unique key identified by its unique name.';
CREATE OR REPLACE FUNCTION sql_saga.drop_for_portion_of_view(table_oid regclass, era_name name DEFAULT 'valid', drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    view_schema name;
    view_name name;
    trigger_name name;
    target_schema_name name;
    target_table_name name;
BEGIN
    /*
     * If table_oid and era_name are specified, then just drop the views for that.
     *
     * If no period is specified, drop the views for all periods of the table.
     *
     * If no table is specified, drop the views everywhere.
     *
     * If no table is specified but a period is, that doesn't make any sense.
     */
    IF table_oid IS NULL AND era_name IS NOT NULL THEN
        RAISE EXCEPTION 'cannot specify era name without table name';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    IF table_oid IS NOT NULL THEN
        SELECT n.nspname, c.relname
        INTO target_schema_name, target_table_name
        FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.oid = table_oid;
    END IF;

    FOR view_schema, view_name, trigger_name IN
        DELETE FROM sql_saga.updatable_view AS v
        WHERE (table_oid IS NULL OR (v.table_schema, v.table_name) = (target_schema_name, target_table_name))
          AND (era_name IS NULL OR v.era_name = era_name)
          AND v.view_type = 'for_portion_of'
        RETURNING v.view_schema, v.view_name, v.trigger_name
    LOOP
        IF to_regclass(format('%I.%I', view_schema, view_name)) IS NOT NULL THEN
            IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = to_regclass(format('%I.%I', view_schema, view_name)) AND t.tgname = trigger_name) THEN
                EXECUTE format('DROP TRIGGER %I ON %I.%I', trigger_name, view_schema, view_name);
            END IF;
            EXECUTE format('DROP VIEW IF EXISTS %I.%I %s', view_schema, view_name, drop_behavior);
        END IF;
    END LOOP;

    RETURN true;
END;
$function$;

COMMENT ON FUNCTION sql_saga.drop_for_portion_of_view(regclass, name, sql_saga.drop_behavior, boolean) IS
'Drops the "for_portion_of" view associated with a table and era.';
CREATE OR REPLACE FUNCTION sql_saga.drop_era(table_oid regclass, era_name name DEFAULT 'valid', drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    era_row sql_saga.era;
    portion_view regclass;
    is_dropped boolean;
    table_schema name;
    table_name name;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = table_oid;

    IF era_name IS NULL THEN
        RAISE EXCEPTION 'no era name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    /*
     * Has the table been dropped already?  This could happen if the period is
     * being dropped by the drop_protection event trigger or through a DROP
     * cascading drop.
     */
    is_dropped := NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid);

    SELECT p.*
    INTO era_row
    FROM sql_saga.era AS p
    WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

    IF NOT FOUND THEN
        RAISE DEBUG 'era % not found for table %', era_name, table_oid;
        RETURN false;
    END IF;

    /* If this is a system_time era, get rid of the triggers */
    DECLARE
        system_time_era_row sql_saga.system_time_era;
    BEGIN
        DELETE FROM sql_saga.system_time_era AS ste
        WHERE (ste.table_schema, ste.table_name, ste.era_name) = (table_schema, table_name, era_name)
        RETURNING * INTO system_time_era_row;

        IF FOUND AND NOT is_dropped THEN
            IF EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = table_oid AND c.conname = system_time_era_row.infinity_check_constraint) THEN EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid, system_time_era_row.infinity_check_constraint); END IF;
            IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = table_oid AND t.tgname = system_time_era_row.generated_always_trigger) THEN EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.generated_always_trigger, table_oid); END IF;
            IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = table_oid AND t.tgname = system_time_era_row.write_history_trigger) THEN EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.write_history_trigger, table_oid); END IF;
            IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = table_oid AND t.tgname = system_time_era_row.truncate_trigger) THEN EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.truncate_trigger, table_oid); END IF;
        END IF;
    END;

    IF drop_behavior = 'RESTRICT' THEN
        /* Check for UNIQUE or PRIMARY KEYs */
        IF EXISTS (
            SELECT FROM sql_saga.unique_keys AS uk
            WHERE (uk.table_schema, uk.table_name, uk.era_name) = (table_schema, table_name, era_name))
        THEN
            RAISE EXCEPTION 'era % is part of a UNIQUE or PRIMARY KEY', era_name;
        END IF;

        /* Check for FOREIGN KEYs */
        IF EXISTS (
            SELECT FROM sql_saga.foreign_keys AS fk
            WHERE (fk.table_schema, fk.table_name, fk.fk_era_name) = (table_schema, table_name, era_name))
        THEN
            RAISE EXCEPTION 'era % is part of a FOREIGN KEY', era_name;
        END IF;

        /* Check for updatable views */
        IF EXISTS (
            SELECT FROM sql_saga.updatable_view AS v
            WHERE (v.table_schema, v.table_name, v.era_name) = (table_schema, table_name, era_name))
        THEN
            RAISE EXCEPTION 'era % is used by an updatable view', era_name;
        END IF;

        /* Check for SYSTEM VERSIONING - must use drop_system_versioning() instead */
        IF EXISTS (
            SELECT FROM sql_saga.system_versioning AS sv
            WHERE (sv.table_schema, sv.table_name, sv.era_name) = (table_schema, table_name, era_name))
        THEN
            RAISE EXCEPTION 'era % has SYSTEM VERSIONING, use drop_system_versioning() instead', era_name;
        END IF;

        /* Drop synchronization trigger BEFORE removing from catalog */
        IF NOT is_dropped AND (era_row.valid_from_column_name IS NOT NULL OR era_row.valid_to_column_name IS NOT NULL) THEN
            CALL sql_saga.drop_synchronize_temporal_columns_trigger(table_oid, era_name);
        END IF;

        /* Remove from catalog */
        DELETE FROM sql_saga.era AS p
        WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

        /* Delete bounds check constraint (NOT isempty(range)). */
        IF NOT is_dropped AND era_row.bounds_check_constraint IS NOT NULL THEN
            IF EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = table_oid AND c.conname = era_row.bounds_check_constraint) THEN EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid, era_row.bounds_check_constraint); END IF;
        END IF;

        /* Delete boundary check constraint (from < until). */
        IF NOT is_dropped AND era_row.boundary_check_constraint IS NOT NULL THEN
            IF EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = table_oid AND c.conname = era_row.boundary_check_constraint) THEN EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid, era_row.boundary_check_constraint); END IF;
        END IF;

        /* Drop column default if it was set */
        IF NOT is_dropped AND NOT era_row.trigger_applies_defaults AND era_row.valid_until_column_name IS NOT NULL THEN
            EXECUTE format('ALTER TABLE %s ALTER COLUMN %I DROP DEFAULT', table_oid, era_row.valid_until_column_name);
        END IF;

        /* Delete columns if purging */
        IF NOT is_dropped AND cleanup THEN
            DECLARE
                drop_clauses text[] := '{}';
            BEGIN
                -- The range column is always present for an era.
                drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.range_column_name);

                -- Drop from/until if they were part of the era.
                IF era_row.valid_from_column_name IS NOT NULL THEN
                    drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_from_column_name);
                    drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_until_column_name);
                END IF;

                -- Drop to if it was part of the era.
                IF era_row.valid_to_column_name IS NOT NULL THEN
                    drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_to_column_name);
                END IF;

                IF array_length(drop_clauses, 1) > 0 THEN
                     EXECUTE format('ALTER TABLE %s %s', table_oid, array_to_string(drop_clauses, ', '));
                END IF;
            END;
        END IF;

        RETURN true;
    END IF;

    /* We must be in cascading-drop mode now */

    PERFORM sql_saga.drop_for_portion_of_view(table_oid, era_name, drop_behavior, cleanup);

    PERFORM sql_saga.drop_foreign_key_by_name(table_oid, fk.foreign_key_name)
    FROM sql_saga.foreign_keys AS fk
    WHERE (fk.table_schema, fk.table_name, fk.fk_era_name) = (table_schema, table_name, era_name);

    PERFORM sql_saga.drop_unique_key_by_name(table_oid, uk.unique_key_name, drop_behavior, cleanup)
    FROM sql_saga.unique_keys AS uk
    WHERE (uk.table_schema, uk.table_name, uk.era_name) = (table_schema, table_name, era_name);

    /* Drop synchronization trigger BEFORE removing from catalog */
    IF NOT is_dropped AND (era_row.valid_from_column_name IS NOT NULL OR era_row.valid_to_column_name IS NOT NULL) THEN
        CALL sql_saga.drop_synchronize_temporal_columns_trigger(table_oid, era_name);
    END IF;

    /* Remove from catalog */
    DELETE FROM sql_saga.era AS p
    WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

    /* Delete bounds check constraint (NOT isempty(range)). */
    IF NOT is_dropped AND era_row.bounds_check_constraint IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid /* %s */, era_row.bounds_check_constraint /* %I */);
    END IF;

    /* Delete boundary check constraint (from < until). */
    IF NOT is_dropped AND era_row.boundary_check_constraint IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid /* %s */, era_row.boundary_check_constraint /* %I */);
    END IF;

    /* Drop column default if it was set */
    IF NOT is_dropped AND NOT era_row.trigger_applies_defaults AND era_row.valid_until_column_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s ALTER COLUMN %I DROP DEFAULT', table_oid, era_row.valid_until_column_name);
    END IF;

    /* Delete columns if purging */
    IF NOT is_dropped AND cleanup THEN
        DECLARE
            drop_clauses text[] := '{}';
        BEGIN
            -- The range column is always present for an era.
            drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.range_column_name);

            -- Drop from/until if they were part of the era.
            IF era_row.valid_from_column_name IS NOT NULL THEN
                drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_from_column_name);
                drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_until_column_name);
            END IF;

            -- Drop to if it was part of the era.
            IF era_row.valid_to_column_name IS NOT NULL THEN
                drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_to_column_name);
            END IF;

            IF array_length(drop_clauses, 1) > 0 THEN
                 EXECUTE format('ALTER TABLE %s %s', table_oid, array_to_string(drop_clauses, ', '));
            END IF;
        END;
    END IF;

    RETURN true;
END;
$function$;

COMMENT ON FUNCTION sql_saga.drop_era(regclass, name, sql_saga.drop_behavior, boolean) IS
'Deregisters a temporal table, removing all associated constraints, triggers, and metadata.';
CREATE OR REPLACE FUNCTION sql_saga.drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS $function$
#variable_conflict use_variable
DECLARE
    system_versioning_row sql_saga.system_versioning;
    is_dropped boolean;
    table_schema name;
    table_name name;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = table_oid;

    DELETE FROM sql_saga.system_versioning AS sv
    WHERE (sv.table_schema, sv.table_name) = (table_schema, table_name)
    RETURNING * INTO system_versioning_row;

    IF NOT FOUND THEN
        RAISE DEBUG 'table % does not have SYSTEM VERSIONING', table_oid;
        RETURN false;
    END IF;

    is_dropped := NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid);

    IF NOT is_dropped THEN
        IF to_regprocedure(system_versioning_row.func_as_of) IS NOT NULL THEN EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_as_of, drop_behavior); END IF;
        IF to_regprocedure(system_versioning_row.func_between) IS NOT NULL THEN EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between, drop_behavior); END IF;
        IF to_regprocedure(system_versioning_row.func_between_symmetric) IS NOT NULL THEN EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between_symmetric, drop_behavior); END IF;
        IF to_regprocedure(system_versioning_row.func_from_to) IS NOT NULL THEN EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_from_to, drop_behavior); END IF;
        IF to_regclass(format('%I.%I', system_versioning_row.view_schema_name, system_versioning_row.view_table_name)) IS NOT NULL THEN
            EXECUTE format('DROP VIEW %I.%I %s', system_versioning_row.view_schema_name, system_versioning_row.view_table_name, drop_behavior);
        END IF;
        IF to_regclass(format('%I.%I', system_versioning_row.history_schema_name, sql_saga.__internal_history_gated_name(system_versioning_row.history_table_name))) IS NOT NULL THEN
            EXECUTE format('DROP VIEW %I.%I %s', system_versioning_row.history_schema_name,
                sql_saga.__internal_history_gated_name(system_versioning_row.history_table_name), drop_behavior);
        END IF;
    END IF;

    IF NOT is_dropped AND cleanup THEN
        PERFORM sql_saga.drop_system_time_era(table_oid, drop_behavior, cleanup);
        IF to_regclass(format('%I.%I', system_versioning_row.history_schema_name, system_versioning_row.history_table_name)) IS NOT NULL THEN
            EXECUTE format('DROP TABLE %I.%I %s', system_versioning_row.history_schema_name, system_versioning_row.history_table_name, drop_behavior);
        END IF;
    END IF;

    RETURN true;
END;
$function$;

COMMENT ON FUNCTION sql_saga.drop_system_versioning(regclass, sql_saga.drop_behavior, boolean) IS
'Removes system versioning from a table, dropping the history table and all associated objects.';
/*
 * The views and as-of functions of a system-versioned table (tmp/design_manual_schema_changes.md).
 *
 * One generator serves add_system_versioning and rebuild_system_versioning_views, so both create the
 * same objects. Columns correspond by NAME: the views show the base table's columns in its order,
 * then the history-only columns in the history's order (NULL for current rows). Every base column
 * must be in the history with the same type, typmod and collation, except those left out with
 * without_columns, which only shapes the views for one call and is stored nowhere.
 *
 * The objects keep the fixed access shape of tmp/design_final.md: owner = base owner, SELECT (views)
 * or EXECUTE (functions) granted to PUBLIC and to nobody else, B_with_history security_invoker,
 * B_history_gated owner-rights with its gate.
 */

-- A column type as written in DDL, with COLLATE when the collation is not the type's default.
CREATE OR REPLACE FUNCTION sql_saga.__internal_column_type_sql(type_oid oid, typmod integer, collation_oid oid)
RETURNS text
LANGUAGE sql
STABLE
-- Only pg_catalog is visible, so format_type schema-qualifies every other type (sql_saga's included)
-- and the DDL in HINTs runs whatever the caller's search_path.
SET search_path = pg_catalog
AS $__internal_column_type_sql$
    SELECT pg_catalog.format_type(type_oid, typmod)
        || coalesce((SELECT ' COLLATE ' || pg_catalog.quote_ident(co.collname)
                     FROM pg_catalog.pg_collation AS co
                     WHERE co.oid = collation_oid
                       AND collation_oid IS DISTINCT FROM (SELECT t.typcollation FROM pg_catalog.pg_type AS t WHERE t.oid = type_oid)), '');
$__internal_column_type_sql$;

/*
 * Refuses a history table that cannot take every base column, with the same errors the history
 * writer raises (src/sql_saga.c, CheckHistoryTupleCompatibility). Columns in without_columns are not
 * checked: they are not in the views being generated (the writer still checks them).
 */
-- No fixed search_path, so relation names in the messages read as the caller would write them
-- (ms, not public.ms), as in every sql_saga message. Every reference is schema-qualified.
CREATE OR REPLACE FUNCTION sql_saga.__internal_check_history_columns(table_oid oid, history_table_oid oid, without_columns name[] DEFAULT '{}')
RETURNS void
LANGUAGE plpgsql
STABLE
AS $__internal_check_history_columns$
DECLARE
    c record;
    rebuild_call text;
BEGIN
    FOR c IN
        SELECT ba.attname,
               sql_saga.__internal_column_type_sql(ba.atttypid, ba.atttypmod, ba.attcollation) AS base_type,
               ha.attname IS NOT NULL AS in_history,
               sql_saga.__internal_column_type_sql(ha.atttypid, ha.atttypmod, ha.attcollation) AS history_type
        FROM pg_catalog.pg_attribute AS ba
        LEFT JOIN pg_catalog.pg_attribute AS ha
               ON ha.attrelid = history_table_oid AND ha.attname = ba.attname AND ha.attnum > 0 AND NOT ha.attisdropped
        WHERE ba.attrelid = table_oid AND ba.attnum > 0 AND NOT ba.attisdropped
          AND ba.attname <> ALL (without_columns)
          AND (ha.attname IS NULL
               OR (ha.atttypid, ha.atttypmod, ha.attcollation) IS DISTINCT FROM (ba.atttypid, ba.atttypmod, ba.attcollation))
        ORDER BY ba.attnum
        LIMIT 1
    LOOP
        rebuild_call := pg_catalog.format('SELECT sql_saga.rebuild_system_versioning_views(%L);', sql_saga.__internal_qualified_name(table_oid));
        IF NOT c.in_history THEN
            RAISE EXCEPTION 'sql_saga: column % of base table % is missing from history table %',
                pg_catalog.quote_ident(c.attname), table_oid::pg_catalog.regclass, history_table_oid::pg_catalog.regclass
                USING ERRCODE = 'undefined_column',
                      HINT = pg_catalog.format('Run: ALTER TABLE %s ADD COLUMN %s %s; %s',
                                    sql_saga.__internal_qualified_name(history_table_oid), pg_catalog.quote_ident(c.attname), c.base_type, rebuild_call);
        END IF;
        RAISE EXCEPTION 'sql_saga: column % has type % in base table % but type % in history table %',
            pg_catalog.quote_ident(c.attname), c.base_type, table_oid::pg_catalog.regclass, c.history_type, history_table_oid::pg_catalog.regclass
            USING ERRCODE = 'datatype_mismatch',
                  HINT = pg_catalog.format('Run: ALTER TABLE %s ALTER COLUMN %s TYPE %s; %s',
                                sql_saga.__internal_qualified_name(history_table_oid), pg_catalog.quote_ident(c.attname), c.base_type, rebuild_call);
    END LOOP;
END;
$__internal_check_history_columns$;

/*
 * (Re)creates B_history_gated, B_with_history and the four as-of functions of a registered family
 * from the current columns. Callers check arguments and authorization and hold the family's lock.
 * Existing objects are dropped first (without cascading: a user object depending on them makes this
 * fail with PostgreSQL's dependency error). The drops are allowed by drop_protection and the new
 * definitions by health_checks only while sql_saga.rebuilding_system_versioning_views names this table.
 */
CREATE OR REPLACE FUNCTION sql_saga.__internal_refresh_system_time_columns_snapshot(table_oid oid)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog
AS $__internal_refresh_system_time_columns_snapshot$
    UPDATE sql_saga.system_time_era AS ste
    SET table_columns_snapshot = live.column_names
    FROM (
        SELECT pg_catalog.array_agg(a.attname ORDER BY a.attnum) AS column_names
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = table_oid
          AND a.attnum > 0
          AND NOT a.attisdropped
    ) AS live
    WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = table_oid;
$__internal_refresh_system_time_columns_snapshot$;

REVOKE ALL ON FUNCTION sql_saga.__internal_refresh_system_time_columns_snapshot(oid) FROM PUBLIC;

CREATE OR REPLACE FUNCTION sql_saga.__internal_system_versioning_query_functions(table_oid oid)
RETURNS TABLE(role text, identity text, argument_types text, body text)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_system_versioning_query_functions$
    SELECT q.role,
           q.identity,
           q.argument_types,
           pg_catalog.format(q.body_template, sv.view_schema_name, sv.view_table_name, e.range_column_name) AS body
    FROM sql_saga.system_versioning AS sv
    JOIN sql_saga.era AS e
      ON (e.table_schema, e.table_name, e.era_name) = (sv.table_schema, sv.table_name, sv.era_name)
    CROSS JOIN LATERAL (VALUES
        (1, 'as_of', sv.func_as_of, 'timestamp with time zone', 'SELECT * FROM %1$I.%2$I WHERE %3$I @> $1'),
        (2, 'between', sv.func_between, 'timestamp with time zone, timestamp with time zone', 'SELECT * FROM %1$I.%2$I WHERE %3$I && tstzrange($1, $2, ''[]'')'),
        (3, 'between_symmetric', sv.func_between_symmetric, 'timestamp with time zone, timestamp with time zone', 'SELECT * FROM %1$I.%2$I WHERE %3$I && tstzrange(least($1, $2), greatest($1, $2), ''[]'')'),
        (4, 'from_to', sv.func_from_to, 'timestamp with time zone, timestamp with time zone', 'SELECT * FROM %1$I.%2$I WHERE %3$I && tstzrange($1, $2)')
    ) AS q(ordinality, role, identity, argument_types, body_template)
    WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid = table_oid
    ORDER BY q.ordinality;
$__internal_system_versioning_query_functions$;

CREATE OR REPLACE FUNCTION sql_saga.__internal_rebuild_system_versioning_views(table_oid oid, without_columns name[] DEFAULT '{}')
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_rebuild_system_versioning_views$
DECLARE
    sv sql_saga.system_versioning;
    history_table_oid oid;
    table_owner name;
    range_column name;
    gated_view_name name;
    base_projection text;
    history_projection text;
    fn record;
    previous_flag text;
BEGIN
    SELECT s.* INTO sv
    FROM sql_saga.system_versioning AS s
    WHERE to_regclass(format('%I.%I', s.table_schema, s.table_name))::oid = table_oid;
    history_table_oid := to_regclass(format('%I.%I', sv.history_schema_name, sv.history_table_name))::oid;
    table_owner := (SELECT c.relowner::regrole::name FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid);
    range_column := (SELECT e.range_column_name FROM sql_saga.era AS e
                     WHERE (e.table_schema, e.table_name, e.era_name) = (sv.table_schema, sv.table_name, sv.era_name));
    gated_view_name := sql_saga.__internal_history_gated_name(sv.history_table_name);

    PERFORM sql_saga.__internal_check_history_columns(table_oid, history_table_oid, without_columns);

    -- The union of both tables' columns, by name: base columns in base order, then history-only ones.
    WITH u AS (
        SELECT coalesce(ba.attname, ha.attname) AS attname, ba.attnum AS base_attnum, ha.attnum AS history_attnum,
               ha.atttypid, ha.atttypmod, ha.attcollation
        FROM (SELECT a.* FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = table_oid AND a.attnum > 0 AND NOT a.attisdropped) AS ba
        FULL JOIN (SELECT a.* FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = history_table_oid AND a.attnum > 0 AND NOT a.attisdropped) AS ha
               ON ha.attname = ba.attname
        WHERE coalesce(ba.attname, ha.attname) <> ALL (without_columns)
    )
    SELECT string_agg(CASE WHEN u.base_attnum IS NOT NULL THEN quote_ident(u.attname)
                           ELSE format('NULL::%s AS %I', sql_saga.__internal_column_type_sql(u.atttypid, u.atttypmod, u.attcollation), u.attname)
                      END, ', ' ORDER BY u.base_attnum NULLS LAST, u.history_attnum),
           string_agg(quote_ident(u.attname), ', ' ORDER BY u.base_attnum NULLS LAST, u.history_attnum)
    INTO base_projection, history_projection
    FROM u;

    previous_flag := current_setting('sql_saga.rebuilding_system_versioning_views', true);
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', table_oid::text, true);
    BEGIN
        FOR fn IN
            SELECT u.f FROM unnest(ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]) AS u (f)
            WHERE to_regprocedure(u.f) IS NOT NULL
        LOOP
            EXECUTE format('DROP FUNCTION %s', fn.f::regprocedure);
        END LOOP;
        -- add_system_versioning calls this before the views exist.
        IF to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name)) IS NOT NULL THEN
            EXECUTE format('DROP VIEW %I.%I', sv.view_schema_name, sv.view_table_name);
        END IF;
        IF to_regclass(format('%I.%I', sv.history_schema_name, gated_view_name)) IS NOT NULL THEN
            EXECUTE format('DROP VIEW %I.%I', sv.history_schema_name, gated_view_name);
        END IF;

        EXECUTE format('CREATE VIEW %I.%I AS %s', sv.history_schema_name, gated_view_name,
            sql_saga.__internal_history_gated_view_sql(table_oid, history_table_oid, history_projection));
        EXECUTE format('ALTER VIEW %I.%I OWNER TO %I', sv.history_schema_name, gated_view_name, table_owner);
        EXECUTE format('GRANT SELECT ON TABLE %I.%I TO PUBLIC', sv.history_schema_name, gated_view_name);

        EXECUTE format($view$CREATE VIEW %1$I.%2$I WITH (security_invoker = on) AS
        SELECT %3$s FROM %4$s
        UNION ALL SELECT %5$s FROM %6$I.%7$I
        UNION ALL SELECT %5$s FROM %8$s
            WHERE pg_catalog.row_security_active(%9$L::regclass::oid)
              AND sql_saga.__internal_history_policies_synchronized(%9$L::regclass::oid, %10$L::regclass::oid)$view$,
            sv.view_schema_name, /* %1$I */
            sv.view_table_name, /* %2$I */
            base_projection, /* %3$s */
            sql_saga.__internal_qualified_name(table_oid), /* %4$s */
            history_projection, /* %5$s */
            sv.history_schema_name, /* %6$I */
            gated_view_name, /* %7$I */
            sql_saga.__internal_qualified_name(history_table_oid), /* %8$s */
            sql_saga.__internal_qualified_name(table_oid), /* %9$L */
            sql_saga.__internal_qualified_name(history_table_oid) /* %10$L */
        );
        EXECUTE format('ALTER VIEW %I.%I OWNER TO %I', sv.view_schema_name, sv.view_table_name, table_owner);
        EXECUTE format('GRANT SELECT ON TABLE %I.%I TO PUBLIC', sv.view_schema_name, sv.view_table_name);

        -- Each registered signature names a function; its body is fixed by its role.
        FOR fn IN
            SELECT q.identity AS f, q.body
            FROM sql_saga.__internal_system_versioning_query_functions(table_oid) AS q
        LOOP
            EXECUTE format('CREATE FUNCTION %s RETURNS SETOF %I.%I LANGUAGE sql STABLE AS %L',
                fn.f, sv.view_schema_name, sv.view_table_name,
                fn.body);
            EXECUTE format('ALTER FUNCTION %s OWNER TO %I', fn.f::regprocedure, table_owner);
            -- PUBLIC EXECUTE is explicit, because hardened databases revoke the default.
            EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO PUBLIC', fn.f::regprocedure);
        END LOOP;

        -- The fixed ACL is PUBLIC plus the owner. New objects can receive more through
        -- ALTER DEFAULT PRIVILEGES; take those away, so a rebuild always ends in the fixed state.
        FOR fn IN
            SELECT format('REVOKE ALL ON %s %s FROM %s',
                          CASE WHEN o.is_function THEN 'FUNCTION' ELSE 'TABLE' END, o.ident,
                          string_agg(DISTINCT quote_ident(x.grantee::regrole::text), ', ')) AS stmt
            FROM (SELECT false AS is_function, c.oid::regclass::text AS ident, c.relacl AS acl, c.relowner AS owner
                  FROM pg_catalog.pg_class AS c
                  WHERE c.oid IN (to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name)),
                                  to_regclass(format('%I.%I', sv.history_schema_name, gated_view_name)))
                  UNION ALL
                  SELECT true, p.oid::regprocedure::text, p.proacl, p.proowner
                  FROM pg_catalog.pg_proc AS p
                  WHERE p.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
                 ) AS o
            CROSS JOIN LATERAL aclexplode(o.acl) AS x
            WHERE x.grantee NOT IN (0, o.owner)
            GROUP BY o.is_function, o.ident
        LOOP
            EXECUTE fn.stmt;
        END LOOP;
    EXCEPTION WHEN OTHERS THEN
        PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_flag, ''), true);
        RAISE;
    END;
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_flag, ''), true);

    RAISE DEBUG 'sql_saga: generated SYSTEM VERSIONING views and query functions for table %', table_oid::regclass;
END;
$__internal_rebuild_system_versioning_views$;

CREATE OR REPLACE FUNCTION sql_saga.__internal_reconcile_system_versioning_family(
    table_oid regclass,
    without_columns name[] DEFAULT '{}')
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog
AS $__internal_reconcile_system_versioning_family$
DECLARE
    sv sql_saga.system_versioning;
    history_table_oid oid;
    table_owner name;
    acl record;
    needs_rebuild boolean;
    expected_columns integer;
    actual_columns integer;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'sql_saga: cannot reconcile a NULL table'
            USING ERRCODE = 'null_value_not_allowed',
                  HINT = 'Pass a registered base table to sql_saga.__internal_reconcile_system_versioning_family().';
    END IF;
    PERFORM sql_saga.__internal_serialize(table_oid);
    SELECT s.* INTO sv FROM sql_saga.system_versioning AS s
    WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', s.table_schema, s.table_name)) OPERATOR(pg_catalog.=) table_oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'sql_saga: table % does not have SYSTEM VERSIONING', table_oid
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    IF current_setting('sql_saga.rebuilding_system_versioning_views', true) OPERATOR(pg_catalog.=) table_oid::oid::text THEN
        PERFORM sql_saga.__internal_sync_history_policies(table_oid);
        PERFORM sql_saga.__internal_refresh_system_time_columns_snapshot(table_oid);
        RETURN;
    END IF;
    history_table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name, sv.history_table_name))::oid;
    IF history_table_oid IS NULL THEN
        RAISE EXCEPTION 'sql_saga: history table %.% registered for % does not exist',
            pg_catalog.quote_ident(sv.history_schema_name), pg_catalog.quote_ident(sv.history_table_name), table_oid
            USING ERRCODE = 'undefined_table', HINT = 'Restore the registered history table before retrying.';
    END IF;
    -- Preserve history-only columns outside canonical views. The public rebuild API calls the
    -- generator directly when an operator explicitly wants those columns included.
    SELECT coalesce(without_columns, '{}'::name[]) || coalesce(pg_catalog.array_agg(a.attname ORDER BY a.attnum), '{}'::name[])
    INTO without_columns
    FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = history_table_oid AND a.attnum > 0 AND NOT a.attisdropped
      AND NOT EXISTS (
          SELECT FROM pg_catalog.pg_attribute AS b
          WHERE b.attrelid = table_oid AND b.attnum > 0 AND NOT b.attisdropped
            AND b.attname OPERATOR(pg_catalog.=) a.attname)
      AND a.attname <> ALL (coalesce(without_columns, '{}'::name[]));
    BEGIN
      EXECUTE pg_catalog.format('LOCK TABLE %s, %s IN ACCESS EXCLUSIVE MODE', table_oid, history_table_oid::regclass);
      table_owner := (SELECT c.relowner::regrole::name FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid);
      IF (SELECT c.relowner::regrole::name FROM pg_catalog.pg_class AS c WHERE c.oid = history_table_oid) <> table_owner THEN
          EXECUTE pg_catalog.format('ALTER TABLE %s OWNER TO %I', history_table_oid::regclass, table_owner);
      END IF;
      IF NOT (SELECT c.relrowsecurity FROM pg_catalog.pg_class AS c WHERE c.oid = history_table_oid) THEN
          EXECUTE pg_catalog.format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', history_table_oid::regclass);
      END IF;
      FOR acl IN
        SELECT pg_catalog.format('REVOKE ALL ON TABLE %s FROM %s', history_table_oid::regclass,
                                 pg_catalog.string_agg(DISTINCT pg_catalog.quote_ident(x.grantee::regrole::text), ', ')) AS stmt
        FROM pg_catalog.pg_class AS c CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) AS x
        WHERE c.oid = history_table_oid AND x.grantee NOT IN (0, c.relowner) GROUP BY c.oid
      LOOP
          EXECUTE acl.stmt;
      END LOOP;
      IF NOT EXISTS (
          SELECT FROM pg_catalog.pg_class AS c
          CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) AS x
          WHERE c.oid = history_table_oid AND x.grantee = 0 AND x.privilege_type = 'SELECT') THEN
          EXECUTE pg_catalog.format('GRANT SELECT ON TABLE %s TO PUBLIC', history_table_oid::regclass);
      END IF;

      SELECT count(*) INTO expected_columns
      FROM (
          SELECT a.attname FROM pg_catalog.pg_attribute AS a
          WHERE a.attrelid = table_oid AND a.attnum > 0 AND NOT a.attisdropped
            AND a.attname <> ALL (coalesce(without_columns, '{}'))
          UNION
          SELECT a.attname FROM pg_catalog.pg_attribute AS a
          WHERE a.attrelid = history_table_oid AND a.attnum > 0 AND NOT a.attisdropped
            AND a.attname <> ALL (coalesce(without_columns, '{}'))
      ) AS expected;
      SELECT count(*) INTO actual_columns
      FROM pg_catalog.pg_attribute AS a
      WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.view_schema_name, sv.view_table_name))
        AND a.attnum > 0 AND NOT a.attisdropped;
      needs_rebuild := pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name,
                           sql_saga.__internal_history_gated_name(sv.history_table_name))) IS NULL
          OR pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.view_schema_name, sv.view_table_name)) IS NULL
          OR expected_columns IS DISTINCT FROM actual_columns
          OR NOT EXISTS (
              SELECT FROM pg_catalog.pg_class AS c
              WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.view_schema_name, sv.view_table_name))
                AND 'security_invoker=on' = ANY (coalesce(c.reloptions, '{}')))
          OR EXISTS (
              SELECT FROM unnest(ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]) AS f(fn)
              WHERE pg_catalog.to_regprocedure(f.fn) IS NULL);
      IF needs_rebuild THEN
          PERFORM sql_saga.__internal_rebuild_system_versioning_views(table_oid, coalesce(without_columns, '{}'));
          PERFORM sql_saga.__internal_refresh_system_time_columns_snapshot(table_oid);
      END IF;
      PERFORM sql_saga.__internal_sync_history_policies(table_oid);
    END;
END;
$__internal_reconcile_system_versioning_family$;

-- 1.2 adds a trailing history_table_must_exist parameter (DEFAULT false), so every 1.1 call form
-- still resolves. A changed signature needs DROP and CREATE. The new function gets what a fresh 1.2
-- install gets; customised privileges on the 1.1 function were refused at the top of this script.
DROP FUNCTION sql_saga.add_system_versioning(regclass, name, name, name, name, name, name);

CREATE FUNCTION sql_saga.add_system_versioning(
    table_oid regclass,
    history_table_name name DEFAULT NULL,
    view_name name DEFAULT NULL,
    function_as_of_name name DEFAULT NULL,
    function_between_name name DEFAULT NULL,
    function_between_symmetric_name name DEFAULT NULL,
    function_from_to_name name DEFAULT NULL,
    history_table_must_exist boolean DEFAULT false)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    table_owner regrole;
    persistence "char";
    kind "char";
    era_row sql_saga.era;
    history_table_id oid;
    history_row_count bigint;
    other_schema name;
    other_family record;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT n.nspname, c.relname, c.relowner, c.relpersistence, c.relkind
    INTO table_schema, table_name, table_owner, persistence, kind
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    IF EXISTS (SELECT 1 FROM sql_saga.system_versioning sv WHERE (sv.table_schema, sv.table_name) = (table_schema, table_name)) THEN
        RAISE EXCEPTION 'table already has SYSTEM VERSIONING';
    END IF;

    IF kind <> 'r' THEN
        IF kind = 'p' THEN
            RAISE EXCEPTION 'partitioned tables are not supported yet';
        END IF;
        RAISE EXCEPTION 'relation % is not a table', table_oid;
    END IF;

    IF persistence <> 'p' THEN
        RAISE EXCEPTION 'table "%" must be persistent', sql_saga.__internal_display_name(table_oid);
    END IF;

    /*
     * Validate an existing history table before anything is created, so a refusal leaves no trace:
     * missing (with history_table_must_exist), already another family's history, or carrying an era.
     */
    history_table_name := coalesce(history_table_name, sql_saga.__internal_make_name(ARRAY[table_name], 'history'));

    SELECT c.oid INTO history_table_id
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE (n.nspname, c.relname) = (table_schema, history_table_name);

    IF history_table_id IS NULL THEN
        IF history_table_must_exist THEN
            SELECT n.nspname INTO other_schema
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            WHERE c.relname = history_table_name AND c.relkind IN ('r', 'p')
              AND n.nspname <> table_schema
              AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
            ORDER BY n.nspname
            LIMIT 1;
            IF other_schema IS NOT NULL THEN
                RAISE EXCEPTION 'history table "%" does not exist, and history_table_must_exist is true',
                    sql_saga.__internal_display_name(format('%I.%I', table_schema, history_table_name))
                USING ERRCODE = 'undefined_table',
                      HINT = format('Table "%s" has that name in another schema. Move it first: ALTER TABLE %I.%I SET SCHEMA %I;',
                          sql_saga.__internal_display_name(format('%I.%I', other_schema, history_table_name)),
                          other_schema, history_table_name, table_schema);
            END IF;
            RAISE EXCEPTION 'history table "%" does not exist, and history_table_must_exist is true',
                sql_saga.__internal_display_name(format('%I.%I', table_schema, history_table_name))
            USING ERRCODE = 'undefined_table';
        END IF;
    ELSE
        SELECT sv.table_schema, sv.table_name INTO other_family
        FROM sql_saga.system_versioning AS sv
        WHERE (sv.history_schema_name, sv.history_table_name) = (table_schema, history_table_name);
        IF FOUND THEN
            RAISE EXCEPTION 'table "%" is already the history table of system-versioned table "%"',
                sql_saga.__internal_display_name(history_table_id::regclass),
                sql_saga.__internal_display_name(format('%I.%I', other_family.table_schema, other_family.table_name))
            USING ERRCODE = 'object_in_use';
        END IF;
        IF EXISTS (SELECT 1 FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name) = (table_schema, history_table_name)) THEN
            RAISE EXCEPTION 'history tables for SYSTEM VERSIONING cannot have eras';
        END IF;
        /*
         * Compare with the base table as it will be once versioned: the system time era adds
         * system_valid_range when the base lacks it. Checked here, before any change.
         */
        IF EXISTS (
            WITH
            L (attname, atttypid, atttypmod, attcollation) AS (
                SELECT a.attname, a.atttypid, a.atttypmod, a.attcollation FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = table_oid AND NOT a.attisdropped
                UNION ALL
                SELECT 'system_valid_range'::name, 'pg_catalog.tstzrange'::pg_catalog.regtype::oid, -1, 0::oid
                WHERE NOT EXISTS (SELECT FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name, e.era_name) = (table_schema, table_name, 'system_time'))
                  AND NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_oid, 'system_valid_range') AND NOT a.attisdropped)),
            R (attname, atttypid, atttypmod, attcollation) AS (SELECT a.attname, a.atttypid, a.atttypmod, a.attcollation FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = history_table_id AND NOT a.attisdropped)
            SELECT FROM L NATURAL FULL JOIN R WHERE L.attname IS NULL OR R.attname IS NULL)
        THEN
            RAISE EXCEPTION 'base table "%" and history table "%" are not compatible', sql_saga.__internal_display_name(table_oid), sql_saga.__internal_display_name(history_table_id::regclass);
        END IF;
    END IF;

    SELECT e.*
    INTO era_row
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (table_schema, table_name, 'system_time');

    IF NOT FOUND THEN
        PERFORM sql_saga.__internal_add_system_time_era(table_oid);
        -- Re-fetch era row
        SELECT e.* INTO era_row FROM sql_saga.era e WHERE (e.table_schema, e.table_name, e.era_name) = (table_schema, table_name, 'system_time');
    END IF;

    view_name := coalesce(view_name, sql_saga.__internal_make_name(ARRAY[table_name], 'with_history'));
    function_as_of_name := coalesce(function_as_of_name, sql_saga.__internal_make_name(ARRAY[table_name], '_as_of'));
    function_between_name := coalesce(function_between_name, sql_saga.__internal_make_name(ARRAY[table_name], '_between'));
    function_between_symmetric_name := coalesce(function_between_symmetric_name, sql_saga.__internal_make_name(ARRAY[table_name], '_between_symmetric'));
    function_from_to_name := coalesce(function_from_to_name, sql_saga.__internal_make_name(ARRAY[table_name], '_from_to'));

    IF history_table_id IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s OWNER TO %I', history_table_id::regclass /* %s */, table_owner /* %I */);
        EXECUTE format('SELECT count(*) FROM %s', history_table_id::regclass /* %s */) INTO history_row_count;
        RAISE NOTICE 'sql_saga: Adopted existing table "%" (% rows) as the history table of system-versioned table "%"',
            sql_saga.__internal_display_name(history_table_id::regclass), history_row_count,
            sql_saga.__internal_display_name(table_oid);
    ELSE
        EXECUTE format('CREATE TABLE %1$I.%2$I (LIKE %1$I.%3$I)', table_schema /* %1$I */, history_table_name /* %2$I */, table_name /* %3$I */);
        history_table_id := format('%I.%I', table_schema /* %I */, history_table_name /* %I */)::regclass;
        EXECUTE format('ALTER TABLE %1$I.%2$I OWNER TO %3$I', table_schema /* %1$I */, history_table_name /* %2$I */, table_owner /* %3$I */);
        RAISE NOTICE 'sql_saga: Created history table "%" for system-versioned table "%"',
            sql_saga.__internal_notice_name(table_schema, history_table_name), sql_saga.__internal_notice_name(table_schema, table_name);
        RAISE DEBUG 'history table "%" created for "%", be sure to index it properly', history_table_id::regclass, table_oid;
    END IF;

    /*
     * Access-time authority (tmp/design_final.md): the history objects are granted to PUBLIC and
     * hold no copy of the base table's ACL. Callers are checked against the base when they read:
     *   - B_history_gated (owner-rights) raises 42501 unless the caller may SELECT B, and serves
     *     callers that B's row-level security does not apply to;
     *   - B_history is read as the caller when B's RLS applies, filtered by the copies of B's
     *     policies (sql_saga.__internal_sync_history_policies), after checking they are in sync.
     */
    EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', history_table_id::regclass);
    EXECUTE format('GRANT SELECT ON TABLE %s TO PUBLIC', history_table_id::regclass);

    -- Registered first: the generator reads the family's names from the catalog.
    INSERT INTO sql_saga.system_versioning (table_schema, table_name, era_name, history_schema_name, history_table_name, view_schema_name, view_table_name, func_as_of, func_between, func_between_symmetric, func_from_to)
    VALUES (
        table_schema, table_name, 'system_time',
        table_schema, history_table_name,
        table_schema, view_name,
        format('%I.%I(timestamp with time zone)', table_schema /* %I */, function_as_of_name /* %I */),
        format('%I.%I(timestamp with time zone,timestamp with time zone)', table_schema /* %I */, function_between_name /* %I */),
        format('%I.%I(timestamp with time zone,timestamp with time zone)', table_schema /* %I */, function_between_symmetric_name /* %I */),
        format('%I.%I(timestamp with time zone,timestamp with time zone)', table_schema /* %I */, function_from_to_name /* %I */)
    );

    -- B_history_gated, B_with_history and the as-of functions: the same generator as
    -- rebuild_system_versioning_views, so a rebuild of an unchanged family reproduces them exactly.
    PERFORM sql_saga.__internal_reconcile_system_versioning_family(table_oid, '{}');
END;
$function$;

COMMENT ON FUNCTION sql_saga.add_system_versioning(regclass, name, name, name, name, name, name, boolean) IS
'Adds system versioning to a table, creating a history table and triggers to automatically track all data changes. An existing table named history_table_name in the table''s schema is adopted as the history (reported by a NOTICE); history_table_must_exist => true requires it, as when moving a system-versioned table.';

/*
 * The as-of functions of a versioned table are executable by PUBLIC (tmp/design_final.md section 5).
 * add_system_versioning grants it explicitly, because hardened databases revoke EXECUTE from PUBLIC by
 * default. pg_dump writes no GRANT for a function ACL equal to the default, so a pg_restore --clean into
 * a database where those default privileges already apply recreates the functions owner-only. The
 * catalog row is loaded after the functions exist, so granting when it is inserted keeps the rule after
 * any restore. Runs as the inserting role: add_system_versioning (the extension owner) or pg_restore.
 */
CREATE OR REPLACE FUNCTION sql_saga.__internal_grant_query_functions()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_grant_query_functions$
DECLARE
    query_function regprocedure;
BEGIN
    FOR query_function IN
        SELECT to_regprocedure(u.signature)
        FROM unnest(ARRAY[NEW.func_as_of, NEW.func_between, NEW.func_between_symmetric, NEW.func_from_to]) AS u (signature)
        WHERE to_regprocedure(u.signature) IS NOT NULL
    LOOP
        EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO PUBLIC', query_function /* %s */);
    END LOOP;
    EXECUTE format('GRANT SELECT ON TABLE %I.%I TO PUBLIC', NEW.history_schema_name, NEW.history_table_name);
    RETURN NULL;
END;
$__internal_grant_query_functions$;

/*
 * Manual schema changes (tmp/design_manual_schema_changes.md): after the owner changed the base and
 * history tables by hand, this recreates the views and as-of functions from the current columns.
 *
 * SECURITY INVOKER, like sync_history_policies: the caller must be a member of the base table's
 * owner role (a SECURITY DEFINER function sees its own owner as current_user and could not tell),
 * and the replacement runs with the caller's rights over the objects that role owns.
 * No fixed search_path, so relation names in messages read as the caller would write them; every
 * reference in the body is schema-qualified. The generator itself runs with a fixed search_path.
 */
CREATE OR REPLACE FUNCTION sql_saga.rebuild_system_versioning_views(
    table_oid regclass,
    without_columns name[] DEFAULT '{}')
RETURNS void
LANGUAGE plpgsql
AS $rebuild_system_versioning_views$
DECLARE
    sv sql_saga.system_versioning;
    history_table_oid oid;
    range_column name;
    excluded name;
    previous_rebuild_flag text;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT s.* INTO sv
    FROM sql_saga.system_versioning AS s
    WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', s.table_schema, s.table_name)) OPERATOR(pg_catalog.=) table_oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'table % does not have SYSTEM VERSIONING', table_oid;
    END IF;

    IF NOT pg_catalog.pg_has_role((SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid OPERATOR(pg_catalog.=) table_oid), 'USAGE') THEN
        RAISE EXCEPTION 'must be owner of table %', table_oid USING ERRCODE = 'insufficient_privilege';
    END IF;

    history_table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name, sv.history_table_name))::pg_catalog.oid;
    range_column := (SELECT e.range_column_name FROM sql_saga.era AS e
                     WHERE (e.table_schema, e.table_name, e.era_name) OPERATOR(pg_catalog.=) (sv.table_schema, sv.table_name, sv.era_name));

    without_columns := coalesce(without_columns, '{}');
    IF pg_catalog.array_position(without_columns, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: without_columns must not contain NULL';
    END IF;
    FOR excluded IN
        SELECT u.c FROM pg_catalog.unnest(without_columns) WITH ORDINALITY AS u (c, n)
        GROUP BY u.c HAVING pg_catalog.count(*) OPERATOR(pg_catalog.>) 1 ORDER BY pg_catalog.min(u.n)
    LOOP
        RAISE EXCEPTION 'sql_saga: column % is listed more than once in without_columns', pg_catalog.quote_ident(excluded);
    END LOOP;
    FOR excluded IN
        SELECT u.c FROM pg_catalog.unnest(without_columns) WITH ORDINALITY AS u (c, n)
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                          WHERE a.attrelid IN (table_oid, history_table_oid) AND a.attname OPERATOR(pg_catalog.=) u.c
                            AND a.attnum OPERATOR(pg_catalog.>) 0 AND NOT a.attisdropped)
        ORDER BY u.n
    LOOP
        RAISE EXCEPTION 'sql_saga: column % does not exist in either % or %',
            pg_catalog.quote_ident(excluded), table_oid, history_table_oid::pg_catalog.regclass;
    END LOOP;
    IF range_column OPERATOR(pg_catalog.=) ANY (without_columns) THEN
        RAISE EXCEPTION 'sql_saga: cannot exclude system time column % from SYSTEM VERSIONING views for table %',
            pg_catalog.quote_ident(range_column), table_oid;
    END IF;

    -- Checked here, in the caller's search_path, so the error names relations as the writer's does.
    PERFORM sql_saga.__internal_check_history_columns(table_oid, history_table_oid, without_columns);

    PERFORM sql_saga.__internal_rebuild_system_versioning_views(table_oid, without_columns);

    -- Use the existing authorized reconciler as the private refresh gateway.
    previous_rebuild_flag := current_setting('sql_saga.rebuilding_system_versioning_views', true);
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', table_oid::oid::text, true);
    BEGIN
        PERFORM sql_saga.__internal_reconcile_system_versioning_family(table_oid, without_columns);
    EXCEPTION WHEN OTHERS THEN
        PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_rebuild_flag, ''), true);
        RAISE;
    END;
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_rebuild_flag, ''), true);

    RAISE NOTICE 'sql_saga: Rebuilt SYSTEM VERSIONING views and query functions for table %', table_oid;
END;
$rebuild_system_versioning_views$;

COMMENT ON FUNCTION sql_saga.rebuild_system_versioning_views(regclass, name[]) IS
'Rebuilds the views and as-of query functions of a system-versioned table from the current base and history columns. Columns are matched by name; history may have additional columns. without_columns temporarily omits named columns so dependent columns can be changed manually.';
CREATE OR REPLACE FUNCTION sql_saga.__internal_generate_pk_consistency_constraints(
    p_unique_key_name name,
    p_natural_key_columns name[],
    p_mutually_exclusive_columns name[],
    p_primary_key_column name
)
RETURNS jsonb -- { "cmds": text[], "names": name[] }
LANGUAGE plpgsql AS
$function$
#variable_conflict use_variable
DECLARE
    v_constraint_cmds text[] := '{}';
    v_constraint_names name[] := '{}';
    v_constraint_name name;
    v_constraint_sql text;
    v_withs text[];
BEGIN
    IF p_mutually_exclusive_columns IS NULL THEN
        v_constraint_name := p_unique_key_name || '_pk_consistency_excl';
        SELECT array_agg(format('%I WITH =', col)) INTO v_withs FROM unnest(p_natural_key_columns) AS col;
        v_withs := v_withs || format('%I WITH <>', p_primary_key_column);
        v_constraint_sql := format('ADD CONSTRAINT %I EXCLUDE USING gist (%s)', v_constraint_name, array_to_string(v_withs, ', '));
        v_constraint_cmds := array_append(v_constraint_cmds, v_constraint_sql);
        v_constraint_names := array_append(v_constraint_names, v_constraint_name);
    ELSE
        DECLARE
            non_xor_cols name[];
            xor_col name;
            where_clause_partial text;
        BEGIN
            non_xor_cols := ARRAY(SELECT c FROM unnest(p_natural_key_columns) AS c WHERE c <> ALL(p_mutually_exclusive_columns));
            FOREACH xor_col IN ARRAY p_mutually_exclusive_columns LOOP
                v_constraint_name := sql_saga.__internal_make_name(ARRAY[p_unique_key_name, xor_col, 'pk_consistency_excl']);
                SELECT array_agg(format('%I WITH =', col)) INTO v_withs FROM unnest(non_xor_cols || xor_col) AS col;
                v_withs := v_withs || format('%I WITH <>', p_primary_key_column);
                where_clause_partial := format('WHERE (%I IS NOT NULL AND (%s))', xor_col, (SELECT string_agg(format('%I IS NULL', other_col), ' AND ') FROM unnest(p_mutually_exclusive_columns) AS other_col WHERE other_col <> xor_col));
                v_constraint_sql := format('ADD CONSTRAINT %I EXCLUDE USING gist (%s) %s', v_constraint_name, array_to_string(v_withs, ', '), where_clause_partial);
                v_constraint_cmds := array_append(v_constraint_cmds, v_constraint_sql);
                v_constraint_names := array_append(v_constraint_names, v_constraint_name);
            END LOOP;
        END;
    END IF;

    RETURN jsonb_build_object('cmds', v_constraint_cmds, 'names', v_constraint_names);
END;
$function$;

CREATE OR REPLACE FUNCTION sql_saga.add_unique_key(
        table_oid regclass,
        column_names name[],
        era_name name DEFAULT 'valid',
        key_type sql_saga.unique_key_type DEFAULT 'natural',
        enforce_consistency_with_primary_key boolean DEFAULT NULL,
        unique_key_name name DEFAULT NULL,
        unique_constraint name DEFAULT NULL,
        exclude_constraint name DEFAULT NULL,
        predicate text DEFAULT NULL,
        mutually_exclusive_columns name[] DEFAULT NULL)
 RETURNS name
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    era_row sql_saga.era;
    column_attnums smallint[];
    era_attnums smallint[];
    idx integer;
    constraint_record record;
    pass integer;
    sql text;
    alter_cmds text[];
    v_unique_constraint_found boolean := false;
    v_exclude_constraint_found boolean := false;
    v_check_constraint name;
    unique_sql text;
    exclude_sql text;
    where_clause text;

    v_pk_consistency_constraint_names name[];
    v_enforce_consistency_original boolean := enforce_consistency_with_primary_key;
    v_stable_cols_to_store name[];
BEGIN
    RAISE DEBUG 'add_unique_key entry: table_oid=%, column_names=%, era_name=%, key_type=%', table_oid, column_names, era_name, key_type;
    -- If the parameter is NULL (the default), set it to true for natural keys and false otherwise.
    IF enforce_consistency_with_primary_key IS NULL THEN
        enforce_consistency_with_primary_key := (key_type = 'natural');
    END IF;

    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT p.*
    INTO era_row
    FROM sql_saga.era AS p
    WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'era "%" does not exist', era_name;
    END IF;

    -- For metadata storage, we use the exact column list provided by the user,
    -- as this is what symmetric drop functions will expect. The native unique
    -- constraint will include the temporal column, but our metadata only tracks
    -- the stable part of the key. An explicit check below ensures the user does
    -- not include the range column here.
    v_stable_cols_to_store := add_unique_key.column_names;

    -- This is no longer used by native temporal keys, but is still needed for XOR key logic on predicated keys.
    IF era_row.valid_from_column_name IS NOT NULL THEN
        era_attnums := ARRAY[
            (SELECT a.attnum FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_oid, era_row.valid_from_column_name)),
            (SELECT a.attnum FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_oid, era_row.valid_until_column_name))
        ];
    END IF;

    /* Get attnums from column names */
    SELECT array_agg(a.attnum ORDER BY n.ordinality)
    INTO column_attnums
    FROM unnest(column_names) WITH ORDINALITY AS n (name, ordinality)
    LEFT JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (table_oid, n.name);

    /* System columns are not allowed */
    IF 0 > ANY (column_attnums) THEN
        RAISE EXCEPTION 'index creation on system columns is not supported';
    END IF;

    /* Report if any columns weren't found */
    idx := array_position(column_attnums, NULL);
    IF idx IS NOT NULL THEN
        RAISE EXCEPTION 'column "%" does not exist', column_names[idx];
    END IF;

    /* Make sure the range column isn't also in the normal columns */
    IF era_row.range_column_name = ANY (column_names) THEN
        RAISE EXCEPTION 'range column "%" cannot also be part of the key columns', era_row.range_column_name;
    END IF;

    /*
     * Columns belonging to a SYSTEM_TIME period are not allowed in a UNIQUE
     * key. SQL:2016 11.7 SR 5)b)
     */
    IF EXISTS (
        SELECT FROM sql_saga.era AS e
        WHERE (e.table_schema, e.table_name, e.era_name) = (era_row.table_schema, era_row.table_name, 'system_time')
          AND ARRAY[e.valid_from_column_name, e.valid_until_column_name] && column_names)
    THEN
        RAISE EXCEPTION 'columns in era for SYSTEM_TIME are not allowed in UNIQUE keys';
    END IF;

    -- Validate parameter combinations
    IF key_type = 'predicated' AND predicate IS NULL THEN
        RAISE EXCEPTION 'a predicate must be provided when key_type is ''predicated''';
    END IF;

    IF key_type IN ('primary', 'natural') AND predicate IS NOT NULL THEN
        RAISE EXCEPTION 'a predicate can only be provided when key_type is ''predicated''';
    END IF;

    IF mutually_exclusive_columns IS NOT NULL THEN
        IF NOT column_names @> mutually_exclusive_columns THEN
            RAISE EXCEPTION 'all mutually_exclusive_columns must be present in column_names';
        END IF;
        IF key_type <> 'natural' THEN
            RAISE EXCEPTION 'mutually_exclusive_columns can only be used with key_type = ''natural''';
        END IF;
    END IF;

    -- This check is now only relevant if the user explicitly provides the parameter.
    IF enforce_consistency_with_primary_key AND key_type <> 'natural' THEN
        RAISE EXCEPTION 'enforce_consistency_with_primary_key can only be used with key_type = ''natural''';
    END IF;

    where_clause := CASE WHEN predicate IS NOT NULL THEN format(' WHERE (%s)', predicate /* %s */) ELSE '' END;

    IF key_type IN ('primary', 'natural') THEN
        -- When creating a primary key, validate that the table schema is compatible with SCD Type 2 history.
        IF key_type = 'primary' THEN
            DECLARE
                identity_column_name name;
            BEGIN
                -- Check for a GENERATED ALWAYS identity column first, as it's a more specific error.
                SELECT a.attname
                INTO identity_column_name
                FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = table_oid
                AND a.attidentity = 'a'; -- 'a' = always, 'd' = by default

                IF identity_column_name IS NOT NULL THEN
                    RAISE EXCEPTION 'table "%" has a GENERATED ALWAYS AS IDENTITY column ("%"); this is incompatible with SCD Type 2 history as it prevents inserting new historical versions of an entity', sql_saga.__internal_display_name(table_oid), identity_column_name;
                END IF;

                -- Check for a simple primary key that does not include the temporal columns.
                DECLARE
                    pk_columns name[];
                BEGIN
                    SELECT
                        array_agg(a.attname ORDER BY u.ord)
                    INTO
                        pk_columns
                    FROM
                        pg_catalog.pg_constraint c
                        JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE
                        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum
                    WHERE
                        c.conrelid = table_oid AND c.contype = 'p';

                    IF pk_columns IS NOT NULL AND NOT (pk_columns @> ARRAY[era_row.range_column_name]) THEN
                        RAISE EXCEPTION 'table "%" has a simple PRIMARY KEY that does not include the temporal range column ("%"); this is incompatible with SCD Type 2 history', sql_saga.__internal_display_name(table_oid), era_row.range_column_name;
                    END IF;
                END;
            END;
        END IF;
        /* If we were given a unique constraint to use, look it up and make sure it matches */
        -- For primary and natural keys, we now use a native temporal constraint.
        -- These constraints must be NOT DEFERRABLE (the default) to be referenceable by
        -- native temporal foreign keys. The with_temporary_temporal_gaps execution strategy
        -- (DELETE->UPDATE->INSERT) avoids creating overlaps that would violate this constraint.
        SELECT format('%s (%s, %I WITHOUT OVERLAPS)',
            CASE WHEN key_type = 'primary' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END, /* %s */
            string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality), /* %s */
            era_row.range_column_name /* %I */
        )
        INTO unique_sql
        FROM unnest(column_names) WITH ORDINALITY AS u (column_name, ordinality);

        IF unique_constraint IS NULL AND key_type = 'primary' THEN
            -- If this is a primary key and no name was given, check if one already exists.
            SELECT c.conname INTO unique_constraint
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.contype) = (table_oid, 'p');
        END IF;

        IF unique_constraint IS NOT NULL THEN
            SELECT c.oid, c.contype, c.condeferrable, c.condeferred, c.conkey
            INTO constraint_record
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (table_oid, unique_constraint);

            IF FOUND THEN
                v_unique_constraint_found := true;

                IF key_type = 'primary' AND constraint_record.contype <> 'p' THEN
                    RAISE EXCEPTION 'constraint "%" is not a PRIMARY KEY', unique_constraint;
                ELSIF key_type = 'natural' AND constraint_record.contype NOT IN ('p', 'u') THEN
                    RAISE EXCEPTION 'constraint "%" is not a PRIMARY KEY or UNIQUE KEY', unique_constraint;
                END IF;

                -- PostgreSQL 18 requirement: WITHOUT OVERLAPS constraints must be NOT DEFERRABLE
                -- to be referenceable by native temporal FKs (FOREIGN KEY ... PERIOD).
                -- Users migrating from trigger-based FKs may have DEFERRABLE PKs which must be changed.
                -- The with_temporary_temporal_gaps strategy (DELETE→UPDATE→INSERT) creates temporary
                -- gaps (tolerated by DEFERRABLE FKs) rather than overlaps (which would violate this constraint).
                DECLARE
                    unique_sql_nulls_not_distinct text;
                    actual_def text;
                BEGIN
                    -- Generate the NULLS NOT DISTINCT variant for comparison
                    SELECT format('%s (%s, %I WITHOUT OVERLAPS)',
                        CASE WHEN key_type = 'primary' THEN 'PRIMARY KEY NULLS NOT DISTINCT' ELSE 'UNIQUE NULLS NOT DISTINCT' END,
                        string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality),
                        era_row.range_column_name
                    )
                    INTO unique_sql_nulls_not_distinct
                    FROM unnest(column_names) WITH ORDINALITY AS u (column_name, ordinality);

                    actual_def := pg_catalog.pg_get_constraintdef(constraint_record.oid);

                    -- Check if constraint matches either traditional or NULLS NOT DISTINCT format
                    IF actual_def <> unique_sql AND actual_def <> unique_sql_nulls_not_distinct THEN
                        RAISE EXCEPTION E'constraint "%" does not match expected format.\nExpected (traditional): "%"\nExpected (NULLS NOT DISTINCT): "%"\nFound: "%"\n\nPostgreSQL 18 Temporal FK Requirement:\nUnique constraints with WITHOUT OVERLAPS must be NOT DEFERRABLE to be referenceable by native temporal foreign keys. The DELETE→UPDATE→INSERT execution strategy avoids overlaps while tolerating temporary gaps through DEFERRABLE FK checks.\n\nSee AGENTS.md section "PostgreSQL 18 Temporal Constraints and Foreign Keys" for details.',
                            unique_constraint,
                            unique_sql,
                            unique_sql_nulls_not_distinct,
                            actual_def;
                    END IF;
                END;

                /* Looks good, let's use it. */
            END IF;
        END IF;
    ELSIF key_type = 'predicated' THEN
        -- For predicated keys, we don't create a UNIQUE constraint/index.
        -- Uniqueness is enforced by the EXCLUDE constraint.
        -- We can clear unique_sql as it's not applicable.
        unique_sql := NULL;
    END IF;

    -- Native temporal constraints (WITHOUT OVERLAPS) replace exclusion constraints
    -- for primary and natural keys. Predicated keys still require an exclusion constraint.
    IF key_type = 'predicated' THEN
        DECLARE
            withs text[];
        BEGIN
            SELECT array_agg(format('%I WITH =', column_name /* %I */) ORDER BY n.ordinality)
            INTO withs
            FROM unnest(column_names) WITH ORDINALITY AS n (column_name, ordinality);

            withs := withs || format('%I WITH &&',
                era_row.range_column_name /* %I */
            );

            exclude_sql := format('EXCLUDE USING gist (%s)%s DEFERRABLE', array_to_string(withs, ', ') /* %s */, where_clause /* %s */);
        END;

        IF exclude_constraint IS NOT NULL THEN
            SELECT c.oid, c.contype, c.condeferrable, c.condeferred, pg_catalog.pg_get_constraintdef(c.oid) AS definition
            INTO constraint_record
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (table_oid, exclude_constraint);

            IF FOUND THEN
                v_exclude_constraint_found := true;

                IF constraint_record.contype <> 'x' THEN
                    RAISE EXCEPTION 'constraint "%" is not an EXCLUDE constraint', exclude_constraint;
                END IF;

                IF NOT constraint_record.condeferrable THEN
                    RAISE EXCEPTION 'constraint "%" must be DEFERRABLE', exclude_constraint;
                END IF;

                IF constraint_record.condeferred THEN
                    RAISE EXCEPTION 'constraint "%" must be INITIALLY IMMEDIATE', exclude_constraint;
                END IF;

                IF constraint_record.definition <> exclude_sql THEN
                    RAISE EXCEPTION 'constraint "%" does not match. Expected definition: "%", but found definition: "%".',
                        exclude_constraint,
                        exclude_sql,
                        constraint_record.definition;
                END IF;

                /* Looks good, let's use it. */
            END IF;
        END IF;
    END IF;

    /*
     * Generate a name for the unique constraint.  We don't have to worry about
     * concurrency here because all period ddl commands lock the periods table.
     */
    IF unique_key_name IS NULL THEN
        unique_key_name := sql_saga.__internal_make_name(
            ARRAY[(SELECT c.relname FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid)]
                || column_names
                || ARRAY[era_name]);
    END IF;
    pass := 0;
    WHILE EXISTS (
       SELECT FROM sql_saga.unique_keys AS uk
       WHERE uk.unique_key_name = unique_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END)
    LOOP
       pass := pass + 1;
    END LOOP;
    unique_key_name := unique_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;

    /* Time to make the underlying constraints */
    IF key_type = 'primary' THEN
        SELECT array_agg(format('ALTER COLUMN %I SET NOT NULL', u.column_name))
        INTO alter_cmds
        FROM unnest(column_names) WITH ORDINALITY AS u (column_name, ordinality);
    ELSE
        alter_cmds := '{}';
    END IF;

    IF key_type IN ('primary', 'natural') THEN
        -- For XOR keys, we don't create a single overarching UNIQUE constraint.
        -- Uniqueness is handled by the partial indexes.
        IF mutually_exclusive_columns IS NULL THEN
            IF NOT v_unique_constraint_found THEN
                IF unique_constraint IS NULL THEN
                    -- If this is a primary key, Postgres will name it automatically.
                    -- For natural keys, we'll generate a name for consistency.
                    IF key_type = 'natural' THEN
                        unique_constraint := unique_key_name || '_uniq';
                        alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(unique_constraint) || ' ' || unique_sql);
                    ELSE
                        alter_cmds := alter_cmds || ('ADD ' || unique_sql);
                    END IF;
                ELSE
                     alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(unique_constraint) || ' ' || unique_sql);
                END IF;
            END IF;
        END IF;
    ELSIF key_type = 'predicated' THEN
        -- For predicated keys, uniqueness is handled by the EXCLUDE constraint, not a UNIQUE index.
        -- No action needed here for unique_sql.
    END IF;

    -- For XOR keys, we create partial exclusion constraints instead of one broad one.
    IF mutually_exclusive_columns IS NULL THEN
        -- Add the exclusion constraint only for predicated keys.
        IF key_type = 'predicated' AND NOT v_exclude_constraint_found THEN
            IF exclude_constraint IS NULL THEN
                exclude_constraint := unique_key_name || '_excl';
            END IF;
            alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(exclude_constraint) || ' ' || exclude_sql);
        END IF;
    ELSE -- mutually_exclusive_columns IS NOT NULL (XOR keys)
        BEGIN
            -- XOR keys now use a single UNIQUE constraint with NULLS NOT DISTINCT
            -- This replaces the complex CHECK + partial indexes + partial EXCLUDE constraints

            -- For XOR keys, we use the unified unique constraint approach
            IF unique_constraint IS NULL THEN
                unique_constraint := unique_key_name;
            END IF;

            -- Generate UNIQUE constraint with NULLS NOT DISTINCT to handle XOR columns
            unique_sql := format('UNIQUE NULLS NOT DISTINCT (%s, %I WITHOUT OVERLAPS)',
                (SELECT string_agg(format('%I', column_name), ', ') FROM unnest(column_names) AS column_name),
                era_row.range_column_name
            );

            alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(unique_constraint) || ' ' || unique_sql);

            -- Clear XOR-specific metadata (no longer using partial constraints)
            v_check_constraint := NULL;
            exclude_constraint := NULL;
        END;
    END IF;

    -- For native temporal keys, there is no separate exclusion constraint.
    IF key_type IN ('primary', 'natural') THEN
        exclude_constraint := NULL;
    END IF;

    -- When adding a natural key, determine the state of PK consistency enforcement.
    IF key_type = 'natural' THEN
        IF enforce_consistency_with_primary_key THEN
            DECLARE
                pk_column_names name[];
                stable_pk_columns name[];
                pk_is_compatible boolean := true;
            BEGIN
                -- A PK exists, so we can enforce immediately.
                SELECT uk.column_names INTO pk_column_names FROM sql_saga.unique_keys uk WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name) AND uk.key_type = 'primary'::sql_saga.unique_key_type;
                IF NOT FOUND THEN
                    SELECT array_agg(a.attname ORDER BY u.ord) INTO pk_column_names FROM pg_catalog.pg_constraint c JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum WHERE c.conrelid = table_oid AND c.contype = 'p';
                END IF;

                IF pk_column_names IS NOT NULL THEN
                    -- Subtract temporal columns to find the stable part of the PK.
                    SELECT array_agg(c) INTO stable_pk_columns
                    FROM unnest(pk_column_names) as c
                    WHERE c <> era_row.range_column_name;

                    IF stable_pk_columns IS NULL OR array_length(stable_pk_columns, 1) = 0 THEN
                        IF v_enforce_consistency_original = true THEN -- Error only if explicitly requested
                            RAISE EXCEPTION 'cannot enforce primary key consistency on table "%" because its PRIMARY KEY (%) does not contain a stable (non-temporal) column', sql_saga.__internal_display_name(table_oid), array_to_string(pk_column_names, ', ');
                        ELSE -- Silently opt-out if using default (NULL) or explicit false
                            pk_is_compatible := false;
                        END IF;
                    END IF;

                    IF pk_is_compatible AND array_length(stable_pk_columns, 1) > 1 THEN
                        IF v_enforce_consistency_original = true THEN -- Error only if explicitly requested
                            RAISE EXCEPTION 'cannot enforce primary key consistency for natural key (%) on table "%" because its stable PRIMARY KEY component (%) is composite', array_to_string(column_names, ', '), sql_saga.__internal_display_name(table_oid), array_to_string(stable_pk_columns, ', ');
                        ELSE -- Silently opt-out if using default (NULL) or explicit false
                            pk_is_compatible := false;
                        END IF;
                    END IF;

                    IF pk_is_compatible THEN
                        -- If we get here, we have a single stable PK column.
                        DECLARE
                            pk_column_name name := stable_pk_columns[1];
                            consistency_result jsonb;
                        BEGIN
                            -- Skip consistency constraint when natural key IS the PK column (single column case)
                            IF array_length(column_names, 1) = 1 AND column_names[1] = pk_column_name THEN
                                -- Natural key is identical to PK column, no consistency check needed
                                v_pk_consistency_constraint_names := '{}';
                            ELSE
                                consistency_result := sql_saga.__internal_generate_pk_consistency_constraints(
                                    p_unique_key_name => unique_key_name,
                                    p_natural_key_columns => column_names,
                                    p_mutually_exclusive_columns => mutually_exclusive_columns,
                                    p_primary_key_column => pk_column_name
                                );
                                alter_cmds := alter_cmds || (SELECT array_agg(value) FROM jsonb_array_elements_text(consistency_result->'cmds'));
                                v_pk_consistency_constraint_names := (SELECT array_agg(value::name) FROM jsonb_array_elements_text(consistency_result->'names'));
                            END IF;
                        END;
                    ELSE
                        -- Incompatible PK, silently opt-out.
                        v_pk_consistency_constraint_names := NULL;
                    END IF;
                ELSE
                    -- No PK exists yet. Set state to "pending" (empty array).
                    v_pk_consistency_constraint_names := '{}';
                END IF;
            END;
        ELSE
            -- User opted out. Set state to "opt-out" (NULL).
            v_pk_consistency_constraint_names := NULL;
        END IF;
    END IF;

    IF alter_cmds <> '{}' THEN

        SELECT format('ALTER TABLE %I.%I %s', n.nspname /* %I */, c.relname /* %I */, array_to_string(alter_cmds, ', ') /* %s */)
        INTO sql
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        WHERE c.oid = table_oid;

        EXECUTE sql;
        RAISE NOTICE 'sql_saga: Added constraints to table %: %', table_oid, array_to_string(alter_cmds, '; ');
    END IF;

    /* If we don't already have a unique_constraint, it must be the one with the highest oid */
    IF unique_constraint IS NULL AND key_type IN ('primary', 'natural') THEN
        SELECT c.conname
        INTO unique_constraint
        FROM pg_catalog.pg_constraint AS c
        WHERE c.conrelid = table_oid AND c.contype = (CASE WHEN key_type = 'primary' THEN 'p' ELSE 'u' END)
        ORDER BY oid DESC
        LIMIT 1;
    END IF;

    RAISE DEBUG 'add_unique_key: about to insert with v_stable_cols_to_store=%', v_stable_cols_to_store;

    RAISE DEBUG 'add_unique_key: Inserting into sql_saga.unique_keys: name=%, table=%.%, key_type=%, cols=%, era=%, u_constraint=%, ex_constraint=%, check_constraint=%, predicate=%, mut_ex_cols=%, pk_consistency_constraints=%',
        unique_key_name, table_schema, table_name, key_type, v_stable_cols_to_store, era_name, unique_constraint, exclude_constraint, v_check_constraint, predicate, mutually_exclusive_columns, v_pk_consistency_constraint_names;
    INSERT INTO sql_saga.unique_keys (unique_key_name, table_schema, table_name, key_type, column_names, era_name, unique_constraint, exclude_constraint, check_constraint, predicate, mutually_exclusive_columns, pk_consistency_constraint_names)
    VALUES (unique_key_name, table_schema, table_name, key_type, v_stable_cols_to_store, era_name, unique_constraint, exclude_constraint, v_check_constraint, predicate, mutually_exclusive_columns, v_pk_consistency_constraint_names);

    -- Create compound index for efficient exact match queries (needed by temporal_merge)
    -- This index helps UPDATE/DELETE operations that need to find rows by both
    -- identity columns AND exact range match
    IF era_name IS NOT NULL AND array_length(v_stable_cols_to_store, 1) > 0 THEN
        DECLARE
            compound_index_name name;
            compound_index_sql text;
            compound_columns text;
        BEGIN
            -- Build list of columns for the compound index
            compound_columns := (
                SELECT string_agg(quote_ident(c), ', ')
                FROM unnest(v_stable_cols_to_store) AS c
            );

            -- Add the range column at the end
            compound_columns := compound_columns || ', ' || quote_ident(era_row.range_column_name);

            compound_index_name := sql_saga.__internal_make_name(
                ARRAY[table_name] || v_stable_cols_to_store || ARRAY[era_row.range_column_name, 'idx']
            );

            -- Check if index already exists
            IF NOT EXISTS (
                SELECT 1 FROM pg_index i
                JOIN pg_class c ON c.oid = i.indexrelid
                WHERE i.indrelid = table_oid
                AND c.relname = compound_index_name
            ) THEN
                compound_index_sql := format(
                    'CREATE INDEX %I ON %I.%I (%s)',
                    compound_index_name,
                    table_schema,
                    table_name,
                    compound_columns
                );

                RAISE DEBUG 'add_unique_key: Creating compound index for temporal_merge performance: %', compound_index_sql;
                EXECUTE compound_index_sql;
                RAISE DEBUG 'sql_saga: created compound index % on % for temporal_merge performance', compound_index_name, table_oid::regclass;
            END IF;
        END;
    END IF;

    -- When adding a primary key, scan for any natural keys in a "pending" state.
    IF key_type = 'primary' THEN
        DECLARE
            nk_row sql_saga.unique_keys;
            pk_column_name name;
            consistency_cmds text[];
        BEGIN
            DECLARE
                stable_pk_columns name[];
            BEGIN
                -- Subtract temporal columns from the new PK to find its stable part.
                SELECT array_agg(c) INTO stable_pk_columns
                FROM unnest(column_names) as c
                WHERE c <> era_row.range_column_name;

                IF stable_pk_columns IS NULL OR array_length(stable_pk_columns, 1) = 0 THEN
                    IF EXISTS (SELECT 1 FROM sql_saga.unique_keys uk WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name) AND uk.key_type = 'natural' AND uk.pk_consistency_constraint_names = '{}') THEN
                        RAISE EXCEPTION 'cannot add primary key (%) to table "%" because it has no stable (non-temporal) part, which is required by existing natural keys pending consistency enforcement', array_to_string(column_names, ', '), sql_saga.__internal_display_name(table_oid);
                    END IF;
                ELSIF array_length(stable_pk_columns, 1) > 1 THEN
                    IF EXISTS (SELECT 1 FROM sql_saga.unique_keys uk WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name) AND uk.key_type = 'natural' AND uk.pk_consistency_constraint_names = '{}') THEN
                        RAISE EXCEPTION 'cannot add primary key (%) to table "%" because its stable part (%) is composite, which is not supported for consistency enforcement required by existing natural keys', array_to_string(column_names, ', '), sql_saga.__internal_display_name(table_oid), array_to_string(stable_pk_columns, ', ');
                    END IF;
                ELSE
                    -- This is a compatible, single-column stable PK. Apply to pending NKs.
                    pk_column_name := stable_pk_columns[1];
                    FOR nk_row IN
                        SELECT * FROM sql_saga.unique_keys uk
                        WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name)
                        AND uk.key_type = 'natural' AND uk.pk_consistency_constraint_names = '{}' -- Find pending keys
                    LOOP
                        DECLARE
                            consistency_result jsonb;
                            newly_created_constraint_names name[];
                        BEGIN
                             consistency_result := sql_saga.__internal_generate_pk_consistency_constraints(
                                p_unique_key_name => nk_row.unique_key_name,
                                p_natural_key_columns => nk_row.column_names,
                                p_mutually_exclusive_columns => nk_row.mutually_exclusive_columns,
                                p_primary_key_column => pk_column_name
                            );
                            consistency_cmds := consistency_cmds || (SELECT array_agg(value) FROM jsonb_array_elements_text(consistency_result->'cmds'));
                            newly_created_constraint_names := (SELECT array_agg(value::name) FROM jsonb_array_elements_text(consistency_result->'names'));
                            UPDATE sql_saga.unique_keys SET pk_consistency_constraint_names = newly_created_constraint_names WHERE unique_key_name = nk_row.unique_key_name;
                        END;
                    END LOOP;

                    IF consistency_cmds IS NOT NULL AND array_length(consistency_cmds, 1) > 0 THEN
                        RAISE NOTICE 'sql_saga: Applied retroactive primary key consistency constraints to table %.%: %', table_schema, table_name, array_to_string(consistency_cmds, '; ');
                        EXECUTE format('ALTER TABLE %I.%I %s', table_schema, table_name, array_to_string(consistency_cmds, ', '));
                    END IF;
                END IF;
            END;
        END;
    END IF;

    -- Create a standard B-Tree index on the unique key columns to support
    -- fast lookups for foreign key checks (both temporal and regular).
    -- This is only created if a suitable index doesn't already exist.
    DECLARE
        index_name name;
        index_sql text;
        existing_index_oid oid;
    BEGIN
        -- Find an existing B-Tree index on exactly these columns in this order
        SELECT i.indexrelid INTO existing_index_oid
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid = c.relam
        WHERE i.indrelid = table_oid
          AND i.indisvalid
          AND i.indnkeyatts = array_length(column_names, 1)
          AND i.indkey::text = array_to_string(column_attnums, ' ')
          AND am.amname = 'btree'
        LIMIT 1;

        IF existing_index_oid IS NULL THEN
            index_name := sql_saga.__internal_make_name(
                ARRAY[table_name] || column_names || ARRAY['idx']
            );
            index_sql := format('CREATE INDEX %I ON %I.%I USING btree (%s)',
                index_name, /* %I */
                table_schema, /* %I */
                table_name, /* %I */
                (SELECT string_agg(quote_ident(c), ', ') FROM unnest(column_names) AS u(c)) /* %s */
            );
            EXECUTE index_sql;
        END IF;
    END;

    RETURN unique_key_name;
END;
$function$;

COMMENT ON FUNCTION sql_saga.add_unique_key(regclass, name[], name, sql_saga.unique_key_type, boolean, name, name, name, text, name[]) IS
'Adds a temporal unique key to a table, ensuring uniqueness across time for a given set of columns within an era. Supports primary, natural, and predicated keys. Can also enforce consistency between a natural key and the primary key.';
CREATE OR REPLACE FUNCTION sql_saga.health_checks()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 -- Set a safe search path for this SECURITY DEFINER function
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    cmd text;
    r record;
    managed_policy record;
    gated_columns text;
    expected_gate text;
    alter_function_resolved text;
BEGIN
    IF tg_tag IN ('ALTER FUNCTION', 'ALTER ROUTINE') THEN
        alter_function_resolved := current_setting('sql_saga.alter_function_resolved', true);
        /* Consume the statement snapshot before doing any comparison or raising. */
        PERFORM set_config('sql_saga.alter_function_resolved', '', true);

        IF alter_function_resolved IS NULL OR alter_function_resolved = '' THEN
            RETURN;
        END IF;

        FOR r IN
            SELECT to_regclass(format('%I.%I', x.table_schema, x.table_name)) AS table_oid,
                   x.identity
            FROM jsonb_to_recordset(alter_function_resolved::jsonb) AS x(
                table_schema name,
                table_name name,
                role text,
                identity text)
            WHERE to_regprocedure(x.identity) IS NULL
            ORDER BY x.table_schema, x.table_name, x.role
        LOOP
            RAISE EXCEPTION 'cannot drop or rename function "%" because it is used in SYSTEM VERSIONING for table "%"',
                sql_saga.__internal_display_function_name(r.identity), sql_saga.__internal_display_name(r.table_oid);
        END LOOP;
    END IF;

    /* sql_drop protects exact managed members before DROP reaches ddl_command_end. */
    IF tg_tag LIKE 'DROP %' THEN
        RETURN;
    END IF;

    -- Exit early if the DDL command does not affect a managed object.
    IF NOT sql_saga.__internal_ddl_command_affects_managed_object() THEN
        RETURN;
    END IF;

    /* Make sure that all of our tables are still persistent */
    FOR r IN
        SELECT to_regclass(format('%I.%I', e.table_schema /* %I */, e.table_name /* %I */)) AS table_oid
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_class AS c ON c.relname = e.table_name
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = e.table_schema
        WHERE c.relpersistence <> 'p'
    LOOP
        RAISE EXCEPTION 'table "%" must remain persistent because it has an era',
            sql_saga.__internal_display_name(r.table_oid);
    END LOOP;

    /* And the history tables, too */
    FOR r IN
        SELECT to_regclass(format('%I.%I', e.table_schema /* %I */, e.table_name /* %I */)) AS table_oid
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_class AS c ON c.relname = e.audit_table_name
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace AND n.nspname = e.audit_schema_name
        WHERE e.audit_table_name IS NOT NULL AND c.relpersistence <> 'p'
    LOOP
        RAISE EXCEPTION 'history table "%" must remain persistent because it has an era',
            sql_saga.__internal_display_name(r.table_oid);
    END LOOP;

    /* Fix up history and for-portion objects ownership */
    FOR cmd IN
        /* Fix ownership of history tables */
        SELECT format('ALTER TABLE %I.%I OWNER TO %I',
            sv.history_schema_name /* %I */, sv.history_table_name /* %I */, t.relowner::regrole /* %I */)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        JOIN pg_class AS ht ON ht.relname = sv.history_table_name
        JOIN pg_namespace AS htn ON htn.oid = ht.relnamespace AND htn.nspname = sv.history_schema_name
        WHERE t.relowner <> ht.relowner

        UNION ALL

        /* Fix ownership of system versioning views */
        SELECT format('ALTER VIEW %I.%I OWNER TO %I',
            sv.view_schema_name /* %I */, sv.view_table_name /* %I */, t.relowner::regrole /* %I */)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        JOIN pg_class AS vt ON vt.relname = sv.view_table_name
        JOIN pg_namespace AS vtn ON vtn.oid = vt.relnamespace AND vtn.nspname = sv.view_schema_name
        WHERE t.relowner <> vt.relowner

        UNION ALL

        /* Fix ownership of the gated history views */
        SELECT format('ALTER VIEW %s OWNER TO %I', f.gated_oid::regclass, t.relowner::regrole)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        CROSS JOIN LATERAL sql_saga.__internal_history_family(t.oid) AS f
        JOIN pg_class AS gv ON gv.oid = f.gated_oid
        WHERE f.base_oid = t.oid AND t.relowner <> gv.relowner

        UNION ALL

        /* Fix ownership of for-portion-of views */
        SELECT format('ALTER VIEW %I.%I OWNER TO %I', v.view_schema /* %I */, v.view_name /* %I */, t.relowner::regrole /* %I */)
        FROM sql_saga.updatable_view v
        JOIN pg_class t ON t.relname = v.table_name
        JOIN pg_namespace tn ON tn.oid = t.relnamespace AND tn.nspname = v.table_schema
        JOIN pg_class vt ON vt.relname = v.view_name
        JOIN pg_namespace vn ON vn.oid = vt.relnamespace AND vn.nspname = v.view_schema
        WHERE t.relowner <> vt.relowner

        UNION ALL

        /* Fix ownership of system versioning functions */
        SELECT format('ALTER FUNCTION %s OWNER TO %I', p.oid::regprocedure, t.relowner::regrole)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        JOIN pg_proc AS p ON p.oid IN (to_regprocedure(sv.func_as_of), to_regprocedure(sv.func_between),
                                       to_regprocedure(sv.func_between_symmetric), to_regprocedure(sv.func_from_to))
        WHERE t.relowner <> p.proowner
    LOOP
        EXECUTE cmd;
    END LOOP;

    /*
     * Row-level security of history tables (tmp/design_final.md sections 3, 8-10, 15).
     * Policy DDL on a versioned base table (or its history), and ENABLE/DISABLE/FORCE/NO FORCE on
     * either, re-synchronizes that family's history policies. Direct changes to the policies
     * sql_saga manages, and to the history's RLS settings, are refused. Only the synchronizer's own
     * nested DDL is exempt, recognized by its private guard row
     * (sql_saga.__internal_history_policy_sync_active), never by a setting a caller could forge.
     */
    FOR r IN
        SELECT DISTINCT f.base_oid, f.history_oid, ev.command_tag, ev.objid, ev.classid
        FROM pg_event_trigger_ddl_commands() AS ev
        LEFT JOIN pg_catalog.pg_policy AS pol
               ON ev.classid = 'pg_catalog.pg_policy'::regclass AND pol.oid = ev.objid
        CROSS JOIN LATERAL sql_saga.__internal_history_family(
            CASE WHEN ev.classid = 'pg_catalog.pg_policy'::regclass THEN pol.polrelid ELSE ev.objid END) AS f
        WHERE ev.command_tag IN ('CREATE POLICY', 'ALTER POLICY', 'ALTER TABLE')
    LOOP
        IF NOT sql_saga.__internal_history_policy_sync_active(r.base_oid) THEN
            -- Refuse direct DDL on managed history policies.
            -- CREATE POLICY of a managed name is what pg_restore does when it replays a dump: the sync
            -- below then keeps it (current installation) or drops it (another installation's token).
            IF r.classid = 'pg_catalog.pg_policy'::regclass THEN
                SELECT p.polname, p.polrelid INTO managed_policy
                FROM pg_catalog.pg_policy AS p WHERE p.oid = r.objid;
                IF managed_policy.polrelid = r.history_oid THEN
                    IF NOT sql_saga.__internal_is_managed_history_policy(managed_policy.polname) THEN
                        -- A policy of one's own on the history table (decision 7): it would be OR-ed
                        -- with the copies and could show more history than the base table allows.
                        -- ALTER POLICY ... RENAME TO a non-managed name lands here too.
                        RAISE EXCEPTION 'sql_saga: policies on history table "%" are managed by sql_saga', sql_saga.__internal_display_name(r.history_oid::regclass)
                            USING HINT = format('Create the policy on "%s"; sql_saga copies its SELECT and ALL policies to the history.',
                                                sql_saga.__internal_display_name(r.base_oid::regclass));
                    ELSIF r.command_tag = 'ALTER POLICY' THEN
                        RAISE EXCEPTION 'cannot drop or alter policy "%" on "%" because it is managed by sql_saga for table "%"',
                            managed_policy.polname, sql_saga.__internal_display_name(r.history_oid::regclass), sql_saga.__internal_display_name(r.base_oid::regclass);
                    END IF;
                END IF;
            END IF;
            -- Refuse direct changes to the history's RLS settings.
            IF r.command_tag = 'ALTER TABLE' AND r.objid = r.history_oid
               AND NOT EXISTS (SELECT FROM pg_catalog.pg_class AS h, pg_catalog.pg_class AS b
                               WHERE h.oid = r.history_oid AND b.oid = r.base_oid
                                 AND h.relrowsecurity AND h.relforcerowsecurity = b.relforcerowsecurity) THEN
                RAISE EXCEPTION 'history table "%" must keep row level security enabled, and forced exactly when its base table is, because it is used in SYSTEM VERSIONING for table "%"',
                    sql_saga.__internal_display_name(r.history_oid::regclass), sql_saga.__internal_display_name(r.base_oid::regclass);
            END IF;
        END IF;
        -- Not during the teardown by pg_restore --clean (its ALTER TABLE .. DROP CONSTRAINT on the base
        -- would recreate the history policies drop_protection has just dropped for the teardown).
        -- Not in a declared parallel pg_restore either: the sync locks both tables of the family, which
        -- other workers may hold (deadlock); sync_all_history_policies() runs it after the restore.
        IF NOT sql_saga.__internal_history_policy_sync_active(r.base_oid)
           AND NOT sql_saga.__internal_is_clean_teardown()
           AND NOT sql_saga.__internal_is_parallel_restore()
           AND (r.objid <> r.history_oid OR r.classid = 'pg_catalog.pg_policy'::regclass) THEN
            PERFORM sql_saga.__internal_sync_history_policies(r.base_oid);
        END IF;
    END LOOP;

    /* The fixed shape of views sql_saga manages. */
    FOR r IN
        SELECT ev.objid AS view_oid, v.base_oid, v.kind
        FROM pg_event_trigger_ddl_commands() AS ev
        JOIN (
            SELECT format('%I.%I', sv.view_schema_name, sv.view_table_name)::regclass::oid AS view_oid,
                   format('%I.%I', sv.table_schema, sv.table_name)::regclass::oid AS base_oid,
                   'SYSTEM VERSIONING'::text AS kind
            FROM sql_saga.system_versioning AS sv
            WHERE to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name)) IS NOT NULL
            UNION ALL
            SELECT format('%I.%I', uv.view_schema, uv.view_name)::regclass::oid,
                   format('%I.%I', uv.table_schema, uv.table_name)::regclass::oid,
                   CASE uv.view_type WHEN 'for_portion_of' THEN 'FOR PORTION OF' ELSE 'a current view' END
            FROM sql_saga.updatable_view AS uv
            WHERE to_regclass(format('%I.%I', uv.view_schema, uv.view_name)) IS NOT NULL
        ) AS v ON v.view_oid = ev.objid
        WHERE ev.command_tag IN ('ALTER VIEW', 'CREATE VIEW', 'ALTER TABLE')
    LOOP
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c
                       WHERE c.oid = r.view_oid AND 'security_invoker=on' = ANY (c.reloptions)) THEN
            RAISE EXCEPTION 'view "%" must keep security_invoker because it is used in % for table "%"',
                sql_saga.__internal_display_name(r.view_oid::regclass), r.kind, sql_saga.__internal_display_name(r.base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT ev.objid AS view_oid, f.base_oid, f.history_oid
        FROM pg_event_trigger_ddl_commands() AS ev
        JOIN sql_saga.system_versioning AS sv
          ON to_regclass(format('%I.%I', sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)))::oid = ev.objid
        CROSS JOIN LATERAL sql_saga.__internal_history_family(format('%I.%I', sv.table_schema, sv.table_name)::regclass) AS f
        WHERE ev.command_tag IN ('ALTER VIEW', 'CREATE VIEW', 'ALTER TABLE')
    LOOP
        -- The gate must be exactly the generated definition (owner-rights, same body), not just
        -- contain its parts: e.g. "WHERE true OR (gate)" keeps every function reference but opens
        -- the view. The canonical body is rendered from the view's own column list, so the check
        -- holds whatever columns the history has, and both are deparsed the same way.
        gated_columns := (SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum)
                          FROM pg_catalog.pg_attribute AS a
                          WHERE a.attrelid = r.view_oid AND a.attnum > 0 AND NOT a.attisdropped);
        EXECUTE 'CREATE TEMPORARY VIEW sql_saga_expected_gate AS '
            || sql_saga.__internal_history_gated_view_sql(r.base_oid, r.history_oid, gated_columns);
        expected_gate := pg_catalog.pg_get_viewdef('pg_temp.sql_saga_expected_gate'::regclass);
        DROP VIEW pg_temp.sql_saga_expected_gate;
        IF EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = r.view_oid AND c.reloptions IS NOT NULL)
           OR pg_catalog.pg_get_viewdef(r.view_oid) IS DISTINCT FROM expected_gate THEN
            RAISE EXCEPTION 'view "%" must keep its access gate because it is used in SYSTEM VERSIONING for table "%"',
                sql_saga.__internal_display_name(r.view_oid::regclass), sql_saga.__internal_display_name(r.base_oid::regclass);
        END IF;
    END LOOP;

END;
$function$;

CREATE OR REPLACE FUNCTION sql_saga.__internal_unique_key_object_is_attributable(unique_key_name name, object_kind text)
 RETURNS boolean
 LANGUAGE plpgsql
 STABLE
 SET search_path TO 'sql_saga', 'pg_catalog'
AS $function$
#variable_conflict use_variable
DECLARE
    uk sql_saga.unique_keys;
    table_oid oid;
    range_column name;
    expected_attnums smallint[];
    object_oid oid;
    attributable boolean;
BEGIN
    SELECT x.* INTO uk FROM sql_saga.unique_keys AS x
    WHERE x.unique_key_name = $1;
    IF NOT FOUND THEN RETURN false; END IF;

    table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name));
    IF table_oid IS NULL THEN RETURN false; END IF;
    SELECT e.range_column_name INTO range_column FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name);
    SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) INTO expected_attnums
    FROM pg_catalog.unnest(uk.column_names || range_column) WITH ORDINALITY AS u(column_name, n)
    JOIN pg_catalog.pg_attribute AS a ON a.attrelid = table_oid AND a.attname = u.column_name
        AND a.attnum > 0 AND NOT a.attisdropped;
    IF pg_catalog.cardinality(expected_attnums) IS DISTINCT FROM pg_catalog.cardinality(uk.column_names) + 1 THEN
        RETURN false;
    END IF;

    IF object_kind = 'unique' THEN
        IF uk.unique_constraint IS NULL THEN RETURN true; END IF;
        IF uk.key_type = 'predicated' THEN
            object_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.unique_constraint));
            RETURN EXISTS (
                SELECT 1 FROM pg_catalog.pg_index AS i
                JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
                JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
                WHERE i.indexrelid = object_oid AND i.indrelid = table_oid
                  AND i.indisunique AND i.indisvalid AND i.indisready
                  AND i.indnkeyatts = pg_catalog.cardinality(expected_attnums)
                  AND i.indkey::text = pg_catalog.array_to_string(expected_attnums, ' ')
                  AND i.indexprs IS NULL AND am.amname = 'btree'
                  AND pg_catalog.pg_get_expr(i.indpred, i.indrelid) IS NOT DISTINCT FROM uk.predicate);
        END IF;
        SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_constraint AS c
            JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.conindid
            JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
            JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
            WHERE c.conrelid = table_oid AND c.conname = uk.unique_constraint
              AND c.contype = CASE uk.key_type WHEN 'primary' THEN 'p' ELSE 'u' END
              AND c.conkey = expected_attnums AND c.conperiod
              AND NOT c.condeferrable AND NOT c.condeferred
              AND i.indrelid = table_oid AND i.indisunique AND i.indisvalid AND i.indisready
              AND i.indnkeyatts = pg_catalog.cardinality(expected_attnums)
              AND i.indkey::text = pg_catalog.array_to_string(expected_attnums, ' ') AND i.indexprs IS NULL
              AND i.indpred IS NULL AND am.amname = 'gist')
        INTO attributable;
        RETURN attributable;
    ELSIF object_kind = 'exclude' THEN
        IF uk.exclude_constraint IS NULL THEN RETURN true; END IF;
        SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_constraint AS c
            JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.conindid
            JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
            JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
            WHERE c.conrelid = table_oid AND c.conname = uk.exclude_constraint
              AND c.contype = 'x' AND c.conkey = expected_attnums
              AND c.condeferrable AND NOT c.condeferred
              AND i.indrelid = table_oid AND i.indisvalid AND i.indisready
              AND i.indnkeyatts = pg_catalog.cardinality(expected_attnums)
              AND i.indkey::text = pg_catalog.array_to_string(expected_attnums, ' ') AND i.indexprs IS NULL
              AND am.amname = 'gist'
              AND pg_catalog.cardinality(c.conexclop) = pg_catalog.cardinality(expected_attnums)
              AND NOT EXISTS (
                  SELECT 1
                  FROM pg_catalog.unnest(c.conexclop) WITH ORDINALITY AS x(oproid, n)
                  JOIN pg_catalog.pg_operator AS o ON o.oid = x.oproid
                  WHERE (x.n <= pg_catalog.cardinality(uk.column_names) AND o.oprname <> '=')
                     OR (x.n = pg_catalog.cardinality(expected_attnums) AND o.oprname <> '&&')))
        INTO attributable;
        RETURN attributable;
    ELSIF object_kind = 'check' THEN
        /* Deprecated catalog rows have no canonical 1.2 CHECK object. */
        RETURN uk.check_constraint IS NULL;
    END IF;
    RETURN false;
END;
$function$;

CREATE FUNCTION sql_saga.__internal_system_versioning_member_attribution(table_oid oid)
RETURNS TABLE(
    as_of_attributable boolean,
    between_attributable boolean,
    between_symmetric_attributable boolean,
    from_to_attributable boolean,
    public_view_attributable boolean,
    gated_view_attributable boolean)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_system_versioning_member_attribution$
    WITH family AS (
        SELECT base.relowner,
               public_view.oid AS public_view_oid,
               gated_view.oid AS gated_view_oid,
               public_view.reltype AS public_view_type
        FROM pg_catalog.pg_class AS base
        JOIN sql_saga.system_versioning AS sv
          ON pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name)) = base.oid
        LEFT JOIN pg_catalog.pg_class AS public_view
          ON public_view.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.view_schema_name, sv.view_table_name))
        LEFT JOIN pg_catalog.pg_class AS gated_view
          ON gated_view.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name,
               sql_saga.__internal_history_gated_name(sv.history_table_name)))
        WHERE base.oid = table_oid
    ), functions AS (
        SELECT q.role,
               p.oid IS NOT NULL
               AND p.prokind = 'f'
               AND p.prolang = (SELECT l.oid FROM pg_catalog.pg_language AS l WHERE l.lanname = 'sql')
               AND p.prorettype = f.public_view_type AND p.proretset AND p.provolatile = 's'
               AND NOT p.proisstrict AND NOT p.prosecdef AND NOT p.proleakproof
               AND p.proparallel = 'u' AND p.prosupport = 0 AND p.procost = 100 AND p.prorows = 1000
               AND p.proconfig IS NULL AND p.proowner = f.relowner AND p.prosrc = q.body
               AND p.prosqlbody IS NULL AND p.proargnames IS NULL AND p.proallargtypes IS NULL
               AND p.proargmodes IS NULL
               AND p.proargtypes = CASE q.role WHEN 'as_of' THEN '1184'::pg_catalog.oidvector ELSE '1184 1184'::pg_catalog.oidvector END
               AND NOT EXISTS (
                   SELECT 1 FROM pg_catalog.aclexplode(coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS x
                   WHERE x.grantee NOT IN (0, p.proowner) OR (x.grantee = 0 AND x.privilege_type <> 'EXECUTE'))
               AND EXISTS (
                   SELECT 1 FROM pg_catalog.aclexplode(coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS x
                   WHERE x.grantee = 0 AND x.privilege_type = 'EXECUTE')
               AND 1 = (SELECT pg_catalog.count(*) FROM pg_catalog.pg_depend AS d
                        WHERE d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass AND d.objid = p.oid
                          AND d.refclassid = 'pg_catalog.pg_type'::pg_catalog.regclass AND d.deptype = 'n')
               AND EXISTS (SELECT 1 FROM pg_catalog.pg_depend AS d
                           WHERE d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass AND d.objid = p.oid
                             AND d.refclassid = 'pg_catalog.pg_type'::pg_catalog.regclass
                             AND d.refobjid = f.public_view_type AND d.deptype = 'n') AS attributable
        FROM family AS f
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_query_functions(table_oid) AS q
        LEFT JOIN pg_catalog.pg_proc AS p ON p.oid = pg_catalog.to_regprocedure(q.identity)
    )
    SELECT coalesce(bool_and(attributable) FILTER (WHERE role = 'as_of'), false),
           coalesce(bool_and(attributable) FILTER (WHERE role = 'between'), false),
           coalesce(bool_and(attributable) FILTER (WHERE role = 'between_symmetric'), false),
           coalesce(bool_and(attributable) FILTER (WHERE role = 'from_to'), false),
           sql_saga.__internal_system_versioning_view_is_attributable(table_oid, f.public_view_oid, 'public'),
           sql_saga.__internal_system_versioning_view_is_attributable(table_oid, f.gated_view_oid, 'gated')
    FROM family AS f
    LEFT JOIN functions ON true
    GROUP BY f.public_view_oid, f.gated_view_oid;
$__internal_system_versioning_member_attribution$;

CREATE FUNCTION sql_saga.__internal_unique_key_inventory(filter_unique_key_name name DEFAULT NULL)
RETURNS TABLE(
    unique_key_name name,
    table_oid oid,
    unique_attributable boolean,
    exclude_attributable boolean,
    check_attributable boolean,
    object_missing boolean)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_unique_key_inventory$
    WITH keys AS (
        SELECT uk.*, e.range_column_name,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name))::oid AS table_oid
        FROM sql_saga.unique_keys AS uk
        LEFT JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
        WHERE filter_unique_key_name IS NULL OR uk.unique_key_name = filter_unique_key_name
    ), expected AS (
        SELECT k.*,
               x.expected_attnums,
               pg_catalog.cardinality(x.expected_attnums) = pg_catalog.cardinality(k.column_names) + 1 AS columns_valid
        FROM keys AS k
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS expected_attnums
            FROM pg_catalog.unnest(k.column_names || k.range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = k.table_oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS x ON true
    )
    SELECT k.unique_key_name, k.table_oid,
        CASE
            WHEN k.table_oid IS NULL OR NOT k.columns_valid THEN false
            WHEN uk.unique_constraint IS NULL THEN true
            WHEN uk.key_type = 'predicated' THEN EXISTS (
                SELECT 1 FROM pg_catalog.pg_index AS i
                JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
                JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
                WHERE i.indexrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.unique_constraint))
                  AND i.indrelid = k.table_oid AND i.indisunique AND i.indisvalid AND i.indisready
                  AND i.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                  AND i.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                  AND i.indexprs IS NULL AND am.amname = 'btree'
                  AND pg_catalog.regexp_replace(
                          pg_catalog.regexp_replace(
                              pg_catalog.pg_get_expr(i.indpred, i.indrelid),
                              '::(text|character varying)', '', 'g'),
                          '\(([[:alnum:]_".]+)\)', '\1', 'g')
                      IS NOT DISTINCT FROM
                      CASE WHEN uk.predicate IS NULL THEN NULL ELSE '(' || uk.predicate || ')' END)
            ELSE EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint AS c
                JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.conindid
                JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
                JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
                WHERE c.conrelid = k.table_oid AND c.conname = uk.unique_constraint
                  AND c.contype = CASE uk.key_type WHEN 'primary' THEN 'p' ELSE 'u' END
                  AND c.conkey = k.expected_attnums AND c.conperiod
                  AND NOT c.condeferrable AND NOT c.condeferred
                  AND i.indrelid = k.table_oid AND i.indisunique AND i.indisvalid AND i.indisready
                  AND i.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                  AND i.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                  AND i.indexprs IS NULL AND i.indpred IS NULL AND am.amname = 'gist')
        END AS unique_attributable,
        CASE
            WHEN k.table_oid IS NULL OR NOT k.columns_valid THEN false
            WHEN uk.exclude_constraint IS NULL THEN true
            ELSE EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint AS c
                JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.conindid
                JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
                JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
                WHERE c.conrelid = k.table_oid AND c.conname = uk.exclude_constraint
                  AND c.contype = 'x' AND c.conkey = k.expected_attnums
                  AND c.condeferrable AND NOT c.condeferred
                  AND i.indrelid = k.table_oid AND i.indisvalid AND i.indisready
                  AND i.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                  AND i.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                  AND i.indexprs IS NULL AND am.amname = 'gist'
                  AND pg_catalog.regexp_replace(
                          pg_catalog.regexp_replace(
                              pg_catalog.pg_get_expr(i.indpred, i.indrelid),
                              '::(text|character varying)', '', 'g'),
                          '\(([[:alnum:]_".]+)\)', '\1', 'g')
                      IS NOT DISTINCT FROM
                      CASE WHEN uk.predicate IS NULL THEN NULL ELSE '(' || uk.predicate || ')' END
                  AND pg_catalog.cardinality(c.conexclop) = pg_catalog.cardinality(k.expected_attnums)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pg_catalog.unnest(c.conexclop) WITH ORDINALITY AS x(oproid, n)
                      JOIN pg_catalog.pg_operator AS o ON o.oid = x.oproid
                      WHERE (x.n <= pg_catalog.cardinality(uk.column_names) AND o.oprname <> '=')
                         OR (x.n = pg_catalog.cardinality(k.expected_attnums) AND o.oprname <> '&&')))
        END AS exclude_attributable,
        uk.check_constraint IS NULL AS check_attributable,
        k.table_oid IS NULL
        OR (uk.unique_constraint IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = k.table_oid AND c.conname = uk.unique_constraint)
            AND pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.unique_constraint)) IS NULL)
        OR (uk.exclude_constraint IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = k.table_oid AND c.conname = uk.exclude_constraint)) AS object_missing
    FROM expected AS k
    CROSS JOIN LATERAL (SELECT k.*) AS uk;
$__internal_unique_key_inventory$;

CREATE FUNCTION sql_saga.__internal_unique_key_attribution(unique_key_name name)
RETURNS TABLE(unique_attributable boolean, exclude_attributable boolean, check_attributable boolean)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_unique_key_attribution$
    SELECT i.unique_attributable, i.exclude_attributable, i.check_attributable
    FROM sql_saga.__internal_unique_key_inventory($1) AS i
    WHERE i.unique_key_name = $1;
$__internal_unique_key_attribution$;

CREATE OR REPLACE FUNCTION sql_saga.__internal_system_versioning_member_attribution(table_oid oid)
RETURNS TABLE(
    as_of_attributable boolean,
    between_attributable boolean,
    between_symmetric_attributable boolean,
    from_to_attributable boolean,
    public_view_attributable boolean,
    gated_view_attributable boolean)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_system_versioning_member_attribution$
    WITH family AS (
        SELECT base.relowner,
               history.oid AS history_oid,
               public_view.oid AS public_view_oid,
               gated_view.oid AS gated_view_oid,
               public_view.reltype AS public_view_type
        FROM pg_catalog.pg_class AS base
        JOIN pg_catalog.pg_namespace AS bn ON bn.oid = base.relnamespace
        JOIN sql_saga.system_versioning AS sv
          ON (sv.table_schema, sv.table_name) = (bn.nspname, base.relname)
        LEFT JOIN pg_catalog.pg_namespace AS hn ON hn.nspname = sv.history_schema_name
        LEFT JOIN pg_catalog.pg_class AS history
          ON history.relnamespace = hn.oid AND history.relname = sv.history_table_name
        LEFT JOIN pg_catalog.pg_namespace AS vn ON vn.nspname = sv.view_schema_name
        LEFT JOIN pg_catalog.pg_class AS public_view
          ON public_view.relnamespace = vn.oid AND public_view.relname = sv.view_table_name
        LEFT JOIN pg_catalog.pg_class AS gated_view
          ON gated_view.relnamespace = hn.oid
         AND gated_view.relname = sql_saga.__internal_history_gated_name(sv.history_table_name)
        WHERE base.oid = table_oid
    ), function_objects AS (
        SELECT q.role, q.body, p.*
        FROM family AS f
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_query_functions(table_oid) AS q
        LEFT JOIN pg_catalog.pg_proc AS p ON p.oid = pg_catalog.to_regprocedure(q.identity)
    ), function_acl AS (
        SELECT fo.oid,
               pg_catalog.bool_and(x.grantee IN (0, fo.proowner)
                   AND (x.grantee <> 0 OR x.privilege_type = 'EXECUTE')) AS valid,
               pg_catalog.bool_or(x.grantee = 0 AND x.privilege_type = 'EXECUTE') AS public_execute
        FROM function_objects AS fo
        LEFT JOIN LATERAL pg_catalog.aclexplode(
            coalesce(fo.proacl, pg_catalog.acldefault('f', fo.proowner))) AS x ON true
        WHERE fo.oid IS NOT NULL
        GROUP BY fo.oid
    ), function_dependencies AS (
        SELECT d.objid,
               pg_catalog.count(*) AS dependency_count,
               pg_catalog.bool_or(d.refobjid = f.public_view_type) AS depends_on_public_view
        FROM family AS f
        JOIN pg_catalog.pg_depend AS d
          ON d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
         AND d.refclassid = 'pg_catalog.pg_type'::pg_catalog.regclass
         AND d.deptype = 'n'
        JOIN function_objects AS fo ON fo.oid = d.objid
        GROUP BY d.objid
    ), functions AS (
        SELECT fo.role,
               fo.oid IS NOT NULL
               AND fo.prokind = 'f'
               AND fo.prolang = (SELECT l.oid FROM pg_catalog.pg_language AS l WHERE l.lanname = 'sql')
               AND fo.prorettype = f.public_view_type AND fo.proretset AND fo.provolatile = 's'
               AND NOT fo.proisstrict AND NOT fo.prosecdef AND NOT fo.proleakproof
               AND fo.proparallel = 'u' AND fo.prosupport = 0 AND fo.procost = 100 AND fo.prorows = 1000
               AND fo.proconfig IS NULL AND fo.proowner = f.relowner AND fo.prosrc = fo.body
               AND fo.prosqlbody IS NULL AND fo.proargnames IS NULL AND fo.proallargtypes IS NULL
               AND fo.proargmodes IS NULL
               AND fo.proargtypes = CASE fo.role WHEN 'as_of' THEN '1184'::pg_catalog.oidvector ELSE '1184 1184'::pg_catalog.oidvector END
               AND acl.valid AND acl.public_execute
               AND dep.dependency_count = 1 AND dep.depends_on_public_view AS attributable
        FROM family AS f
        CROSS JOIN function_objects AS fo
        LEFT JOIN function_acl AS acl ON acl.oid = fo.oid
        LEFT JOIN function_dependencies AS dep ON dep.objid = fo.oid
    ), view_objects AS (
        SELECT 'public'::text AS role, v.*, f.relowner AS base_owner,
               f.history_oid, f.gated_view_oid
        FROM family AS f JOIN pg_catalog.pg_class AS v ON v.oid = f.public_view_oid
        UNION ALL
        SELECT 'gated', v.*, f.relowner, f.history_oid, f.gated_view_oid
        FROM family AS f JOIN pg_catalog.pg_class AS v ON v.oid = f.gated_view_oid
    ), view_acl AS (
        SELECT vo.oid,
               pg_catalog.bool_and(x.grantee IN (0, vo.relowner)
                   AND (x.grantee <> 0 OR x.privilege_type = 'SELECT')) AS valid,
               pg_catalog.bool_or(x.grantee = 0 AND x.privilege_type = 'SELECT') AS public_select
        FROM view_objects AS vo
        LEFT JOIN LATERAL pg_catalog.aclexplode(
            coalesce(vo.relacl, pg_catalog.acldefault('r', vo.relowner))) AS x ON true
        GROUP BY vo.oid
    ), view_dependencies AS (
        SELECT vo.oid,
               pg_catalog.bool_or(d.refobjid = table_oid) AS depends_on_base,
               pg_catalog.bool_or(d.refobjid = vo.history_oid) AS depends_on_history,
               pg_catalog.bool_or(d.refobjid = vo.gated_view_oid) AS depends_on_gated,
               pg_catalog.bool_or(d.refobjid = 'sql_saga.__internal_deny_history(oid)'::pg_catalog.regprocedure) AS depends_on_deny
        FROM view_objects AS vo
        LEFT JOIN pg_catalog.pg_rewrite AS rw ON rw.ev_class = vo.oid
        LEFT JOIN pg_catalog.pg_depend AS d
          ON d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND d.objid = rw.oid
        GROUP BY vo.oid
    ), views AS (
        SELECT vo.role,
               vo.relkind = 'v' AND vo.relpersistence = 'p' AND vo.relowner = vo.base_owner
               AND acl.valid AND acl.public_select
               AND CASE vo.role
                   WHEN 'public' THEN vo.reloptions = ARRAY['security_invoker=on']::text[]
                       AND dep.depends_on_base AND dep.depends_on_history AND dep.depends_on_gated
                   WHEN 'gated' THEN vo.reloptions IS NULL AND dep.depends_on_deny
                   ELSE false
               END AS attributable
        FROM view_objects AS vo
        LEFT JOIN view_acl AS acl ON acl.oid = vo.oid
        LEFT JOIN view_dependencies AS dep ON dep.oid = vo.oid
    )
    SELECT coalesce(pg_catalog.bool_and(attributable) FILTER (WHERE role = 'as_of'), false),
           coalesce(pg_catalog.bool_and(attributable) FILTER (WHERE role = 'between'), false),
           coalesce(pg_catalog.bool_and(attributable) FILTER (WHERE role = 'between_symmetric'), false),
           coalesce(pg_catalog.bool_and(attributable) FILTER (WHERE role = 'from_to'), false),
           coalesce((SELECT v.attributable FROM views AS v WHERE v.role = 'public'), false),
           coalesce((SELECT v.attributable FROM views AS v WHERE v.role = 'gated'), false)
    FROM functions;
$__internal_system_versioning_member_attribution$;

CREATE OR REPLACE FUNCTION sql_saga.__internal_unique_key_object_is_attributable(
    unique_key_name name,
    object_kind text)
RETURNS boolean
LANGUAGE plpgsql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_unique_key_object_is_attributable$
#variable_conflict use_variable
DECLARE
    uk sql_saga.unique_keys;
    table_oid oid;
    range_column name;
    expected_attnums smallint[];
    object_oid oid;
    attributable boolean;
BEGIN
    SELECT x.* INTO uk FROM sql_saga.unique_keys AS x
    WHERE x.unique_key_name = $1;
    IF NOT FOUND THEN RETURN false; END IF;

    IF object_kind = 'exclude' AND uk.exclude_constraint IS NULL THEN RETURN true; END IF;
    IF object_kind = 'check' THEN RETURN uk.check_constraint IS NULL; END IF;

    IF object_kind = 'unique' AND uk.key_type <> 'predicated' THEN
        table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name));
        IF table_oid IS NULL THEN RETURN false; END IF;
        SELECT e.range_column_name INTO range_column
        FROM sql_saga.era AS e
        WHERE (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name);
        IF range_column IS NULL THEN RETURN false; END IF;
        SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n)
        INTO expected_attnums
        FROM pg_catalog.unnest(uk.column_names || range_column) WITH ORDINALITY AS u(column_name, n)
        JOIN pg_catalog.pg_attribute AS a
          ON a.attrelid = table_oid AND a.attname = u.column_name
         AND a.attnum > 0 AND NOT a.attisdropped;
        IF pg_catalog.cardinality(expected_attnums) IS DISTINCT FROM pg_catalog.cardinality(uk.column_names) + 1 THEN
            RETURN false;
        END IF;
        RETURN EXISTS (
            SELECT 1
            FROM pg_catalog.pg_constraint AS c
            JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.conindid
            JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
            JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
            WHERE c.conrelid = table_oid AND c.conname = uk.unique_constraint
              AND c.contype = CASE uk.key_type WHEN 'primary' THEN 'p' ELSE 'u' END
              AND c.conkey = expected_attnums AND c.conperiod
              AND NOT c.condeferrable AND NOT c.condeferred
              AND i.indrelid = table_oid AND i.indisunique AND i.indisvalid AND i.indisready
              AND i.indnkeyatts = pg_catalog.cardinality(expected_attnums)
              AND i.indkey::text = pg_catalog.array_to_string(expected_attnums, ' ')
              AND i.indexprs IS NULL AND i.indpred IS NULL AND am.amname = 'gist');
    END IF;

    SELECT CASE object_kind
        WHEN 'unique' THEN a.unique_attributable
        WHEN 'exclude' THEN a.exclude_attributable
        WHEN 'check' THEN a.check_attributable
        ELSE false
    END
    INTO attributable
    FROM sql_saga.__internal_unique_key_attribution(unique_key_name) AS a;
    RETURN attributable;
END;
$__internal_unique_key_object_is_attributable$;

CREATE FUNCTION sql_saga.__internal_temporal_foreign_key_is_attributable(foreign_key_name name)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_temporal_foreign_key_is_attributable$
    WITH family AS (
        SELECT fk.*,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', fk.table_schema, fk.table_name))::oid AS fk_table_oid,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name))::oid AS uk_table_oid,
               fk_era.range_column_name AS fk_range_column_name,
               uk_era.range_column_name AS uk_range_column_name,
               uk.column_names AS uk_column_names
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        LEFT JOIN sql_saga.era AS fk_era
          ON (fk_era.table_schema, fk_era.table_name, fk_era.era_name)
           = (fk.table_schema, fk.table_name, fk.fk_era_name)
        LEFT JOIN sql_saga.era AS uk_era
          ON (uk_era.table_schema, uk_era.table_name, uk_era.era_name)
           = (uk.table_schema, uk.table_name, uk.era_name)
        WHERE fk.foreign_key_name = $1 AND fk.type = 'temporal_to_temporal'
    ), expected AS (
        SELECT f.*,
               fk_cols.attnums AS fk_attnums,
               uk_cols.attnums AS uk_attnums
        FROM family AS f
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS attnums
            FROM pg_catalog.unnest(f.column_names || f.fk_range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = f.fk_table_oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS fk_cols ON true
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS attnums
            FROM pg_catalog.unnest(f.uk_column_names || f.uk_range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = f.uk_table_oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS uk_cols ON true
    )
    SELECT coalesce(bool_and(
        c.oid IS NOT NULL
        AND c.contype = 'f'
        AND c.conrelid = e.fk_table_oid
        AND c.confrelid = e.uk_table_oid
        AND c.conkey = e.fk_attnums
        AND c.confkey = e.uk_attnums
        AND c.conperiod
        AND c.condeferrable
        AND NOT c.condeferred
        AND c.convalidated
        AND c.conenforced
        AND c.confmatchtype = CASE e.match_type WHEN 'FULL' THEN 'f' WHEN 'PARTIAL' THEN 'p' ELSE 's' END
        AND c.confupdtype = CASE e.update_action::text WHEN 'RESTRICT' THEN 'r' WHEN 'CAS' || 'CADE' THEN 'c' WHEN 'SET NULL' THEN 'n' WHEN 'SET DEFAULT' THEN 'd' ELSE 'a' END
        AND c.confdeltype = CASE e.delete_action::text WHEN 'RESTRICT' THEN 'r' WHEN 'CAS' || 'CADE' THEN 'c' WHEN 'SET NULL' THEN 'n' WHEN 'SET DEFAULT' THEN 'd' ELSE 'a' END), false)
    FROM expected AS e
    LEFT JOIN pg_catalog.pg_constraint AS c
      ON c.conrelid = e.fk_table_oid AND c.conname = e.foreign_key_name;
$__internal_temporal_foreign_key_is_attributable$;

CREATE OR REPLACE FUNCTION sql_saga.check_integrity()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog
AS $check_integrity$
DECLARE
    r record;
    base_oid oid;
    history_oid oid;
    view_oid oid;
    expected_columns name[];
    function_oid oid;
    expected_return_type oid;
    missing_column name;
BEGIN
    /* SYSTEM_TIME invariants have the highest managed-object priority. */
    FOR r IN
        SELECT ste.*, e.range_column_name
        FROM sql_saga.system_time_era AS ste
        JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
        ORDER BY ste.table_schema COLLATE "C", ste.table_name COLLATE "C"
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        IF base_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%.%": catalogued base table is missing', r.table_schema, r.table_name;
        END IF;

        IF NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.generated_always_trigger)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.write_history_trigger)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.truncate_trigger) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME trigger is missing; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        SELECT x.attname INTO missing_column
        FROM pg_catalog.unnest(r.excluded_column_names) AS x(attname)
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = base_oid AND a.attname = x.attname AND a.attnum > 0 AND NOT a.attisdropped)
        ORDER BY x.attname COLLATE "C" LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": excluded SYSTEM_TIME column "%" is missing; repair with set_system_time_era_excluded_columns or drop_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass), missing_column;
        END IF;

        /* The snapshot detects added and removed columns only; compare it as a set, because a
         * restore recreates inherited children with inherited columns first. */
        SELECT pg_catalog.array_agg(a.attname ORDER BY a.attname COLLATE "C") INTO expected_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = base_oid AND a.attnum > 0 AND NOT a.attisdropped;
        IF expected_columns IS DISTINCT FROM (
                SELECT pg_catalog.array_agg(s.attname ORDER BY s.attname COLLATE "C")
                FROM pg_catalog.unnest(r.table_columns_snapshot) AS s(attname)) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME column snapshot is stale; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT uk.* FROM sql_saga.unique_keys AS uk
        ORDER BY uk.table_schema COLLATE "C", uk.table_name COLLATE "C", uk.unique_key_name COLLATE "C"
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        IF base_oid IS NULL
           OR (r.unique_constraint IS NOT NULL AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = base_oid AND c.conname = r.unique_constraint)
               AND pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.unique_constraint)) IS NULL)
           OR (r.exclude_constraint IS NOT NULL AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = base_oid AND c.conname = r.exclude_constraint)) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": managed constraint or index is missing; repair with drop_unique_key and add_unique_key',
                r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT fk.*, uk.table_schema AS uk_schema, uk.table_name AS uk_table
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        ORDER BY fk.table_schema COLLATE "C", fk.table_name COLLATE "C", fk.foreign_key_name COLLATE "C"
    LOOP
        IF r.type = 'regular_to_temporal' THEN
            base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.uk_schema, r.uk_table));
            IF base_oid IS NULL
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_update_trigger)
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_delete_trigger) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": referenced-side helper trigger is missing; repair with drop_foreign_key and add_regular_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
        END IF;
    END LOOP;

    FOR r IN
        SELECT v.* FROM sql_saga.updatable_view AS v
        ORDER BY v.table_schema COLLATE "C", v.table_name COLLATE "C", v.view_type, v.view_schema COLLATE "C", v.view_name COLLATE "C"
    LOOP
        view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema, r.view_name));
        IF view_oid IS NULL
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = view_oid AND t.tgname = r.trigger_name) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": identifier column or trigger is missing; repair with drop_%_view and add_%_view',
                r.view_schema, r.view_name, r.table_schema, r.table_name,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END;
        END IF;
    END LOOP;

    FOR r IN
        SELECT sv.* FROM sql_saga.system_versioning AS sv
        ORDER BY sv.table_schema COLLATE "C", sv.table_name COLLATE "C"
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        history_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.history_schema_name, r.history_table_name));
        view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name));
        IF view_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history view "%.%" is missing; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass), r.view_schema_name, r.view_table_name;
        END IF;
        BEGIN
            IF history_oid IS NULL OR NOT sql_saga.__internal_history_policies_synchronized(base_oid, history_oid) THEN
                RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'policy mismatch';
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": managed history policy set is inconsistent; repair with sync_history_policies',
                sql_saga.__internal_display_name(base_oid::regclass);
        END;
    END LOOP;

    FOR r IN
        SELECT sv.table_schema, sv.table_name, sv.view_schema_name, sv.view_table_name, q.*
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_query_functions(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS q
        ORDER BY sv.table_schema COLLATE "C", sv.table_name COLLATE "C",
                 CASE q.role WHEN 'as_of' THEN 1 WHEN 'between' THEN 2 WHEN 'between_symmetric' THEN 3 ELSE 4 END
    LOOP
        function_oid := pg_catalog.to_regprocedure(r.identity);
        IF function_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" is missing; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
        SELECT c.reltype INTO expected_return_type FROM pg_catalog.pg_class AS c
        WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name));
        IF (SELECT p.prorettype FROM pg_catalog.pg_proc AS p WHERE p.oid = function_oid) IS DISTINCT FROM expected_return_type THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" has a noncanonical return-type dependency; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
        IF EXISTS (
            SELECT FROM pg_catalog.pg_proc AS p
            WHERE p.oid = function_oid
              AND (p.prokind <> 'f' OR p.prolang <> (SELECT l.oid FROM pg_catalog.pg_language AS l WHERE l.lanname = 'sql')
                   OR p.provolatile <> 's' OR p.proisstrict OR p.prosecdef OR p.proleakproof
                   OR p.proparallel <> 'u' OR p.prosrc IS DISTINCT FROM r.body OR p.prosqlbody IS NOT NULL)) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" definition is noncanonical; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
    END LOOP;
END;
$check_integrity$;

REVOKE ALL ON FUNCTION sql_saga.check_integrity() FROM PUBLIC;

CREATE OR REPLACE FUNCTION sql_saga.__internal_unique_key_attribution(unique_key_name name)
RETURNS TABLE(unique_attributable boolean, exclude_attributable boolean, check_attributable boolean)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_unique_key_attribution$
    WITH key AS (
        SELECT uk.*, e.range_column_name, t.oid AS table_oid, tn.oid AS table_namespace_oid
        FROM sql_saga.unique_keys AS uk
        LEFT JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
        LEFT JOIN pg_catalog.pg_namespace AS tn ON tn.nspname = uk.table_schema
        LEFT JOIN pg_catalog.pg_class AS t ON t.relnamespace = tn.oid AND t.relname = uk.table_name
        WHERE uk.unique_key_name = $1
    ), expected AS (
        SELECT k.*, x.expected_attnums,
               pg_catalog.cardinality(x.expected_attnums) = pg_catalog.cardinality(k.column_names) + 1 AS columns_valid
        FROM key AS k
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS expected_attnums
            FROM pg_catalog.unnest(k.column_names || k.range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = k.table_oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS x ON true
    )
    SELECT CASE
               WHEN k.table_oid IS NULL OR NOT k.columns_valid THEN false
               WHEN k.unique_constraint IS NULL THEN true
               WHEN k.key_type = 'predicated' THEN
                   ui.indexrelid IS NOT NULL AND ui.indrelid = k.table_oid
                   AND ui.indisunique AND ui.indisvalid AND ui.indisready
                   AND ui.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                   AND ui.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                   AND ui.indexprs IS NULL AND uam.amname = 'btree'
                   AND pg_catalog.regexp_replace(
                           pg_catalog.regexp_replace(pg_catalog.pg_get_expr(ui.indpred, ui.indrelid),
                               '::(text|character varying)', '', 'g'),
                           '\(([[:alnum:]_".]+)\)', '\1', 'g')
                       IS NOT DISTINCT FROM '(' || k.predicate || ')'
               ELSE uc.oid IS NOT NULL
                   AND uc.contype = CASE k.key_type WHEN 'primary' THEN 'p' ELSE 'u' END
                   AND uc.conkey = k.expected_attnums AND uc.conperiod
                   AND NOT uc.condeferrable AND NOT uc.condeferred
                   AND ui.indrelid = k.table_oid AND ui.indisunique AND ui.indisvalid AND ui.indisready
                   AND ui.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                   AND ui.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                   AND ui.indexprs IS NULL AND ui.indpred IS NULL AND uam.amname = 'gist'
           END,
           CASE
               WHEN k.exclude_constraint IS NULL THEN true
               WHEN k.table_oid IS NULL OR NOT k.columns_valid THEN false
               ELSE ec.oid IS NOT NULL AND ec.contype = 'x' AND ec.conkey = k.expected_attnums
                   AND ec.condeferrable AND NOT ec.condeferred
                   AND ei.indrelid = k.table_oid AND ei.indisvalid AND ei.indisready
                   AND ei.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                   AND ei.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                   AND ei.indexprs IS NULL AND eam.amname = 'gist'
                   AND pg_catalog.regexp_replace(
                           pg_catalog.regexp_replace(pg_catalog.pg_get_expr(ei.indpred, ei.indrelid),
                               '::(text|character varying)', '', 'g'),
                           '\(([[:alnum:]_".]+)\)', '\1', 'g')
                       IS NOT DISTINCT FROM CASE WHEN k.predicate IS NULL THEN NULL ELSE '(' || k.predicate || ')' END
                   AND pg_catalog.cardinality(ec.conexclop) = pg_catalog.cardinality(k.expected_attnums)
                   AND NOT EXISTS (
                       SELECT 1
                       FROM pg_catalog.unnest(ec.conexclop) WITH ORDINALITY AS x(oproid, n)
                       JOIN pg_catalog.pg_operator AS o ON o.oid = x.oproid
                       WHERE (x.n <= pg_catalog.cardinality(k.column_names) AND o.oprname <> '=')
                          OR (x.n = pg_catalog.cardinality(k.expected_attnums) AND o.oprname <> '&&'))
           END,
           k.check_constraint IS NULL
    FROM expected AS k
    LEFT JOIN pg_catalog.pg_constraint AS uc
      ON k.key_type <> 'predicated' AND uc.conrelid = k.table_oid AND uc.conname = k.unique_constraint
    LEFT JOIN pg_catalog.pg_class AS uic
      ON uic.relnamespace = k.table_namespace_oid AND uic.relname = k.unique_constraint
    LEFT JOIN pg_catalog.pg_index AS ui
      ON ui.indexrelid = CASE WHEN k.key_type = 'predicated' THEN uic.oid ELSE uc.conindid END
    LEFT JOIN pg_catalog.pg_class AS uidx ON uidx.oid = ui.indexrelid
    LEFT JOIN pg_catalog.pg_am AS uam ON uam.oid = uidx.relam
    LEFT JOIN pg_catalog.pg_constraint AS ec
      ON k.exclude_constraint IS NOT NULL AND ec.conrelid = k.table_oid AND ec.conname = k.exclude_constraint
    LEFT JOIN pg_catalog.pg_index AS ei ON ei.indexrelid = ec.conindid
    LEFT JOIN pg_catalog.pg_class AS eidx ON eidx.oid = ei.indexrelid
    LEFT JOIN pg_catalog.pg_am AS eam ON eam.oid = eidx.relam;
$__internal_unique_key_attribution$;

COMMENT ON FUNCTION sql_saga.check_integrity() IS
'Validates all catalogued sql_saga families without modifying them, and raises on the first deterministic integrity violation.';

COMMENT ON FUNCTION sql_saga.health_checks() IS
'An event trigger function that runs after DDL commands to check for inconsistencies in sql_saga''s metadata catalogs.';
CREATE OR REPLACE FUNCTION sql_saga.drop_current_view(table_oid regclass, era_name name DEFAULT 'valid', drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT')
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    view_schema name;
    view_name name;
    trigger_name name;
    target_schema_name name;
    target_table_name name;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'table_oid must be specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT n.nspname, c.relname
    INTO target_schema_name, target_table_name
    FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = table_oid;

    FOR view_schema, view_name, trigger_name IN
        DELETE FROM sql_saga.updatable_view AS v
        WHERE (v.table_schema, v.table_name) = (target_schema_name, target_table_name)
          AND v.era_name = era_name
          AND v.view_type = 'current'
        RETURNING v.view_schema, v.view_name, v.trigger_name
    LOOP
        IF to_regclass(format('%I.%I', view_schema, view_name)) IS NOT NULL THEN
            IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = to_regclass(format('%I.%I', view_schema, view_name)) AND t.tgname = trigger_name) THEN
                EXECUTE format('DROP TRIGGER %I ON %I.%I', trigger_name, view_schema, view_name);
            END IF;
            EXECUTE format('DROP VIEW IF EXISTS %I.%I %s', view_schema, view_name, drop_behavior);
        END IF;
    END LOOP;

    RETURN true;
END;
$function$;

COMMENT ON FUNCTION sql_saga.drop_current_view(regclass, name, sql_saga.drop_behavior) IS
'Drops the "current" view associated with a table and era.';
CREATE OR REPLACE FUNCTION sql_saga.__internal_system_versioning_function_is_attributable(table_oid oid, function_identity text, function_role text)
 RETURNS boolean
 LANGUAGE sql
 STABLE
 SET search_path TO 'sql_saga', 'pg_catalog'
AS $function$
    SELECT EXISTS (
        SELECT 1
        FROM sql_saga.__internal_system_versioning_query_functions(table_oid) AS q
        JOIN pg_catalog.pg_proc AS p ON p.oid = pg_catalog.to_regprocedure(q.identity)
        JOIN pg_catalog.pg_class AS base ON base.oid = table_oid
        JOIN sql_saga.system_versioning AS sv
          ON pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name)) = table_oid
        JOIN pg_catalog.pg_class AS result_view
          ON result_view.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.view_schema_name, sv.view_table_name))
        WHERE q.identity = function_identity
          AND q.role = function_role
          AND p.prokind = 'f'
          AND p.prolang = (SELECT l.oid FROM pg_catalog.pg_language AS l WHERE l.lanname = 'sql')
          AND p.prorettype = result_view.reltype
          AND p.proretset
          AND p.provolatile = 's'
          AND NOT p.proisstrict
          AND NOT p.prosecdef
          AND NOT p.proleakproof
          AND p.proparallel = 'u'
          AND p.prosupport = 0
          AND p.procost = 100
          AND p.prorows = 1000
          AND p.proconfig IS NULL
          AND p.proowner = base.relowner
          AND p.prosrc = q.body
          AND p.prosqlbody IS NULL
          AND p.proargnames IS NULL
          AND p.proallargtypes IS NULL
          AND p.proargmodes IS NULL
          AND p.proargtypes = CASE q.role
                WHEN 'as_of' THEN '1184'::pg_catalog.oidvector
                ELSE '1184 1184'::pg_catalog.oidvector
              END
          AND NOT EXISTS (
              SELECT 1
              FROM pg_catalog.aclexplode(coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS x
              WHERE x.grantee NOT IN (0, p.proowner)
                 OR (x.grantee = 0 AND x.privilege_type <> 'EXECUTE'))
          AND EXISTS (
              SELECT 1
              FROM pg_catalog.aclexplode(coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS x
              WHERE x.grantee = 0 AND x.privilege_type = 'EXECUTE')
          AND 1 = (
              SELECT pg_catalog.count(*)
              FROM pg_catalog.pg_depend AS d
              WHERE d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                AND d.objid = p.oid
                AND d.refclassid = 'pg_catalog.pg_type'::pg_catalog.regclass
                AND d.deptype = 'n')
          AND EXISTS (
              SELECT 1
              FROM pg_catalog.pg_depend AS d
              WHERE d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                AND d.objid = p.oid
                AND d.refclassid = 'pg_catalog.pg_type'::pg_catalog.regclass
                AND d.refobjid = result_view.reltype
                AND d.deptype = 'n')
          AND EXISTS (
              SELECT 1 FROM pg_catalog.pg_type AS t
              WHERE t.oid = p.prorettype AND t.typrelid = result_view.oid)
    );
$function$;

CREATE OR REPLACE FUNCTION sql_saga.__internal_trigger_is_attributable(relation_oid oid, trigger_name name, function_identity regprocedure, expected_tgtype integer, expected_argument text DEFAULT NULL::text)
 RETURNS boolean
 LANGUAGE sql
 STABLE
 SET search_path TO 'sql_saga', 'pg_catalog'
AS $function$
    SELECT EXISTS (
        SELECT 1 FROM pg_catalog.pg_trigger AS t
        WHERE t.tgrelid = relation_oid
          AND t.tgname = trigger_name
          AND NOT t.tgisinternal
          AND t.tgfoid = function_identity
          AND t.tgtype = expected_tgtype
          AND CASE WHEN expected_argument IS NULL
                   THEN t.tgnargs = 0
                   ELSE t.tgnargs = 1 AND pg_catalog.encode(t.tgargs, 'escape') = expected_argument || E'\\000'
              END
    );
$function$;

CREATE OR REPLACE FUNCTION sql_saga.__internal_system_versioning_view_is_attributable(table_oid oid, view_oid oid, view_role text)
 RETURNS boolean
 LANGUAGE sql
 STABLE
 SET search_path TO 'sql_saga', 'pg_catalog'
AS $function$
    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class AS base
        JOIN sql_saga.system_versioning AS sv
          ON pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name)) = base.oid
        JOIN pg_catalog.pg_class AS v ON v.oid = view_oid
        WHERE base.oid = table_oid
          AND v.relkind = 'v'
          AND v.relpersistence = 'p'
          AND v.relowner = base.relowner
          AND NOT EXISTS (
              SELECT 1 FROM pg_catalog.aclexplode(coalesce(v.relacl, pg_catalog.acldefault('r', v.relowner))) AS x
              WHERE x.grantee NOT IN (0, v.relowner) OR (x.grantee = 0 AND x.privilege_type <> 'SELECT'))
          AND EXISTS (
              SELECT 1 FROM pg_catalog.aclexplode(coalesce(v.relacl, pg_catalog.acldefault('r', v.relowner))) AS x
              WHERE x.grantee = 0 AND x.privilege_type = 'SELECT')
          AND CASE view_role
              WHEN 'public' THEN
                  v.reloptions = ARRAY['security_invoker=on']::text[]
                  AND EXISTS (
                      SELECT 1 FROM pg_catalog.pg_rewrite AS rw
                      JOIN pg_catalog.pg_depend AS d ON d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND d.objid = rw.oid
                      WHERE rw.ev_class = v.oid AND d.refobjid = table_oid)
                  AND EXISTS (
                      SELECT 1 FROM pg_catalog.pg_rewrite AS rw
                      JOIN pg_catalog.pg_depend AS d ON d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND d.objid = rw.oid
                      WHERE rw.ev_class = v.oid
                        AND d.refobjid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name, sv.history_table_name)))
                  AND EXISTS (
                      SELECT 1 FROM pg_catalog.pg_rewrite AS rw
                      JOIN pg_catalog.pg_depend AS d ON d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND d.objid = rw.oid
                      WHERE rw.ev_class = v.oid
                        AND d.refobjid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name,
                            sql_saga.__internal_history_gated_name(sv.history_table_name))))
              WHEN 'gated' THEN
                  v.reloptions IS NULL
                  AND EXISTS (
                      SELECT 1 FROM pg_catalog.pg_rewrite AS rw
                      JOIN pg_catalog.pg_depend AS d ON d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND d.objid = rw.oid
                      WHERE rw.ev_class = v.oid
                        AND d.refobjid = 'sql_saga.__internal_deny_history(oid)'::pg_catalog.regprocedure)
              ELSE false
              END
    );
$function$;

CREATE OR REPLACE FUNCTION sql_saga.__internal_rebuild_system_versioning_views(table_oid oid, without_columns name[] DEFAULT '{}'::name[])
 RETURNS void
 LANGUAGE plpgsql
 SET search_path TO 'sql_saga', 'pg_catalog'
AS $function$
DECLARE
    sv sql_saga.system_versioning;
    history_table_oid oid;
    table_owner name;
    range_column name;
    gated_view_name name;
    base_projection text;
    history_projection text;
    fn record;
    previous_flag text;
    had_view boolean;
    had_gated boolean;
    had_functions text[];
    base_display text;
BEGIN
    SELECT s.* INTO sv
    FROM sql_saga.system_versioning AS s
    WHERE to_regclass(format('%I.%I', s.table_schema, s.table_name))::oid = table_oid;
    history_table_oid := to_regclass(format('%I.%I', sv.history_schema_name, sv.history_table_name))::oid;
    table_owner := (SELECT c.relowner::regrole::name FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid);
    range_column := (SELECT e.range_column_name FROM sql_saga.era AS e
                     WHERE (e.table_schema, e.table_name, e.era_name) = (sv.table_schema, sv.table_name, sv.era_name));
    gated_view_name := sql_saga.__internal_history_gated_name(sv.history_table_name);

    PERFORM sql_saga.__internal_check_history_columns(table_oid, history_table_oid, without_columns);

    -- The union of both tables' columns, by name: base columns in base order, then history-only ones.
    WITH u AS (
        SELECT coalesce(ba.attname, ha.attname) AS attname, ba.attnum AS base_attnum, ha.attnum AS history_attnum,
               ha.atttypid, ha.atttypmod, ha.attcollation
        FROM (SELECT a.* FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = table_oid AND a.attnum > 0 AND NOT a.attisdropped) AS ba
        FULL JOIN (SELECT a.* FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = history_table_oid AND a.attnum > 0 AND NOT a.attisdropped) AS ha
               ON ha.attname = ba.attname
        WHERE coalesce(ba.attname, ha.attname) <> ALL (without_columns)
    )
    SELECT string_agg(CASE WHEN u.base_attnum IS NOT NULL THEN quote_ident(u.attname)
                           ELSE format('NULL::%s AS %I', sql_saga.__internal_column_type_sql(u.atttypid, u.atttypmod, u.attcollation), u.attname)
                      END, ', ' ORDER BY u.base_attnum NULLS LAST, u.history_attnum),
           string_agg(quote_ident(u.attname), ', ' ORDER BY u.base_attnum NULLS LAST, u.history_attnum)
    INTO base_projection, history_projection
    FROM u;

    previous_flag := current_setting('sql_saga.rebuilding_system_versioning_views', true);
    base_display := sql_saga.__internal_notice_name(sv.table_schema, sv.table_name);
    had_view := to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name)) IS NOT NULL;
    had_gated := to_regclass(format('%I.%I', sv.history_schema_name, gated_view_name)) IS NOT NULL;
    had_functions := ARRAY(SELECT u.f FROM unnest(ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]) AS u (f)
                           WHERE to_regprocedure(u.f) IS NOT NULL);
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', table_oid::text, true);
    BEGIN
        FOR fn IN
            SELECT u.f FROM unnest(ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]) AS u (f)
            WHERE to_regprocedure(u.f) IS NOT NULL
        LOOP
            EXECUTE format('DROP ROUTINE %s', fn.f::regprocedure);
        END LOOP;
        -- add_system_versioning calls this before the views exist.
        IF to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name)) IS NOT NULL THEN
            EXECUTE format('DROP VIEW %I.%I', sv.view_schema_name, sv.view_table_name);
        END IF;
        IF to_regclass(format('%I.%I', sv.history_schema_name, gated_view_name)) IS NOT NULL THEN
            EXECUTE format('DROP VIEW %I.%I', sv.history_schema_name, gated_view_name);
        END IF;

        EXECUTE format('CREATE VIEW %I.%I AS %s', sv.history_schema_name, gated_view_name,
            sql_saga.__internal_history_gated_view_sql(table_oid, history_table_oid, history_projection));
        EXECUTE format('ALTER VIEW %I.%I OWNER TO %I', sv.history_schema_name, gated_view_name, table_owner);
        EXECUTE format('GRANT SELECT ON TABLE %I.%I TO PUBLIC', sv.history_schema_name, gated_view_name);
        IF NOT had_gated THEN
            RAISE NOTICE 'sql_saga: Created view "%" for system-versioned table "%"',
                sql_saga.__internal_notice_name(sv.history_schema_name, gated_view_name), base_display;
        END IF;

        EXECUTE format($view$CREATE VIEW %1$I.%2$I WITH (security_invoker = on) AS
        SELECT %3$s FROM %4$s
        UNION ALL SELECT %5$s FROM %6$I.%7$I
        UNION ALL SELECT %5$s FROM %8$s
            WHERE pg_catalog.row_security_active(%9$L::regclass::oid)
              AND sql_saga.__internal_history_policies_synchronized(%9$L::regclass::oid, %10$L::regclass::oid)$view$,
            sv.view_schema_name, /* %1$I */
            sv.view_table_name, /* %2$I */
            base_projection, /* %3$s */
            sql_saga.__internal_qualified_name(table_oid), /* %4$s */
            history_projection, /* %5$s */
            sv.history_schema_name, /* %6$I */
            gated_view_name, /* %7$I */
            sql_saga.__internal_qualified_name(history_table_oid), /* %8$s */
            sql_saga.__internal_qualified_name(table_oid), /* %9$L */
            sql_saga.__internal_qualified_name(history_table_oid) /* %10$L */
        );
        EXECUTE format('ALTER VIEW %I.%I OWNER TO %I', sv.view_schema_name, sv.view_table_name, table_owner);
        EXECUTE format('GRANT SELECT ON TABLE %I.%I TO PUBLIC', sv.view_schema_name, sv.view_table_name);
        IF NOT had_view THEN
            RAISE NOTICE 'sql_saga: Created view "%" for system-versioned table "%"',
                sql_saga.__internal_notice_name(sv.view_schema_name, sv.view_table_name), base_display;
        END IF;

        -- Each registered signature names a function; its body is fixed by its role.
        FOR fn IN
            SELECT q.identity AS f, q.body
            FROM sql_saga.__internal_system_versioning_query_functions(table_oid) AS q
        LOOP
            EXECUTE format('CREATE FUNCTION %s RETURNS SETOF %I.%I LANGUAGE sql STABLE AS %L',
                fn.f, sv.view_schema_name, sv.view_table_name,
                fn.body);
            EXECUTE format('ALTER FUNCTION %s OWNER TO %I', fn.f::regprocedure, table_owner);
            -- PUBLIC EXECUTE is explicit, because hardened databases revoke the default.
            EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO PUBLIC', fn.f::regprocedure);
            IF fn.f <> ALL (had_functions) THEN
                RAISE NOTICE 'sql_saga: Created function "%" for system-versioned table "%"',
                    sql_saga.__internal_notice_function_name(fn.f::regprocedure), base_display;
            END IF;
        END LOOP;

        -- The fixed ACL is PUBLIC plus the owner. New objects can receive more through
        -- ALTER DEFAULT PRIVILEGES; take those away, so a rebuild always ends in the fixed state.
        FOR fn IN
            SELECT format('REVOKE ALL ON %s %s FROM %s',
                          CASE WHEN o.is_function THEN 'FUNCTION' ELSE 'TABLE' END, o.ident,
                          string_agg(DISTINCT quote_ident(x.grantee::regrole::text), ', ')) AS stmt
            FROM (SELECT false AS is_function, c.oid::regclass::text AS ident, c.relacl AS acl, c.relowner AS owner
                  FROM pg_catalog.pg_class AS c
                  WHERE c.oid IN (to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name)),
                                  to_regclass(format('%I.%I', sv.history_schema_name, gated_view_name)))
                  UNION ALL
                  SELECT true, p.oid::regprocedure::text, p.proacl, p.proowner
                  FROM pg_catalog.pg_proc AS p
                  WHERE p.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
                 ) AS o
            CROSS JOIN LATERAL aclexplode(o.acl) AS x
            WHERE x.grantee NOT IN (0, o.owner)
            GROUP BY o.is_function, o.ident
        LOOP
            EXECUTE fn.stmt;
        END LOOP;
    EXCEPTION WHEN OTHERS THEN
        PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_flag, ''), true);
        RAISE;
    END;
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_flag, ''), true);

    RAISE DEBUG 'sql_saga: generated SYSTEM VERSIONING views and query functions for table %', table_oid::regclass;
END;
$function$;

CREATE OR REPLACE FUNCTION sql_saga.check_integrity()
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'sql_saga', 'pg_catalog'
AS $function$
DECLARE
    r record;
    base_oid oid;
    history_oid oid;
    view_oid oid;
    expected_columns name[];
    function_oid oid;
    expected_return_type oid;
    missing_column name;
BEGIN
    /* Base relation and era invariants are the first dependency layer. */
    FOR r IN
        SELECT e.*,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))::oid AS inventory_table_oid,
               CASE
                   WHEN pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name)) IS NULL THEN 1
                   WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c
                               WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                                 AND (c.relkind <> 'r' OR c.relpersistence <> 'p')) THEN 2
                   WHEN (e.sync_temporal_trg_function_name IS NOT NULL
                         AND e.sync_temporal_trg_function_name <> sql_saga.__internal_make_name(
                             ARRAY[e.table_schema, e.table_name, e.era_name], 'template_sync'))
                     OR EXISTS (SELECT 1 FROM sql_saga.system_time_era AS ste
                                WHERE (ste.table_schema, ste.table_name) = (e.table_schema, e.table_name)
                                  AND NOT EXISTS (SELECT 1 FROM sql_saga.era AS x
                                                  WHERE (x.table_schema, x.table_name, x.era_name) =
                                                        (ste.table_schema, ste.table_name, ste.era_name)))
                     OR EXISTS (SELECT 1 FROM sql_saga.unique_keys AS uk
                                WHERE (uk.table_schema, uk.table_name) = (e.table_schema, e.table_name)
                                  AND NOT EXISTS (SELECT 1 FROM sql_saga.era AS x
                                                  WHERE (x.table_schema, x.table_name, x.era_name) =
                                                        (uk.table_schema, uk.table_name, uk.era_name))) THEN 3
                   WHEN NOT EXISTS (
                       SELECT 1 FROM pg_catalog.pg_attribute AS a
                       JOIN pg_catalog.pg_range AS pr ON pr.rngtypid = e.range_type
                       WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                         AND a.attname = e.range_column_name AND a.attnum > 0 AND NOT a.attisdropped
                         AND a.atttypid = e.range_type AND pr.rngmultitypid = e.multirange_type
                         AND pr.rngsubtype = e.range_subtype
                         AND (SELECT t.typcategory FROM pg_catalog.pg_type AS t WHERE t.oid = e.range_subtype) = e.range_subtype_category) THEN 4
                   WHEN (e.valid_from_column_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_attribute AS a
                            WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND a.attname = e.valid_from_column_name AND a.atttypid = e.range_subtype
                              AND a.attnum > 0 AND NOT a.attisdropped))
                     OR (e.valid_until_column_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_attribute AS a
                            WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND a.attname = e.valid_until_column_name AND a.atttypid = e.range_subtype
                              AND a.attnum > 0 AND NOT a.attisdropped))
                     OR (e.bounds_check_constraint IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND c.conname = e.bounds_check_constraint AND c.contype = 'c'
                              AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '%NOT isempty(%'))
                     OR (e.boundary_check_constraint IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND c.conname = e.boundary_check_constraint AND c.contype = 'c'
                              AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '% < %'))
                     OR (e.sync_temporal_trg_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_trigger AS t
                            WHERE t.tgrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND t.tgname = e.sync_temporal_trg_name AND NOT t.tgisinternal AND t.tgtype = 23
                              AND t.tgfoid = pg_catalog.to_regprocedure(
                                  pg_catalog.format('sql_saga.%I()', e.sync_temporal_trg_function_name)))) THEN 5
               END AS violation
        FROM sql_saga.era AS e
        ORDER BY e.table_schema COLLATE "C", e.table_name COLLATE "C", e.era_name COLLATE "C"
    LOOP
        base_oid := r.inventory_table_oid;
        IF r.violation = 1 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%.%": catalogued base table is missing',
                r.table_schema, r.table_name;
        END IF;
        IF r.violation = 2 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": catalogued base relation kind or persistence is noncanonical; repair with drop_era and add_era',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation = 3 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": catalogue relationships are incoherent; repair with drop_era and add_era',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation = 4 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": column or range type metadata is noncanonical; repair with drop_era and add_era',
                r.era_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation = 5 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": named check or synchronization object is noncanonical; repair with drop_era and add_era',
                r.era_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    /* SYSTEM_TIME invariants have the highest managed-object priority. */
    FOR r IN
        SELECT ste.*, e.range_column_name
        FROM sql_saga.system_time_era AS ste
        JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
        ORDER BY ste.table_schema COLLATE "C", ste.table_name COLLATE "C"
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        IF base_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%.%": catalogued base table is missing', r.table_schema, r.table_name;
        END IF;

        IF NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.generated_always_trigger)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.write_history_trigger)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.truncate_trigger) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME trigger is missing; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        IF NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint AS c
                WHERE c.conrelid = base_oid AND c.conname = r.infinity_check_constraint
                  AND c.contype = 'c'
                  AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '%upper(%''infinity''%')
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.generated_always_trigger,
                'sql_saga.generated_always_as_row_start_end()'::regprocedure, 23, r.range_column_name)
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.write_history_trigger,
                'sql_saga.write_history()'::regprocedure, 25, r.range_column_name)
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.truncate_trigger,
                'sql_saga.truncate_system_versioning()'::regprocedure, 32, NULL) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME trigger definition is noncanonical; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        SELECT x.attname INTO missing_column
        FROM pg_catalog.unnest(r.excluded_column_names) AS x(attname)
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = base_oid AND a.attname = x.attname AND a.attnum > 0 AND NOT a.attisdropped)
        ORDER BY x.attname COLLATE "C" LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": excluded SYSTEM_TIME column "%" is missing; repair with set_system_time_era_excluded_columns or drop_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass), missing_column;
        END IF;

        /* The snapshot detects added and removed columns only; compare it as a set, because a
         * restore recreates inherited children with inherited columns first. */
        SELECT pg_catalog.array_agg(a.attname ORDER BY a.attname COLLATE "C") INTO expected_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = base_oid AND a.attnum > 0 AND NOT a.attisdropped;
        IF expected_columns IS DISTINCT FROM (
                SELECT pg_catalog.array_agg(s.attname ORDER BY s.attname COLLATE "C")
                FROM pg_catalog.unnest(r.table_columns_snapshot) AS s(attname)) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME column snapshot is stale; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT uk.*, i.table_oid, i.object_missing,
               i.unique_attributable, i.exclude_attributable, i.check_attributable
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.__internal_unique_key_inventory() AS i USING (unique_key_name)
        ORDER BY uk.table_schema COLLATE "C", uk.table_name COLLATE "C", uk.unique_key_name COLLATE "C"
    LOOP
        base_oid := r.table_oid;
        IF r.object_missing THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": managed constraint or index is missing; repair with drop_unique_key and add_unique_key',
                r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        IF NOT r.unique_attributable OR NOT r.exclude_attributable OR NOT r.check_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": managed constraint or index is noncanonical; repair with drop_unique_key and add_unique_key',
                r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT fk.*, uk.table_schema AS uk_schema, uk.table_name AS uk_table
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        ORDER BY fk.table_schema COLLATE "C", fk.table_name COLLATE "C", fk.foreign_key_name COLLATE "C"
    LOOP
        IF r.type = 'temporal_to_temporal' THEN
            base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
            IF base_oid IS NULL
               OR NOT sql_saga.__internal_temporal_foreign_key_is_attributable(r.foreign_key_name) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
        ELSE
            base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.uk_schema, r.uk_table));
            IF base_oid IS NULL
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_update_trigger)
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_delete_trigger) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": referenced-side helper trigger is missing; repair with drop_foreign_key and add_regular_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
            IF NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_trigger AS t
                    WHERE t.tgrelid = base_oid AND t.tgname = r.uk_update_trigger
                      AND t.tgfoid = 'sql_saga.uk_update_check_c()'::regprocedure)
               OR NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_trigger AS t
                    WHERE t.tgrelid = base_oid AND t.tgname = r.uk_delete_trigger
                      AND t.tgfoid = 'sql_saga.uk_delete_check_c()'::regprocedure)
               OR NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_constraint AS c
                    WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name))
                      AND c.conname = r.fk_check_constraint AND c.contype = 'c')
               OR NOT sql_saga.__internal_regular_foreign_key_is_attributable(r.foreign_key_name) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
        END IF;
    END LOOP;

    FOR r IN
        SELECT v.* FROM sql_saga.updatable_view AS v
        ORDER BY v.table_schema COLLATE "C", v.table_name COLLATE "C", v.view_type, v.view_schema COLLATE "C", v.view_name COLLATE "C"
    LOOP
        view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema, r.view_name));
        IF view_oid IS NULL
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = view_oid AND t.tgname = r.trigger_name) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": identifier column or trigger is missing; repair with drop_%_view and add_%_view',
                r.view_schema, r.view_name, r.table_schema, r.table_name,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END;
        END IF;
        IF EXISTS (
                SELECT 1
                FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name))
                  AND a.attnum > 0 AND NOT a.attisdropped
                  AND NOT EXISTS (
                      SELECT 1 FROM pg_catalog.pg_attribute AS va
                      WHERE va.attrelid = view_oid AND va.attname = a.attname
                        AND va.atttypid = a.atttypid AND va.attnum > 0 AND NOT va.attisdropped))
           OR NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = view_oid AND t.tgname = r.trigger_name
                  AND NOT t.tgisinternal AND t.tgtype = 93
                  AND t.tgfoid = CASE r.view_type
                      WHEN 'for_portion_of' THEN 'sql_saga.for_portion_of_trigger()'::regprocedure
                      ELSE 'sql_saga.current_view_trigger()'::regprocedure END) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": identifier column or trigger definition is noncanonical; repair with drop_%_view and add_%_view',
                r.view_schema, r.view_name, r.table_schema, r.table_name,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END;
        END IF;
    END LOOP;

    FOR r IN
        SELECT sv.*, a.public_view_attributable, a.gated_view_attributable
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_member_attribution(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS a
        ORDER BY sv.table_schema COLLATE "C", sv.table_name COLLATE "C"
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        history_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.history_schema_name, r.history_table_name));
        view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name));
        IF view_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history view "%.%" is missing; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass), r.view_schema_name, r.view_table_name;
        END IF;
        IF history_oid IS NULL OR NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS h
                WHERE h.oid = history_oid AND h.relkind = 'r' AND h.relpersistence = 'p') THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS b JOIN pg_catalog.pg_class AS h ON h.oid = history_oid
                WHERE b.oid = base_oid AND (NOT h.relrowsecurity OR h.relforcerowsecurity <> b.relforcerowsecurity)) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with sync_history_policies or rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS b JOIN pg_catalog.pg_class AS h ON h.oid = history_oid
                WHERE b.oid = base_oid AND h.relowner <> b.relowner) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS h
                WHERE h.oid = history_oid AND (
                    EXISTS (SELECT 1 FROM pg_catalog.aclexplode(coalesce(h.relacl, pg_catalog.acldefault('r', h.relowner))) AS x
                            WHERE x.grantee NOT IN (0, h.relowner) OR (x.grantee = 0 AND x.privilege_type <> 'SELECT'))
                    OR NOT EXISTS (SELECT 1 FROM pg_catalog.aclexplode(coalesce(h.relacl, pg_catalog.acldefault('r', h.relowner))) AS x
                                   WHERE x.grantee = 0 AND x.privilege_type = 'SELECT')))
           OR NOT r.public_view_attributable
           OR NOT r.gated_view_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        BEGIN
            IF history_oid IS NULL OR NOT sql_saga.__internal_history_policies_synchronized(base_oid, history_oid) THEN
                RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'policy mismatch';
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": managed history policy set is inconsistent; repair with sync_history_policies',
                sql_saga.__internal_display_name(base_oid::regclass);
        END;
    END LOOP;

    FOR r IN
        SELECT sv.table_schema, sv.table_name, sv.view_schema_name, sv.view_table_name, q.*,
               CASE q.role
                   WHEN 'as_of' THEN a.as_of_attributable
                   WHEN 'between' THEN a.between_attributable
                   WHEN 'between_symmetric' THEN a.between_symmetric_attributable
                   ELSE a.from_to_attributable
               END AS definition_attributable
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_member_attribution(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS a
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_query_functions(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS q
        ORDER BY sv.table_schema COLLATE "C", sv.table_name COLLATE "C",
                 CASE q.role WHEN 'as_of' THEN 1 WHEN 'between' THEN 2 WHEN 'between_symmetric' THEN 3 ELSE 4 END
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        function_oid := pg_catalog.to_regprocedure(r.identity);
        IF function_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" is missing; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
        SELECT c.reltype INTO expected_return_type FROM pg_catalog.pg_class AS c
        WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name));
        IF (SELECT p.prorettype FROM pg_catalog.pg_proc AS p WHERE p.oid = function_oid) IS DISTINCT FROM expected_return_type
           OR 1 <> (
                SELECT pg_catalog.count(*)
                FROM pg_catalog.pg_depend AS d
                WHERE d.classid = 'pg_catalog.pg_proc'::regclass
                  AND d.objid = function_oid
                  AND d.refclassid = 'pg_catalog.pg_type'::regclass
                  AND d.deptype = 'n')
           OR NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_depend AS d
                JOIN pg_catalog.pg_type AS t ON t.oid = d.refobjid
                WHERE d.classid = 'pg_catalog.pg_proc'::regclass
                  AND d.objid = function_oid
                  AND d.refclassid = 'pg_catalog.pg_type'::regclass
                  AND d.refobjid = expected_return_type
                  AND d.deptype = 'n'
                  AND t.typrelid = pg_catalog.to_regclass(
                      pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name))) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" has a noncanonical return-type dependency; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
        IF NOT r.definition_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" definition is noncanonical; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
    END LOOP;
END;
$function$;

CREATE OR REPLACE FUNCTION sql_saga.drop_era(table_oid regclass, era_name name DEFAULT 'valid'::name, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior, cleanup boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'sql_saga', 'pg_catalog', 'public'
AS $function$
#variable_conflict use_variable
DECLARE
    era_row sql_saga.era;
    portion_view regclass;
    is_dropped boolean;
    table_schema name;
    table_name name;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = table_oid;

    IF era_name IS NULL THEN
        RAISE EXCEPTION 'no era name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    /*
     * Has the table been dropped already?  This could happen if the period is
     * being dropped by the drop_protection event trigger or through a DROP
     * cascading drop.
     */
    is_dropped := NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid);

    SELECT p.*
    INTO era_row
    FROM sql_saga.era AS p
    WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

    IF NOT FOUND THEN
        RAISE DEBUG 'era % not found for table %', era_name, table_oid;
        RETURN false;
    END IF;

    /* If this is a system_time era, get rid of the triggers */
    DECLARE
        system_time_era_row sql_saga.system_time_era;
    BEGIN
        DELETE FROM sql_saga.system_time_era AS ste
        WHERE (ste.table_schema, ste.table_name, ste.era_name) = (table_schema, table_name, era_name)
        RETURNING * INTO system_time_era_row;

        IF FOUND AND NOT is_dropped THEN
            IF EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = table_oid AND c.conname = system_time_era_row.infinity_check_constraint) THEN EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid, system_time_era_row.infinity_check_constraint); RAISE NOTICE 'sql_saga: Dropped constraint "%" on table "%" for system time era', quote_ident(system_time_era_row.infinity_check_constraint), sql_saga.__internal_notice_name(table_schema, table_name); END IF;
            IF sql_saga.__internal_trigger_is_attributable(table_oid, system_time_era_row.generated_always_trigger, 'sql_saga.generated_always_as_row_start_end()'::regprocedure, 23, era_row.range_column_name) THEN EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.generated_always_trigger, table_oid); RAISE NOTICE 'sql_saga: Dropped trigger "%" on table "%" for system time era', quote_ident(system_time_era_row.generated_always_trigger), sql_saga.__internal_notice_name(table_schema, table_name); ELSIF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = table_oid AND t.tgname = system_time_era_row.generated_always_trigger) THEN RAISE NOTICE 'sql_saga: Left trigger "%" on table "%" in place because it is not the sql_saga-generated trigger', system_time_era_row.generated_always_trigger, sql_saga.__internal_display_name(table_oid); END IF;
            IF sql_saga.__internal_trigger_is_attributable(table_oid, system_time_era_row.write_history_trigger, 'sql_saga.write_history()'::regprocedure, 25, era_row.range_column_name) THEN EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.write_history_trigger, table_oid); RAISE NOTICE 'sql_saga: Dropped trigger "%" on table "%" for system time era', quote_ident(system_time_era_row.write_history_trigger), sql_saga.__internal_notice_name(table_schema, table_name); END IF;
            IF sql_saga.__internal_trigger_is_attributable(table_oid, system_time_era_row.truncate_trigger, 'sql_saga.truncate_system_versioning()'::regprocedure, 32, NULL) THEN EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.truncate_trigger, table_oid); RAISE NOTICE 'sql_saga: Dropped trigger "%" on table "%" for system time era', quote_ident(system_time_era_row.truncate_trigger), sql_saga.__internal_notice_name(table_schema, table_name); END IF;
        END IF;
    END;

    IF drop_behavior = 'RESTRICT' THEN
        /* Check for UNIQUE or PRIMARY KEYs */
        IF EXISTS (
            SELECT FROM sql_saga.unique_keys AS uk
            WHERE (uk.table_schema, uk.table_name, uk.era_name) = (table_schema, table_name, era_name))
        THEN
            RAISE EXCEPTION 'era % is part of a UNIQUE or PRIMARY KEY', era_name;
        END IF;

        /* Check for FOREIGN KEYs */
        IF EXISTS (
            SELECT FROM sql_saga.foreign_keys AS fk
            WHERE (fk.table_schema, fk.table_name, fk.fk_era_name) = (table_schema, table_name, era_name))
        THEN
            RAISE EXCEPTION 'era % is part of a FOREIGN KEY', era_name;
        END IF;

        /* Check for updatable views */
        IF EXISTS (
            SELECT FROM sql_saga.updatable_view AS v
            WHERE (v.table_schema, v.table_name, v.era_name) = (table_schema, table_name, era_name))
        THEN
            RAISE EXCEPTION 'era % is used by an updatable view', era_name;
        END IF;

        /* Check for SYSTEM VERSIONING - must use drop_system_versioning() instead */
        IF EXISTS (
            SELECT FROM sql_saga.system_versioning AS sv
            WHERE (sv.table_schema, sv.table_name, sv.era_name) = (table_schema, table_name, era_name))
        THEN
            RAISE EXCEPTION 'era % has SYSTEM VERSIONING, use drop_system_versioning() instead', era_name;
        END IF;

        /* Drop synchronization trigger BEFORE removing from catalog */
        IF NOT is_dropped AND (era_row.valid_from_column_name IS NOT NULL OR era_row.valid_to_column_name IS NOT NULL) THEN
            CALL sql_saga.drop_synchronize_temporal_columns_trigger(table_oid, era_name);
        END IF;

        /* Remove from catalog */
        DELETE FROM sql_saga.era AS p
        WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

        /* Delete bounds check constraint (NOT isempty(range)). */
        IF NOT is_dropped AND era_row.bounds_check_constraint IS NOT NULL THEN
            IF EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = table_oid AND c.conname = era_row.bounds_check_constraint) THEN EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid, era_row.bounds_check_constraint); END IF;
        END IF;

        /* Delete boundary check constraint (from < until). */
        IF NOT is_dropped AND era_row.boundary_check_constraint IS NOT NULL THEN
            IF EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = table_oid AND c.conname = era_row.boundary_check_constraint) THEN EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid, era_row.boundary_check_constraint); END IF;
        END IF;

        /* Drop column default if it was set */
        IF NOT is_dropped AND NOT era_row.trigger_applies_defaults AND era_row.valid_until_column_name IS NOT NULL THEN
            EXECUTE format('ALTER TABLE %s ALTER COLUMN %I DROP DEFAULT', table_oid, era_row.valid_until_column_name);
        END IF;

        /* Delete columns if purging */
        IF NOT is_dropped AND cleanup THEN
            DECLARE
                drop_clauses text[] := '{}';
            BEGIN
                -- The range column is always present for an era.
                drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.range_column_name);

                -- Drop from/until if they were part of the era.
                IF era_row.valid_from_column_name IS NOT NULL THEN
                    drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_from_column_name);
                    drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_until_column_name);
                END IF;

                -- Drop to if it was part of the era.
                IF era_row.valid_to_column_name IS NOT NULL THEN
                    drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_to_column_name);
                END IF;

                IF array_length(drop_clauses, 1) > 0 THEN
                     EXECUTE format('ALTER TABLE %s %s', table_oid, array_to_string(drop_clauses, ', '));
                     IF era_name = 'system_time' THEN RAISE NOTICE 'sql_saga: Dropped column "%" from table "%" for system time era', quote_ident(era_row.range_column_name), sql_saga.__internal_notice_name(table_schema, table_name); END IF;
                END IF;
            END;
        END IF;

        RETURN true;
    END IF;

    /* We must be in cascading-drop mode now */

    PERFORM sql_saga.drop_for_portion_of_view(table_oid, era_name, drop_behavior, cleanup);

    PERFORM sql_saga.drop_foreign_key_by_name(table_oid, fk.foreign_key_name)
    FROM sql_saga.foreign_keys AS fk
    WHERE (fk.table_schema, fk.table_name, fk.fk_era_name) = (table_schema, table_name, era_name);

    PERFORM sql_saga.drop_unique_key_by_name(table_oid, uk.unique_key_name, drop_behavior, cleanup)
    FROM sql_saga.unique_keys AS uk
    WHERE (uk.table_schema, uk.table_name, uk.era_name) = (table_schema, table_name, era_name);

    /* Drop synchronization trigger BEFORE removing from catalog */
    IF NOT is_dropped AND (era_row.valid_from_column_name IS NOT NULL OR era_row.valid_to_column_name IS NOT NULL) THEN
        CALL sql_saga.drop_synchronize_temporal_columns_trigger(table_oid, era_name);
    END IF;

    /* Remove from catalog */
    DELETE FROM sql_saga.era AS p
    WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

    /* Delete bounds check constraint (NOT isempty(range)). */
    IF NOT is_dropped AND era_row.bounds_check_constraint IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid /* %s */, era_row.bounds_check_constraint /* %I */);
    END IF;

    /* Delete boundary check constraint (from < until). */
    IF NOT is_dropped AND era_row.boundary_check_constraint IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid /* %s */, era_row.boundary_check_constraint /* %I */);
    END IF;

    /* Drop column default if it was set */
    IF NOT is_dropped AND NOT era_row.trigger_applies_defaults AND era_row.valid_until_column_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s ALTER COLUMN %I DROP DEFAULT', table_oid, era_row.valid_until_column_name);
    END IF;

    /* Delete columns if purging */
    IF NOT is_dropped AND cleanup THEN
        DECLARE
            drop_clauses text[] := '{}';
        BEGIN
            -- The range column is always present for an era.
            drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.range_column_name);

            -- Drop from/until if they were part of the era.
            IF era_row.valid_from_column_name IS NOT NULL THEN
                drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_from_column_name);
                drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_until_column_name);
            END IF;

            -- Drop to if it was part of the era.
            IF era_row.valid_to_column_name IS NOT NULL THEN
                drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_to_column_name);
            END IF;

            IF array_length(drop_clauses, 1) > 0 THEN
                 EXECUTE format('ALTER TABLE %s %s', table_oid, array_to_string(drop_clauses, ', '));
                 IF era_name = 'system_time' THEN RAISE NOTICE 'sql_saga: Dropped column "%" from table "%" for system time era', quote_ident(era_row.range_column_name), sql_saga.__internal_notice_name(table_schema, table_name); END IF;
            END IF;
        END;
    END IF;

    RETURN true;
END;
$function$;

CREATE OR REPLACE FUNCTION sql_saga.drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior, cleanup boolean DEFAULT true)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'sql_saga', 'pg_catalog', 'public'
AS $function$
#variable_conflict use_variable
DECLARE
    system_versioning_row sql_saga.system_versioning;
    is_dropped boolean;
    table_schema name;
    table_name name;
    base_oid oid;
    public_view_oid oid;
    gated_view_oid oid;
    as_of_attributable boolean;
    between_attributable boolean;
    between_symmetric_attributable boolean;
    from_to_attributable boolean;
    public_view_attributable boolean;
    gated_view_attributable boolean;
    function_depends_on_public_view boolean;
    function_display text;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = table_oid;

    SELECT sv.* INTO system_versioning_row
    FROM sql_saga.system_versioning AS sv
    WHERE (sv.table_schema, sv.table_name) = (table_schema, table_name)
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE DEBUG 'table % does not have SYSTEM VERSIONING', table_oid;
        RETURN false;
    END IF;

    base_oid := table_oid::oid;
    public_view_oid := to_regclass(format('%I.%I', system_versioning_row.view_schema_name, system_versioning_row.view_table_name));
    gated_view_oid := to_regclass(format('%I.%I', system_versioning_row.history_schema_name,
        sql_saga.__internal_history_gated_name(system_versioning_row.history_table_name)));
    SELECT a.as_of_attributable, a.between_attributable,
           a.between_symmetric_attributable, a.from_to_attributable,
           a.public_view_attributable, a.gated_view_attributable
    INTO as_of_attributable, between_attributable,
         between_symmetric_attributable, from_to_attributable,
         public_view_attributable, gated_view_attributable
    FROM sql_saga.__internal_system_versioning_member_attribution(base_oid) AS a;

    DELETE FROM sql_saga.system_versioning AS sv
    WHERE (sv.table_schema, sv.table_name) = (table_schema, table_name);

    is_dropped := NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid);

    IF NOT is_dropped THEN
        SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc AS p
            JOIN pg_catalog.pg_type AS t ON t.oid = p.prorettype
            WHERE p.oid = to_regprocedure(system_versioning_row.func_as_of)
              AND t.typrelid = public_view_oid)
        INTO function_depends_on_public_view;
        IF NOT as_of_attributable AND to_regprocedure(system_versioning_row.func_as_of) IS NOT NULL
           AND NOT function_depends_on_public_view THEN
            RAISE NOTICE 'sql_saga: Left function "%" in place because it is not the sql_saga-generated function for table "%"',
                system_versioning_row.func_as_of, sql_saga.__internal_display_name(table_oid);
        ELSIF NOT between_attributable AND to_regprocedure(system_versioning_row.func_between) IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: Left function "%" in place because it is not the sql_saga-generated function for table "%"',
                system_versioning_row.func_between, sql_saga.__internal_display_name(table_oid);
        ELSIF NOT between_symmetric_attributable AND to_regprocedure(system_versioning_row.func_between_symmetric) IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: Left function "%" in place because it is not the sql_saga-generated function for table "%"',
                system_versioning_row.func_between_symmetric, sql_saga.__internal_display_name(table_oid);
        ELSIF NOT from_to_attributable AND to_regprocedure(system_versioning_row.func_from_to) IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: Left function "%" in place because it is not the sql_saga-generated function for table "%"',
                system_versioning_row.func_from_to, sql_saga.__internal_display_name(table_oid);
        ELSIF NOT public_view_attributable AND public_view_oid IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: Left view "%" in place because it is not the sql_saga-generated view for table "%"',
                public_view_oid::regclass, sql_saga.__internal_display_name(table_oid);
        END IF;
        IF as_of_attributable THEN function_display := sql_saga.__internal_notice_function_name(to_regprocedure(system_versioning_row.func_as_of)); EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_as_of, drop_behavior); RAISE NOTICE 'sql_saga: Dropped function "%" of system-versioned table "%"', function_display, sql_saga.__internal_notice_name(table_schema, table_name); END IF;
        IF between_attributable THEN function_display := sql_saga.__internal_notice_function_name(to_regprocedure(system_versioning_row.func_between)); EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between, drop_behavior); RAISE NOTICE 'sql_saga: Dropped function "%" of system-versioned table "%"', function_display, sql_saga.__internal_notice_name(table_schema, table_name); END IF;
        IF between_symmetric_attributable THEN function_display := sql_saga.__internal_notice_function_name(to_regprocedure(system_versioning_row.func_between_symmetric)); EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between_symmetric, drop_behavior); RAISE NOTICE 'sql_saga: Dropped function "%" of system-versioned table "%"', function_display, sql_saga.__internal_notice_name(table_schema, table_name); END IF;
        IF from_to_attributable THEN function_display := sql_saga.__internal_notice_function_name(to_regprocedure(system_versioning_row.func_from_to)); EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_from_to, drop_behavior); RAISE NOTICE 'sql_saga: Dropped function "%" of system-versioned table "%"', function_display, sql_saga.__internal_notice_name(table_schema, table_name); END IF;
        IF public_view_attributable THEN
            EXECUTE format('DROP VIEW %I.%I %s', system_versioning_row.view_schema_name, system_versioning_row.view_table_name, drop_behavior);
            RAISE NOTICE 'sql_saga: Dropped view "%" of system-versioned table "%"', sql_saga.__internal_notice_name(system_versioning_row.view_schema_name, system_versioning_row.view_table_name), sql_saga.__internal_notice_name(table_schema, table_name);
        END IF;
        IF gated_view_attributable THEN
            EXECUTE format('DROP VIEW %I.%I %s', system_versioning_row.history_schema_name,
                sql_saga.__internal_history_gated_name(system_versioning_row.history_table_name), drop_behavior);
            RAISE NOTICE 'sql_saga: Dropped view "%" of system-versioned table "%"', sql_saga.__internal_notice_name(system_versioning_row.history_schema_name, sql_saga.__internal_history_gated_name(system_versioning_row.history_table_name)), sql_saga.__internal_notice_name(table_schema, table_name);
        END IF;
    END IF;

    IF NOT is_dropped AND cleanup THEN
        PERFORM sql_saga.drop_system_time_era(table_oid, drop_behavior, cleanup);
        IF to_regclass(format('%I.%I', system_versioning_row.history_schema_name, system_versioning_row.history_table_name)) IS NOT NULL THEN
            EXECUTE format('DROP TABLE %I.%I %s', system_versioning_row.history_schema_name, system_versioning_row.history_table_name, drop_behavior);
            RAISE NOTICE 'sql_saga: Dropped history table "%" of system-versioned table "%"', sql_saga.__internal_notice_name(system_versioning_row.history_schema_name, system_versioning_row.history_table_name), sql_saga.__internal_notice_name(table_schema, table_name);
        END IF;
    END IF;

    RETURN true;
END;
$function$;

CREATE OR REPLACE FUNCTION sql_saga.drop_unique_key_by_name(table_oid regclass, key_name name, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior, cleanup boolean DEFAULT true)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'sql_saga', 'pg_catalog', 'public'
AS $function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    foreign_key_row sql_saga.foreign_keys;
    unique_key_row sql_saga.unique_keys;
    unique_object_attributable boolean;
    exclude_object_attributable boolean;
    check_object_attributable boolean;
    range_column_name name;
    expected_attnums smallint[];
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    FOR unique_key_row IN
        SELECT uk.*
        FROM sql_saga.unique_keys AS uk
        WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name)
          AND (uk.unique_key_name = key_name OR key_name IS NULL)
    LOOP
        RAISE DEBUG 'drop_unique_key_by_name: Processing key: %', row_to_json(unique_key_row);

        IF unique_key_row.key_type IN ('primary', 'natural')
           AND unique_key_row.exclude_constraint IS NULL
           AND unique_key_row.check_constraint IS NULL THEN
            SELECT e.range_column_name
            INTO range_column_name
            FROM sql_saga.era AS e
            WHERE (e.table_schema, e.table_name, e.era_name) =
                  (unique_key_row.table_schema, unique_key_row.table_name, unique_key_row.era_name);

            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n)
            INTO expected_attnums
            FROM pg_catalog.unnest(unique_key_row.column_names || range_column_name)
                 WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = table_oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped;

            SELECT uc.oid IS NOT NULL
                   AND pg_catalog.cardinality(expected_attnums) =
                       pg_catalog.cardinality(unique_key_row.column_names) + 1
                   AND uc.contype = CASE unique_key_row.key_type WHEN 'primary' THEN 'p' ELSE 'u' END
                   AND uc.conkey = expected_attnums AND uc.conperiod
                   AND NOT uc.condeferrable AND NOT uc.condeferred
                   AND ui.indrelid = table_oid AND ui.indisunique AND ui.indisvalid AND ui.indisready
                   AND ui.indnkeyatts = pg_catalog.cardinality(expected_attnums)
                   AND ui.indkey::text = pg_catalog.array_to_string(expected_attnums, ' ')
                   AND ui.indexprs IS NULL AND ui.indpred IS NULL AND am.amname = 'gist'
            INTO unique_object_attributable
            FROM pg_catalog.pg_constraint AS uc
            JOIN pg_catalog.pg_index AS ui ON ui.indexrelid = uc.conindid
            JOIN pg_catalog.pg_class AS idx ON idx.oid = ui.indexrelid
            JOIN pg_catalog.pg_am AS am ON am.oid = idx.relam
            WHERE uc.conrelid = table_oid AND uc.conname = unique_key_row.unique_constraint;

            unique_object_attributable := coalesce(unique_object_attributable, false);
            exclude_object_attributable := true;
            check_object_attributable := true;
        ELSE
            SELECT a.unique_attributable, a.exclude_attributable, a.check_attributable
            INTO unique_object_attributable, exclude_object_attributable, check_object_attributable
            FROM sql_saga.__internal_unique_key_attribution(unique_key_row.unique_key_name) AS a;
        END IF;
        /* Propagate the requested drop behavior to foreign keys, if desired */
        FOR foreign_key_row IN
            SELECT fk.*
            FROM sql_saga.foreign_keys AS fk
            WHERE fk.unique_key_name = unique_key_row.unique_key_name
        LOOP
            IF drop_behavior = 'RESTRICT' THEN
                RAISE EXCEPTION 'cannot drop unique key "%" because foreign key "%" on table "%" depends on it',
                    unique_key_row.unique_key_name, foreign_key_row.foreign_key_name,
                    sql_saga.__internal_display_name(format('%I.%I', foreign_key_row.table_schema, foreign_key_row.table_name)::regclass);
            END IF;

            PERFORM sql_saga.drop_foreign_key_by_name(NULL, foreign_key_row.foreign_key_name);
        END LOOP;

        DELETE FROM sql_saga.unique_keys AS uk
        WHERE uk.unique_key_name = unique_key_row.unique_key_name;

        /* If purging, drop the underlying constraints unless the table has been dropped */
        IF cleanup AND EXISTS (
            SELECT FROM pg_catalog.pg_class AS c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE (n.nspname, c.relname) = (unique_key_row.table_schema, unique_key_row.table_name))
        THEN
            IF unique_key_row.check_constraint IS NOT NULL AND check_object_attributable THEN
                EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT IF EXISTS %I',
                    unique_key_row.table_schema, unique_key_row.table_name, unique_key_row.check_constraint);
            ELSIF unique_key_row.check_constraint IS NOT NULL
                  AND EXISTS (SELECT FROM pg_catalog.pg_constraint AS c
                             WHERE c.conrelid = table_oid AND c.conname = unique_key_row.check_constraint) THEN
                RAISE NOTICE 'sql_saga: Left constraint "%" on table "%" in place because it is not the sql_saga-managed constraint',
                    unique_key_row.check_constraint, sql_saga.__internal_display_name(table_oid);
            END IF;
            IF unique_key_row.mutually_exclusive_columns IS NOT NULL THEN
                DECLARE
                    alter_parts text[];
                    object_name name;
                BEGIN
                    -- Build a list of constraints to drop in a single ALTER TABLE command.
                    alter_parts := ARRAY[]::text[];

                    -- No partial constraints or indexes to drop for XOR keys using NULLS NOT DISTINCT.

                    -- 3. Execute the ALTER TABLE command for all constraints.
                    IF cardinality(alter_parts) > 0 THEN
                        EXECUTE format('ALTER TABLE %I.%I %s',
                            unique_key_row.table_schema,
                            unique_key_row.table_name,
                            array_to_string(alter_parts, ', ')
                        );
                    END IF;
                END;
            ELSIF unique_key_row.key_type = 'predicated' THEN
                BEGIN
                    IF unique_object_attributable THEN
                        IF unique_key_row.unique_constraint IS NOT NULL THEN
                            EXECUTE format('DROP INDEX %I.%I', unique_key_row.table_schema, unique_key_row.unique_constraint);
                        END IF;
                    ELSIF to_regclass(format('%I.%I', unique_key_row.table_schema, unique_key_row.unique_constraint)) IS NOT NULL THEN
                        RAISE NOTICE 'sql_saga: Left index "%" on table "%" in place because it is not the sql_saga-managed index',
                            unique_key_row.unique_constraint, sql_saga.__internal_display_name(table_oid);
                    END IF;
                    IF exclude_object_attributable AND unique_key_row.exclude_constraint IS NOT NULL THEN
                        EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I', unique_key_row.table_schema,
                            unique_key_row.table_name, unique_key_row.exclude_constraint);
                    ELSIF EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = table_oid AND c.conname = unique_key_row.exclude_constraint) THEN
                        RAISE NOTICE 'sql_saga: Left constraint "%" on table "%" in place because it is not the sql_saga-managed constraint',
                            unique_key_row.exclude_constraint, sql_saga.__internal_display_name(table_oid);
                    END IF;
                END;
            ELSE -- 'primary' or 'natural'
                DECLARE
                    alter_parts text[] := '{}';
                BEGIN
                    -- This is a unique/primary key constraint, drop both constraints if they exist.
                    IF unique_key_row.unique_constraint IS NOT NULL AND unique_object_attributable THEN
                        alter_parts := alter_parts || format('DROP CONSTRAINT IF EXISTS %I', unique_key_row.unique_constraint);
                    ELSIF unique_key_row.unique_constraint IS NOT NULL
                          AND EXISTS (SELECT FROM pg_catalog.pg_constraint AS c
                                     WHERE c.conrelid = table_oid AND c.conname = unique_key_row.unique_constraint) THEN
                        RAISE NOTICE 'sql_saga: Left constraint "%" on table "%" in place because it is not the sql_saga-managed constraint',
                            unique_key_row.unique_constraint, sql_saga.__internal_display_name(table_oid);
                    ELSIF unique_key_row.unique_constraint IS NOT NULL
                          AND to_regclass(format('%I.%I', unique_key_row.table_schema, unique_key_row.unique_constraint)) IS NOT NULL THEN
                        RAISE NOTICE 'sql_saga: Left index "%" on table "%" in place because it is not the sql_saga-managed index',
                            unique_key_row.unique_constraint, sql_saga.__internal_display_name(table_oid);
                    END IF;
                    IF unique_key_row.exclude_constraint IS NOT NULL AND exclude_object_attributable THEN
                        alter_parts := alter_parts || format('DROP CONSTRAINT IF EXISTS %I', unique_key_row.exclude_constraint);
                    ELSIF unique_key_row.exclude_constraint IS NOT NULL
                          AND EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = table_oid AND c.conname = unique_key_row.exclude_constraint) THEN
                        RAISE NOTICE 'sql_saga: Left constraint "%" on table "%" in place because it is not the sql_saga-managed constraint',
                            unique_key_row.exclude_constraint, sql_saga.__internal_display_name(table_oid);
                    END IF;

                    IF cardinality(alter_parts) > 0 THEN
                        EXECUTE format('ALTER TABLE %I.%I %s',
                            unique_key_row.table_schema,
                            unique_key_row.table_name,
                            array_to_string(alter_parts, ', ')
                        );
                    END IF;
                END;
            END IF;
        END IF;
    END LOOP;

END;
$function$;

CREATE OR REPLACE FUNCTION sql_saga.rebuild_system_versioning_views(table_oid regclass, without_columns name[] DEFAULT '{}'::name[])
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public'
AS $function$
DECLARE
    sv sql_saga.system_versioning;
    history_table_oid oid;
    range_column name;
    excluded name;
    previous_rebuild_flag text;
    caller_role name;
    authorization_table_name name;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    SELECT c.relname INTO authorization_table_name FROM pg_catalog.pg_class AS c
    WHERE c.oid OPERATOR(pg_catalog.=) table_oid;
    caller_role := CASE WHEN current_setting('role') = 'none' THEN session_user ELSE current_setting('role') END;
    IF NOT pg_catalog.pg_has_role(caller_role, (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid OPERATOR(pg_catalog.=) table_oid), 'USAGE') THEN
        RAISE EXCEPTION 'must be owner of table %', authorization_table_name USING ERRCODE = 'insufficient_privilege';
    END IF;

    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT s.* INTO sv
    FROM sql_saga.system_versioning AS s
    WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', s.table_schema, s.table_name)) OPERATOR(pg_catalog.=) table_oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'table % does not have SYSTEM VERSIONING', table_oid;
    END IF;

    history_table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name, sv.history_table_name))::pg_catalog.oid;
    range_column := (SELECT e.range_column_name FROM sql_saga.era AS e
                     WHERE (e.table_schema, e.table_name, e.era_name) OPERATOR(pg_catalog.=) (sv.table_schema, sv.table_name, sv.era_name));

    without_columns := coalesce(without_columns, '{}');
    IF pg_catalog.array_position(without_columns, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: without_columns must not contain NULL';
    END IF;
    FOR excluded IN
        SELECT u.c FROM pg_catalog.unnest(without_columns) WITH ORDINALITY AS u (c, n)
        GROUP BY u.c HAVING pg_catalog.count(*) OPERATOR(pg_catalog.>) 1 ORDER BY pg_catalog.min(u.n)
    LOOP
        RAISE EXCEPTION 'sql_saga: column % is listed more than once in without_columns', pg_catalog.quote_ident(excluded);
    END LOOP;
    FOR excluded IN
        SELECT u.c FROM pg_catalog.unnest(without_columns) WITH ORDINALITY AS u (c, n)
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                          WHERE a.attrelid IN (table_oid, history_table_oid) AND a.attname OPERATOR(pg_catalog.=) u.c
                            AND a.attnum OPERATOR(pg_catalog.>) 0 AND NOT a.attisdropped)
        ORDER BY u.n
    LOOP
        RAISE EXCEPTION 'sql_saga: column % does not exist in either % or %',
            pg_catalog.quote_ident(excluded), table_oid, history_table_oid::pg_catalog.regclass;
    END LOOP;
    IF range_column OPERATOR(pg_catalog.=) ANY (without_columns) THEN
        RAISE EXCEPTION 'sql_saga: cannot exclude system time column % from SYSTEM VERSIONING views for table %',
            pg_catalog.quote_ident(range_column), table_oid;
    END IF;

    -- Checked here, in the caller's search_path, so the error names relations as the writer's does.
    PERFORM sql_saga.__internal_check_history_columns(table_oid, history_table_oid, without_columns);

    PERFORM sql_saga.__internal_rebuild_system_versioning_views(table_oid, without_columns);

    -- Use the existing authorized reconciler as the private refresh gateway.
    previous_rebuild_flag := current_setting('sql_saga.rebuilding_system_versioning_views', true);
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', table_oid::oid::text, true);
    BEGIN
        PERFORM sql_saga.__internal_reconcile_system_versioning_family(table_oid, without_columns);
    EXCEPTION WHEN OTHERS THEN
        PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_rebuild_flag, ''), true);
        RAISE;
    END;
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_rebuild_flag, ''), true);

    RAISE NOTICE 'sql_saga: Rebuilt SYSTEM VERSIONING views and query functions for table %', table_oid;
END;
$function$;

CREATE OR REPLACE FUNCTION sql_saga.__internal_unique_key_object_is_attributable(
    unique_key_name name,
    object_kind text)
RETURNS boolean
LANGUAGE plpgsql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_unique_key_object_is_attributable$
#variable_conflict use_variable
DECLARE
    uk sql_saga.unique_keys;
    table_oid oid;
    range_column name;
    expected_attnums smallint[];
    object_oid oid;
    attributable boolean;
BEGIN
    SELECT x.* INTO uk FROM sql_saga.unique_keys AS x
    WHERE x.unique_key_name = $1;
    IF NOT FOUND THEN RETURN false; END IF;

    IF object_kind = 'exclude' AND uk.exclude_constraint IS NULL THEN RETURN true; END IF;
    IF object_kind = 'check' THEN RETURN uk.check_constraint IS NULL; END IF;

    IF object_kind = 'unique' AND uk.key_type <> 'predicated' THEN
        table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name));
        IF table_oid IS NULL THEN RETURN false; END IF;
        SELECT e.range_column_name INTO range_column
        FROM sql_saga.era AS e
        WHERE (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name);
        IF range_column IS NULL THEN RETURN false; END IF;
        SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n)
        INTO expected_attnums
        FROM pg_catalog.unnest(uk.column_names || range_column) WITH ORDINALITY AS u(column_name, n)
        JOIN pg_catalog.pg_attribute AS a
          ON a.attrelid = table_oid AND a.attname = u.column_name
         AND a.attnum > 0 AND NOT a.attisdropped;
        IF pg_catalog.cardinality(expected_attnums) IS DISTINCT FROM pg_catalog.cardinality(uk.column_names) + 1 THEN
            RETURN false;
        END IF;
        RETURN EXISTS (
            SELECT 1
            FROM pg_catalog.pg_constraint AS c
            JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.conindid
            JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
            JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
            WHERE c.conrelid = table_oid AND c.conname = uk.unique_constraint
              AND c.contype = CASE uk.key_type WHEN 'primary' THEN 'p' ELSE 'u' END
              AND c.conkey = expected_attnums AND c.conperiod
              AND NOT c.condeferrable AND NOT c.condeferred
              AND i.indrelid = table_oid AND i.indisunique AND i.indisvalid AND i.indisready
              AND i.indnkeyatts = pg_catalog.cardinality(expected_attnums)
              AND i.indkey::text = pg_catalog.array_to_string(expected_attnums, ' ')
              AND i.indexprs IS NULL AND i.indpred IS NULL AND am.amname = 'gist');
    END IF;

    SELECT CASE object_kind
        WHEN 'unique' THEN a.unique_attributable
        WHEN 'exclude' THEN a.exclude_attributable
        WHEN 'check' THEN a.check_attributable
        ELSE false
    END
    INTO attributable
    FROM sql_saga.__internal_unique_key_attribution(unique_key_name) AS a;
    RETURN attributable;
END;
$__internal_unique_key_object_is_attributable$;

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

/*
 * Final integrity gate. Runs after every definition and privilege change above, inside the
 * extension update transaction: a damaged managed family raises the checker's exact message and
 * the whole update rolls back, leaving the previous version installed. Repair as the message says
 * and run ALTER EXTENSION sql_saga UPDATE again.
 */
SELECT sql_saga.check_integrity();
