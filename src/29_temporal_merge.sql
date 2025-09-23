-- The main public procedure for temporal_merge. It orchestrates the planning
-- and execution of the merge operation.
CREATE OR REPLACE PROCEDURE sql_saga.temporal_merge(
    target_table regclass,
    source_table regclass,
    identity_columns TEXT[],
    mode sql_saga.temporal_merge_mode DEFAULT 'MERGE_ENTITY_PATCH',
    era_name name DEFAULT 'valid',
    row_id_column name DEFAULT 'row_id',
    founding_id_column name DEFAULT NULL,
    update_source_with_identity BOOLEAN DEFAULT false,
    natural_identity_columns TEXT[] DEFAULT NULL,
    delete_mode sql_saga.temporal_merge_delete_mode DEFAULT 'NONE',
    update_source_with_feedback BOOLEAN DEFAULT false,
    feedback_status_column name DEFAULT NULL,
    feedback_status_key name DEFAULT NULL,
    feedback_error_column name DEFAULT NULL,
    feedback_error_key name DEFAULT NULL,
    ephemeral_columns TEXT[] DEFAULT NULL
)
LANGUAGE plpgsql AS $temporal_merge$
DECLARE
    v_log_trace boolean;
    v_log_sql boolean;
    v_log_plan boolean;
    v_log_id TEXT;
    v_summary_line TEXT;
BEGIN
    -- This is the main user-facing procedure. It is responsible for:
    -- 1. Setting up session-level state (temp tables, logging flags).
    -- 2. Calling the planner to generate the execution plan.
    -- 3. Calling the executor to apply the plan to the database.

    v_log_trace := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_trace', true), ''), 'false')::boolean;
    v_log_sql := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_sql', true), ''), 'false')::boolean;
    v_log_plan := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_plan', true), ''), 'false')::boolean;
    v_log_id := substr(md5(COALESCE(current_setting('sql_saga.temporal_merge.log_id_seed', true), random()::text)), 1, 3);

    v_summary_line := format(
        'on %s: mode=>%s, delete_mode=>%s, identity_columns=>%L, natural_identity_columns=>%L, ephemeral_columns=>%L, founding_id_column=>%L, row_id_column=>%L',
        temporal_merge.target_table,
        temporal_merge.mode,
        temporal_merge.delete_mode,
        temporal_merge.identity_columns,
        temporal_merge.natural_identity_columns,
        temporal_merge.ephemeral_columns,
        temporal_merge.founding_id_column,
        temporal_merge.row_id_column
    );

    -- The plan table is created here and passed implicitly to the planner and executor.
    IF to_regclass('pg_temp.temporal_merge_plan') IS NOT NULL THEN
        DROP TABLE pg_temp.temporal_merge_plan;
    END IF;
    CREATE TEMP TABLE temporal_merge_plan (LIKE sql_saga.temporal_merge_plan, PRIMARY KEY (plan_op_seq)) ON COMMIT DROP;

    -- Step 1: Generate the plan.
    INSERT INTO temporal_merge_plan
    SELECT * FROM sql_saga.temporal_merge_plan(
        target_table => temporal_merge.target_table,
        source_table => temporal_merge.source_table,
        identity_columns => temporal_merge.identity_columns,
        mode => temporal_merge.mode,
        era_name => temporal_merge.era_name,
        row_id_column => temporal_merge.row_id_column,
        founding_id_column => temporal_merge.founding_id_column,
        delete_mode => temporal_merge.delete_mode,
        natural_identity_columns => temporal_merge.natural_identity_columns,
        ephemeral_columns => temporal_merge.ephemeral_columns,
        p_log_trace => v_log_trace,
        p_log_sql => v_log_sql
    );

    -- Conditionally output the plan for debugging.
    DECLARE
        v_plan_rec RECORD;
    BEGIN
        IF v_log_plan THEN
            RAISE NOTICE 'temporal_merge plan (%) %', v_log_id, v_summary_line;
            FOR v_plan_rec IN SELECT * FROM temporal_merge_plan ORDER BY plan_op_seq LOOP
                RAISE NOTICE '(%) %', v_log_id, json_build_object(
                    'plan_op_seq', v_plan_rec.plan_op_seq,
                    'row_ids', v_plan_rec.row_ids,
                    'operation', v_plan_rec.operation,
                    'update_effect', v_plan_rec.update_effect,
                    'entity_ids', v_plan_rec.entity_ids,
                    'old_valid_from', v_plan_rec.old_valid_from,
                    'old_valid_until', v_plan_rec.old_valid_until,
                    'new_valid_from', v_plan_rec.new_valid_from,
                    'new_valid_until', v_plan_rec.new_valid_until,
                    'data', v_plan_rec.data,
                    's_t_relation', v_plan_rec.s_t_relation,
                    'b_a_relation', v_plan_rec.b_a_relation,
                    'trace', v_plan_rec.trace
                );
            END LOOP;
        END IF;
    END;

    -- Step 2: Execute the plan.
    CALL sql_saga.temporal_merge_execute(
        target_table => temporal_merge.target_table,
        source_table => temporal_merge.source_table,
        identity_columns => temporal_merge.identity_columns,
        mode => temporal_merge.mode,
        era_name => temporal_merge.era_name,
        row_id_column => temporal_merge.row_id_column,
        founding_id_column => temporal_merge.founding_id_column,
        update_source_with_identity => temporal_merge.update_source_with_identity,
        natural_identity_columns => temporal_merge.natural_identity_columns,
        delete_mode => temporal_merge.delete_mode,
        update_source_with_feedback => temporal_merge.update_source_with_feedback,
        feedback_status_column => temporal_merge.feedback_status_column,
        feedback_status_key => temporal_merge.feedback_status_key,
        feedback_error_column => temporal_merge.feedback_error_column,
        feedback_error_key => temporal_merge.feedback_error_key,
        ephemeral_columns => temporal_merge.ephemeral_columns
    );
END;
$temporal_merge$;

COMMENT ON PROCEDURE sql_saga.temporal_merge(regclass, regclass, TEXT[], sql_saga.temporal_merge_mode, name, name, name, boolean, text[], sql_saga.temporal_merge_delete_mode, boolean, name, name, name, name, text[]) IS
'Executes a set-based temporal merge operation. It generates a plan using temporal_merge_plan and then executes it.';

