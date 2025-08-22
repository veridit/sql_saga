/**
 * sql_saga.c -
 *
 * Core architectural pattern for complex temporal updates:
 *
 * Due to PostgreSQL's MVCC rules for constraint triggers, multi-statement
 * transactions that are only valid at commit time cannot be reliably validated.
 *
 * The correct solution is the "Plan and Execute" pattern, implemented in a
 * single C function using SPI. This function will:
 * 1. (Plan) Read all source and target data to calculate a complete and
 *    correct DML plan (DELETEs, UPDATEs, INSERTs).
 * 2. (Execute) Apply this plan using a crucial "add-then-modify" order.
 *    New timeline segments must be INSERTed before old ones are UPDATE_d or
 *    DELETE_d. This ensures the trigger's statement-level snapshot contains
 *    all the necessary rows for validation to succeed.
 *
 * From PostgreSQL's perspective, this is a single statement. All deferred
 * triggers fire at the end, validating a state that the planner has already
 * guaranteed is consistent. This is the strategic direction for future API
 * development.
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "mb/pg_wchar.h"

#include "sql_saga.h"

#define NAMEARRAYOID 1003

PG_MODULE_MAGIC;

#define MAX_FK_COLS 16

typedef struct FkValidationPlan
{
	char		fk_name[NAMEDATALEN];
	SPIPlanPtr	plan;
	int			nargs;
	Oid			argtypes[MAX_FK_COLS + 2]; /* FK cols + range start/end */
	int			param_attnums[MAX_FK_COLS + 2]; /* attnums in heap tuple */
} FkValidationPlan;

static HTAB *fk_plan_cache = NULL;

static void
init_fk_plan_cache(void)
{
	HASHCTL ctl;

	if (fk_plan_cache)
		return;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(FkValidationPlan);
	/* Lifetime of cache is transaction */
	ctl.hcxt = CurTransactionContext;
	fk_plan_cache = hash_create("sql_saga fk validation plan cache", 16, &ctl, HASH_ELEM | HASH_STRINGS);
}

static HTAB *uk_delete_plan_cache = NULL;

static void
init_uk_delete_plan_cache(void)
{
	HASHCTL ctl;

	if (uk_delete_plan_cache)
		return;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(FkValidationPlan); /* Reusing struct */
	ctl.hcxt = CurTransactionContext;
	uk_delete_plan_cache = hash_create("sql_saga uk delete validation plan cache", 16, &ctl, HASH_ELEM | HASH_STRINGS);
}

#define MAX_UK_UPDATE_PLAN_ARGS (2 * MAX_FK_COLS + 4)

typedef struct UkUpdateValidationPlan
{
	char		fk_name[NAMEDATALEN];
	SPIPlanPtr	plan;
	int			nargs;
	Oid			argtypes[MAX_UK_UPDATE_PLAN_ARGS];
	int			num_uk_cols;
	int			param_attnums_old[MAX_FK_COLS + 2];
	int			param_attnums_new[MAX_FK_COLS + 2];
} UkUpdateValidationPlan;

static HTAB *uk_update_plan_cache = NULL;

static void
init_uk_update_plan_cache(void)
{
	HASHCTL ctl;

	if (uk_update_plan_cache)
		return;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(UkUpdateValidationPlan);
	ctl.hcxt = CurTransactionContext;
	uk_update_plan_cache = hash_create("sql_saga uk update validation plan cache", 16, &ctl, HASH_ELEM | HASH_STRINGS);
}

static SPIPlanPtr get_range_type_plan = NULL;

/* For NAMEARRAYOID type IO */
static Oid namearray_input_func_oid = InvalidOid;
static Oid namearray_ioparam_oid = InvalidOid;

/* Function definitions */

PG_FUNCTION_INFO_V1(fk_insert_check_c);
PG_FUNCTION_INFO_V1(fk_update_check_c);
PG_FUNCTION_INFO_V1(uk_delete_check_c);
PG_FUNCTION_INFO_V1(uk_update_check_c);

Datum
fk_insert_check_c(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata;
	HeapTuple	rettuple;
	Relation	rel;
	TupleDesc	tupdesc;
	HeapTuple	new_row;
	char	  **tgargs;
	char *foreign_key_name;
	char *fk_column_names_str;
	char *fk_valid_from_column_name;
	char *fk_valid_until_column_name;
	char *uk_schema_name;
	char *uk_table_name;
	char *uk_column_names_str;
	char *uk_era_name;
	char *uk_valid_from_column_name;
	char *uk_valid_until_column_name;
	char *match_type;
	char *fk_schema_name;
	char *fk_table_name;
	char *fk_era_name;

	FkValidationPlan *plan_entry;
	bool found;
	int ret;
	bool isnull, okay;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "fk_insert_check_c: not called by trigger manager");

	trigdata = (TriggerData *) fcinfo->context;
	rettuple = trigdata->tg_trigtuple;
	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;
	new_row = trigdata->tg_trigtuple;

	if (trigdata->tg_trigger->tgnargs != 16)
		elog(ERROR, "fk_insert_check_c: expected 16 arguments, got %d", trigdata->tg_trigger->tgnargs);

	tgargs = trigdata->tg_trigger->tgargs;

	foreign_key_name = tgargs[0];
	fk_schema_name = tgargs[1];
	fk_table_name = tgargs[2];
	fk_column_names_str = tgargs[3];
	fk_era_name = tgargs[4];
	fk_valid_from_column_name = tgargs[5];
	fk_valid_until_column_name = tgargs[6];
	uk_schema_name = tgargs[7];
	uk_table_name = tgargs[8];
	uk_column_names_str = tgargs[9];
	uk_era_name = tgargs[10];
	uk_valid_from_column_name = tgargs[11];
	uk_valid_until_column_name = tgargs[12];
	match_type = tgargs[13];

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	init_fk_plan_cache();
	plan_entry = (FkValidationPlan *) hash_search(fk_plan_cache, foreign_key_name, HASH_ENTER, &found);
	
	if (!found)
	{
		char *fk_range_constructor;
		char *uk_range_constructor;
		char *query;
		Datum get_range_type_values[3];
		StringInfoData where_buf;
		Datum uk_column_names_datum, fk_column_names_datum;
		ArrayType *uk_column_names_array, *fk_column_names_array;
		int num_uk_cols, num_fk_cols;
		Datum *uk_col_datums, *fk_col_datums;
		int i, param_idx = 0;

		/* Get range constructor types from sql_saga.era */
		if (get_range_type_plan == NULL)
		{
			const char *sql = "SELECT range_type::regtype::text FROM sql_saga.era WHERE table_schema = $1 AND table_name = $2 AND era_name = $3";
			Oid plan_argtypes[] = { NAMEOID, NAMEOID, NAMEOID };
				
			get_range_type_plan = SPI_prepare(sql, 3, plan_argtypes);
			if (get_range_type_plan == NULL)
				elog(ERROR, "SPI_prepare for get_range_type failed: %s", SPI_result_code_string(SPI_result));

			ret = SPI_keepplan(get_range_type_plan);
			if (ret != 0)
				elog(ERROR, "SPI_keepplan for get_range_type failed: %s", SPI_result_code_string(ret));
		}
			
		get_range_type_values[0] = CStringGetDatum(fk_schema_name);
		get_range_type_values[1] = CStringGetDatum(fk_table_name);
		get_range_type_values[2] = CStringGetDatum(fk_era_name);
			
		ret = SPI_execute_plan(get_range_type_plan, get_range_type_values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for foreign key table %s.%s era %s", fk_schema_name, fk_table_name, fk_era_name);
		fk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		get_range_type_values[0] = CStringGetDatum(uk_schema_name);
		get_range_type_values[1] = CStringGetDatum(uk_table_name);
		get_range_type_values[2] = CStringGetDatum(uk_era_name);
		ret = SPI_execute_plan(get_range_type_plan, get_range_type_values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for unique key table %s.%s era %s", uk_schema_name, uk_table_name, uk_era_name);
		uk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		/* Build parameterized where clause and collect param info */
		initStringInfo(&where_buf);

		if (namearray_input_func_oid == InvalidOid)
		{
			/*
			 * We only need the input function and ioparam for NAMEARRAYOID,
			 * so we cache them in static variables to avoid repeated catalog
			 * lookups. Other type data is not needed.
			 */
			int16	typlen;
			bool	typbyval;
			char	typalign;
			char	typdelim;
			get_type_io_data(NAMEARRAYOID, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &namearray_ioparam_oid, &namearray_input_func_oid);
		}

		uk_column_names_datum = OidInputFunctionCall(namearray_input_func_oid, uk_column_names_str, namearray_ioparam_oid, -1);
		uk_column_names_array = DatumGetArrayTypeP(uk_column_names_datum);
		deconstruct_array(uk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &uk_col_datums, NULL, &num_uk_cols);
		fk_column_names_datum = OidInputFunctionCall(namearray_input_func_oid, fk_column_names_str, namearray_ioparam_oid, -1);
		fk_column_names_array = DatumGetArrayTypeP(fk_column_names_datum);
		deconstruct_array(fk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &fk_col_datums, NULL, &num_fk_cols);

		if (num_fk_cols > MAX_FK_COLS)
			elog(ERROR, "Number of foreign key columns (%d) exceeds MAX_FK_COLS (%d)", num_fk_cols, MAX_FK_COLS);
		plan_entry->nargs = num_fk_cols + 2;

		for (i = 0; i < num_uk_cols; i++)
		{
			char *ukc = NameStr(*DatumGetName(uk_col_datums[i]));
			char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
			int attnum = SPI_fnumber(tupdesc, fkc);

			if (attnum <= 0)
				elog(ERROR, "column \"%s\" does not exist in table \"%s\"", fkc, RelationGetRelationName(rel));
			if (i > 0)
				appendStringInfoString(&where_buf, " AND ");
			appendStringInfo(&where_buf, "uk.%s = $%d", quote_identifier(ukc), param_idx + 1);
			plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, attnum);
			plan_entry->param_attnums[param_idx] = attnum;
			param_idx++;
		}
		pfree(DatumGetPointer(uk_column_names_datum));
		if (num_uk_cols > 0) pfree(uk_col_datums);
		pfree(DatumGetPointer(fk_column_names_datum));
		if (num_fk_cols > 0) pfree(fk_col_datums);

		/* Add range params */
		plan_entry->param_attnums[param_idx] = SPI_fnumber(tupdesc, fk_valid_from_column_name);
		plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums[param_idx]);
		param_idx++;
		plan_entry->param_attnums[param_idx] = SPI_fnumber(tupdesc, fk_valid_until_column_name);
		plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums[param_idx]);
		
		query = psprintf(
			"SELECT COALESCE(("
			"  SELECT sql_saga.covers_without_gaps("
			"    %s(uk.%s, uk.%s),"
			"    %s($%d, $%d)"
			"    ORDER BY uk.%s"
			"  )"
			"  FROM %s.%s AS uk"
			"  WHERE %s"
			"), false)",
			uk_range_constructor,
			quote_identifier(uk_valid_from_column_name), quote_identifier(uk_valid_until_column_name),
			fk_range_constructor, num_fk_cols + 1, num_fk_cols + 2,
			quote_identifier(uk_valid_from_column_name),
			quote_identifier(uk_schema_name), quote_identifier(uk_table_name),
			where_buf.data
		);
		plan_entry->plan = SPI_prepare(query, plan_entry->nargs, plan_entry->argtypes);
		if (plan_entry->plan == NULL)
			elog(ERROR, "SPI_prepare for validation query failed: %s", SPI_result_code_string(SPI_result));
		if (SPI_keepplan(plan_entry->plan))
			elog(ERROR, "SPI_keepplan for validation query failed");
		pfree(query);
		pfree(where_buf.data);
		if(fk_range_constructor) pfree(fk_range_constructor);
		if(uk_range_constructor) pfree(uk_range_constructor);
	}

	/* Check for NULLs in FK columns using cached attnums */
	{
		bool has_nulls = false;
		bool all_nulls = true;
		int i;
		int num_fk_cols = plan_entry->nargs - 2;

		for (i = 0; i < num_fk_cols; i++)
		{
			(void) heap_getattr(new_row, plan_entry->param_attnums[i], tupdesc, &isnull);
			if (isnull)
				has_nulls = true;
			else
				all_nulls = false;
		}

		if (all_nulls)
		{
			SPI_finish();
			return PointerGetDatum(rettuple);
		}
		if (has_nulls)
		{
			if (strcmp(match_type, "SIMPLE") == 0)
			{
				SPI_finish();
				return PointerGetDatum(rettuple);
			}
			else if (strcmp(match_type, "PARTIAL") == 0)
				ereport(ERROR, (errmsg("MATCH PARTIAL is not implemented")));
			else if (strcmp(match_type, "FULL") == 0)
				ereport(ERROR, (errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
					errmsg("insert or update on table \"%s\" violates foreign key constraint \"%s\" (MATCH FULL with NULLs)",
					RelationGetRelationName(rel), foreign_key_name)));
		}
	}

	/* Execute validation query */
	{
		Datum values[MAX_FK_COLS + 2];
		char nulls[MAX_FK_COLS + 2];
		int i;

		for (i = 0; i < plan_entry->nargs; i++)
		{
			values[i] = heap_getattr(new_row, plan_entry->param_attnums[i], tupdesc, &isnull);
			nulls[i] = isnull ? 'n' : ' ';
		}
		
		ret = SPI_execute_plan(plan_entry->plan, values, nulls, true, 1);
		if (ret != SPI_OK_SELECT)
			elog(ERROR, "SPI_execute_plan failed");
		
		if (SPI_processed > 0)
		{
			okay = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
			if (isnull) okay = false;
		}
		else
			okay = false;
	}

	if (!okay)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
				 errmsg("insert or update on table \"%s.%s\" violates foreign key constraint \"%s\"",
						tgargs[1], tgargs[2], foreign_key_name)));
	}

	SPI_finish();
	return PointerGetDatum(rettuple);
}


Datum
fk_update_check_c(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata;
	HeapTuple	rettuple;
	Relation	rel;
	TupleDesc	tupdesc;
	HeapTuple	new_row;
	char	  **tgargs;
	char *foreign_key_name;
	char *fk_column_names_str;
	char *fk_valid_from_column_name;
	char *fk_valid_until_column_name;
	char *uk_schema_name;
	char *uk_table_name;
	char *uk_column_names_str;
	char *uk_era_name;
	char *uk_valid_from_column_name;
	char *uk_valid_until_column_name;
	char *match_type;
	char *fk_schema_name;
	char *fk_table_name;
	char *fk_era_name;

	FkValidationPlan *plan_entry;
	bool found;
	int ret;
	bool isnull, okay;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "fk_update_check_c: not called by trigger manager");

	trigdata = (TriggerData *) fcinfo->context;
	rettuple = trigdata->tg_newtuple;
	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;
	new_row = trigdata->tg_newtuple;

	if (trigdata->tg_trigger->tgnargs != 16)
		elog(ERROR, "fk_update_check_c: expected 16 arguments, got %d", trigdata->tg_trigger->tgnargs);

	tgargs = trigdata->tg_trigger->tgargs;

	foreign_key_name = tgargs[0];
	fk_schema_name = tgargs[1];
	fk_table_name = tgargs[2];
	fk_column_names_str = tgargs[3];
	fk_era_name = tgargs[4];
	fk_valid_from_column_name = tgargs[5];
	fk_valid_until_column_name = tgargs[6];
	uk_schema_name = tgargs[7];
	uk_table_name = tgargs[8];
	uk_column_names_str = tgargs[9];
	uk_era_name = tgargs[10];
	uk_valid_from_column_name = tgargs[11];
	uk_valid_until_column_name = tgargs[12];
	match_type = tgargs[13];

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	init_fk_plan_cache();
	plan_entry = (FkValidationPlan *) hash_search(fk_plan_cache, foreign_key_name, HASH_ENTER, &found);
	
	if (!found)
	{
		char *fk_range_constructor;
		char *uk_range_constructor;
		char *query;
		Datum get_range_type_values[3];
		StringInfoData where_buf;
		Datum uk_column_names_datum, fk_column_names_datum;
		ArrayType *uk_column_names_array, *fk_column_names_array;
		int num_uk_cols, num_fk_cols;
		Datum *uk_col_datums, *fk_col_datums;
		int i, param_idx = 0;

		/* Get range constructor types from sql_saga.era */
		if (get_range_type_plan == NULL)
		{
			const char *sql = "SELECT range_type::regtype::text FROM sql_saga.era WHERE table_schema = $1 AND table_name = $2 AND era_name = $3";
			Oid plan_argtypes[] = { NAMEOID, NAMEOID, NAMEOID };
				
			get_range_type_plan = SPI_prepare(sql, 3, plan_argtypes);
			if (get_range_type_plan == NULL)
				elog(ERROR, "SPI_prepare for get_range_type failed: %s", SPI_result_code_string(SPI_result));

			ret = SPI_keepplan(get_range_type_plan);
			if (ret != 0)
				elog(ERROR, "SPI_keepplan for get_range_type failed: %s", SPI_result_code_string(ret));
		}
			
		get_range_type_values[0] = CStringGetDatum(fk_schema_name);
		get_range_type_values[1] = CStringGetDatum(fk_table_name);
		get_range_type_values[2] = CStringGetDatum(fk_era_name);
			
		ret = SPI_execute_plan(get_range_type_plan, get_range_type_values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for foreign key table %s.%s era %s", fk_schema_name, fk_table_name, fk_era_name);
		fk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		get_range_type_values[0] = CStringGetDatum(uk_schema_name);
		get_range_type_values[1] = CStringGetDatum(uk_table_name);
		get_range_type_values[2] = CStringGetDatum(uk_era_name);
		ret = SPI_execute_plan(get_range_type_plan, get_range_type_values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for unique key table %s.%s era %s", uk_schema_name, uk_table_name, uk_era_name);
		uk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		/* Build parameterized where clause and collect param info */
		initStringInfo(&where_buf);

		if (namearray_input_func_oid == InvalidOid)
		{
			/*
			 * We only need the input function and ioparam for NAMEARRAYOID,
			 * so we cache them in static variables to avoid repeated catalog
			 * lookups. Other type data is not needed.
			 */
			int16	typlen;
			bool	typbyval;
			char	typalign;
			char	typdelim;
			get_type_io_data(NAMEARRAYOID, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &namearray_ioparam_oid, &namearray_input_func_oid);
		}

		uk_column_names_datum = OidInputFunctionCall(namearray_input_func_oid, uk_column_names_str, namearray_ioparam_oid, -1);
		uk_column_names_array = DatumGetArrayTypeP(uk_column_names_datum);
		deconstruct_array(uk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &uk_col_datums, NULL, &num_uk_cols);
		fk_column_names_datum = OidInputFunctionCall(namearray_input_func_oid, fk_column_names_str, namearray_ioparam_oid, -1);
		fk_column_names_array = DatumGetArrayTypeP(fk_column_names_datum);
		deconstruct_array(fk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &fk_col_datums, NULL, &num_fk_cols);

		if (num_fk_cols > MAX_FK_COLS)
			elog(ERROR, "Number of foreign key columns (%d) exceeds MAX_FK_COLS (%d)", num_fk_cols, MAX_FK_COLS);
		plan_entry->nargs = num_fk_cols + 2;

		for (i = 0; i < num_uk_cols; i++)
		{
			char *ukc = NameStr(*DatumGetName(uk_col_datums[i]));
			char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
			int attnum = SPI_fnumber(tupdesc, fkc);

			if (attnum <= 0)
				elog(ERROR, "column \"%s\" does not exist in table \"%s\"", fkc, RelationGetRelationName(rel));
			if (i > 0)
				appendStringInfoString(&where_buf, " AND ");
			appendStringInfo(&where_buf, "uk.%s = $%d", quote_identifier(ukc), param_idx + 1);
			plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, attnum);
			plan_entry->param_attnums[param_idx] = attnum;
			param_idx++;
		}
		pfree(DatumGetPointer(uk_column_names_datum));
		if (num_uk_cols > 0) pfree(uk_col_datums);
		pfree(DatumGetPointer(fk_column_names_datum));
		if (num_fk_cols > 0) pfree(fk_col_datums);

		/* Add range params */
		plan_entry->param_attnums[param_idx] = SPI_fnumber(tupdesc, fk_valid_from_column_name);
		plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums[param_idx]);
		param_idx++;
		plan_entry->param_attnums[param_idx] = SPI_fnumber(tupdesc, fk_valid_until_column_name);
		plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums[param_idx]);
		
		query = psprintf(
			"SELECT COALESCE(("
			"  SELECT sql_saga.covers_without_gaps("
			"    %s(uk.%s, uk.%s),"
			"    %s($%d, $%d)"
			"    ORDER BY uk.%s"
			"  )"
			"  FROM %s.%s AS uk"
			"  WHERE %s"
			"), false)",
			uk_range_constructor,
			quote_identifier(uk_valid_from_column_name), quote_identifier(uk_valid_until_column_name),
			fk_range_constructor, num_fk_cols + 1, num_fk_cols + 2,
			quote_identifier(uk_valid_from_column_name),
			quote_identifier(uk_schema_name), quote_identifier(uk_table_name),
			where_buf.data
		);
		plan_entry->plan = SPI_prepare(query, plan_entry->nargs, plan_entry->argtypes);
		if (plan_entry->plan == NULL)
			elog(ERROR, "SPI_prepare for validation query failed: %s", SPI_result_code_string(SPI_result));
		if (SPI_keepplan(plan_entry->plan))
			elog(ERROR, "SPI_keepplan for validation query failed");
		pfree(query);
		pfree(where_buf.data);
		if(fk_range_constructor) pfree(fk_range_constructor);
		if(uk_range_constructor) pfree(uk_range_constructor);
	}

	/* Check for NULLs in FK columns using cached attnums */
	{
		bool has_nulls = false;
		bool all_nulls = true;
		int i;
		int num_fk_cols = plan_entry->nargs - 2;

		for (i = 0; i < num_fk_cols; i++)
		{
			(void) heap_getattr(new_row, plan_entry->param_attnums[i], tupdesc, &isnull);
			if (isnull)
				has_nulls = true;
			else
				all_nulls = false;
		}

		if (all_nulls)
		{
			SPI_finish();
			return PointerGetDatum(rettuple);
		}
		if (has_nulls)
		{
			if (strcmp(match_type, "SIMPLE") == 0)
			{
				SPI_finish();
				return PointerGetDatum(rettuple);
			}
			else if (strcmp(match_type, "PARTIAL") == 0)
				ereport(ERROR, (errmsg("MATCH PARTIAL is not implemented")));
			else if (strcmp(match_type, "FULL") == 0)
				ereport(ERROR, (errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
					errmsg("insert or update on table \"%s\" violates foreign key constraint \"%s\" (MATCH FULL with NULLs)",
					RelationGetRelationName(rel), foreign_key_name)));
		}
	}

	/* Execute validation query */
	{
		Datum values[MAX_FK_COLS + 2];
		char nulls[MAX_FK_COLS + 2];
		int i;

		for (i = 0; i < plan_entry->nargs; i++)
		{
			values[i] = heap_getattr(new_row, plan_entry->param_attnums[i], tupdesc, &isnull);
			nulls[i] = isnull ? 'n' : ' ';
		}
		
		ret = SPI_execute_plan(plan_entry->plan, values, nulls, true, 1);
		if (ret != SPI_OK_SELECT)
			elog(ERROR, "SPI_execute_plan failed");
		
		if (SPI_processed > 0)
		{
			okay = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
			if (isnull) okay = false;
		}
		else
			okay = false;
	}

	if (!okay)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
				 errmsg("insert or update on table \"%s.%s\" violates foreign key constraint \"%s\"",
						tgargs[1], tgargs[2], foreign_key_name)));
	}

	SPI_finish();
	return PointerGetDatum(rettuple);
}

Datum
uk_delete_check_c(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata;
	HeapTuple	rettuple;
	Relation	rel;
	TupleDesc	tupdesc;
	HeapTuple	old_row;
	char	  **tgargs;
	char *foreign_key_name;
	char *fk_schema_name;
	char *fk_table_name;
	char *fk_column_names_str;
	char *fk_era_name;
	char *fk_valid_from_column_name;
	char *fk_valid_until_column_name;
	char *uk_schema_name;
	char *uk_table_name;
	char *uk_column_names_str;
	char *uk_era_name;
	char *uk_valid_from_column_name;
	char *uk_valid_until_column_name;

	FkValidationPlan *plan_entry;
	bool found;
	int ret;
	bool isnull, violation;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "uk_delete_check_c: not called by trigger manager");

	trigdata = (TriggerData *) fcinfo->context;
	rettuple = trigdata->tg_trigtuple;
	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;
	old_row = trigdata->tg_trigtuple;

	if (trigdata->tg_trigger->tgnargs != 16)
		elog(ERROR, "uk_delete_check_c: expected 16 arguments, got %d", trigdata->tg_trigger->tgnargs);

	tgargs = trigdata->tg_trigger->tgargs;

	foreign_key_name = tgargs[0];
	fk_schema_name = tgargs[1];
	fk_table_name = tgargs[2];
	fk_column_names_str = tgargs[3];
	fk_era_name = tgargs[4];
	fk_valid_from_column_name = tgargs[5];
	fk_valid_until_column_name = tgargs[6];
	uk_schema_name = tgargs[7];
	uk_table_name = tgargs[8];
	uk_column_names_str = tgargs[9];
	uk_era_name = tgargs[10];
	uk_valid_from_column_name = tgargs[11];
	uk_valid_until_column_name = tgargs[12];

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	init_uk_delete_plan_cache();
	plan_entry = (FkValidationPlan *) hash_search(uk_delete_plan_cache, foreign_key_name, HASH_ENTER, &found);

	if (!found)
	{
		char *fk_range_constructor;
		char *uk_range_constructor;
		char *query;
		Datum get_range_type_values[3];
		StringInfoData join_buf, where_buf, exclude_buf;
		int i;
		Datum uk_column_names_datum, fk_column_names_datum;
		ArrayType *uk_column_names_array, *fk_column_names_array;
		int num_uk_cols, num_fk_cols;
		Datum *uk_col_datums, *fk_col_datums;
		int param_idx = 0;

		/* Get range constructor types from sql_saga.era */
		if (get_range_type_plan == NULL)
		{
			const char *sql = "SELECT range_type::regtype::text FROM sql_saga.era WHERE table_schema = $1 AND table_name = $2 AND era_name = $3";
			Oid plan_argtypes[] = { NAMEOID, NAMEOID, NAMEOID };
			get_range_type_plan = SPI_prepare(sql, 3, plan_argtypes);
			if (get_range_type_plan == NULL)
				elog(ERROR, "SPI_prepare for get_range_type failed: %s", SPI_result_code_string(SPI_result));
			ret = SPI_keepplan(get_range_type_plan);
			if (ret != 0)
				elog(ERROR, "SPI_keepplan for get_range_type failed: %s", SPI_result_code_string(ret));
		}

		get_range_type_values[0] = CStringGetDatum(fk_schema_name);
		get_range_type_values[1] = CStringGetDatum(fk_table_name);
		get_range_type_values[2] = CStringGetDatum(fk_era_name);
		ret = SPI_execute_plan(get_range_type_plan, get_range_type_values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for foreign key table %s.%s era %s", fk_schema_name, fk_table_name, fk_era_name);
		fk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		get_range_type_values[0] = CStringGetDatum(uk_schema_name);
		get_range_type_values[1] = CStringGetDatum(uk_table_name);
		get_range_type_values[2] = CStringGetDatum(uk_era_name);
		ret = SPI_execute_plan(get_range_type_plan, get_range_type_values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for unique key table %s era %s", quote_identifier(RelationGetRelationName(rel)), uk_era_name);
		uk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		/* Build clauses and collect param info */
		initStringInfo(&join_buf);
		initStringInfo(&where_buf);
		initStringInfo(&exclude_buf);

		if (namearray_input_func_oid == InvalidOid)
		{
			/*
			 * We only need the input function and ioparam for NAMEARRAYOID,
			 * so we cache them in static variables to avoid repeated catalog
			 * lookups. Other type data is not needed.
			 */
			int16	typlen;
			bool	typbyval;
			char	typalign;
			char	typdelim;
			get_type_io_data(NAMEARRAYOID, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &namearray_ioparam_oid, &namearray_input_func_oid);
		}

		uk_column_names_datum = OidInputFunctionCall(namearray_input_func_oid, uk_column_names_str, namearray_ioparam_oid, -1);
		uk_column_names_array = DatumGetArrayTypeP(uk_column_names_datum);
		deconstruct_array(uk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &uk_col_datums, NULL, &num_uk_cols);

		fk_column_names_datum = OidInputFunctionCall(namearray_input_func_oid, fk_column_names_str, namearray_ioparam_oid, -1);
		fk_column_names_array = DatumGetArrayTypeP(fk_column_names_datum);
		deconstruct_array(fk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &fk_col_datums, NULL, &num_fk_cols);

		if (num_fk_cols > MAX_FK_COLS)
			elog(ERROR, "Number of foreign key columns (%d) exceeds MAX_FK_COLS (%d)", num_fk_cols, MAX_FK_COLS);
		plan_entry->nargs = num_uk_cols + 2;

		appendStringInfoString(&exclude_buf, " AND NOT (");
		for (i = 0; i < num_uk_cols; i++)
		{
			char *ukc = NameStr(*DatumGetName(uk_col_datums[i]));
			char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
			int attnum = SPI_fnumber(tupdesc, ukc);
			if (attnum <= 0)
				elog(ERROR, "column \"%s\" does not exist in table \"%s\"", ukc, RelationGetRelationName(rel));
			if (i > 0)
			{
				appendStringInfoString(&join_buf, " AND ");
				appendStringInfoString(&where_buf, " AND ");
				appendStringInfoString(&exclude_buf, " AND ");
			}
			appendStringInfo(&join_buf, "fk.%s = uk.%s", quote_identifier(fkc), quote_identifier(ukc));
			appendStringInfo(&where_buf, "fk.%s = $%d", quote_identifier(fkc), param_idx + 1);
			appendStringInfo(&exclude_buf, "uk.%s = $%d", quote_identifier(ukc), param_idx + 1);
			plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, attnum);
			plan_entry->param_attnums[param_idx] = attnum;
			param_idx++;
		}
		
		/* Add era columns to exclude clause and params */
		if (num_uk_cols > 0) appendStringInfoString(&exclude_buf, " AND ");
		plan_entry->param_attnums[param_idx] = SPI_fnumber(tupdesc, uk_valid_from_column_name);
		plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums[param_idx]);
		appendStringInfo(&exclude_buf, "uk.%s = $%d", quote_identifier(uk_valid_from_column_name), param_idx + 1);
		param_idx++;
		appendStringInfoString(&exclude_buf, " AND ");
		plan_entry->param_attnums[param_idx] = SPI_fnumber(tupdesc, uk_valid_until_column_name);
		plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums[param_idx]);
		appendStringInfo(&exclude_buf, "uk.%s = $%d", quote_identifier(uk_valid_until_column_name), param_idx + 1);
		appendStringInfoChar(&exclude_buf, ')');
		
		pfree(DatumGetPointer(uk_column_names_datum));
		if (num_uk_cols > 0) pfree(uk_col_datums);
		pfree(DatumGetPointer(fk_column_names_datum));
		if (num_fk_cols > 0) pfree(fk_col_datums);

		query = psprintf(
			"SELECT EXISTS ("
			"  SELECT 1"
			"  FROM %s.%s AS fk"
			"  WHERE %s AND COALESCE(NOT ("
			"    SELECT sql_saga.covers_without_gaps("
			"      %s(uk.%s, uk.%s),"
			"      %s(fk.%s, fk.%s)"
			"      ORDER BY uk.%s"
			"    )"
			"    FROM %s.%s AS uk"
			"    WHERE %s%s"
			"  ), true)"
			")",
			quote_identifier(fk_schema_name), quote_identifier(fk_table_name),
			where_buf.data,
			uk_range_constructor, quote_identifier(uk_valid_from_column_name), quote_identifier(uk_valid_until_column_name),
			fk_range_constructor, quote_identifier(fk_valid_from_column_name), quote_identifier(fk_valid_until_column_name),
			quote_identifier(uk_valid_from_column_name),
			quote_identifier(uk_schema_name), quote_identifier(uk_table_name),
			join_buf.data, exclude_buf.data
		);
		plan_entry->plan = SPI_prepare(query, plan_entry->nargs, plan_entry->argtypes);
		if (plan_entry->plan == NULL)
			elog(ERROR, "SPI_prepare for validation query failed: %s", SPI_result_code_string(SPI_result));
		if (SPI_keepplan(plan_entry->plan))
			elog(ERROR, "SPI_keepplan for validation query failed");
		pfree(query);
		pfree(join_buf.data); pfree(where_buf.data); pfree(exclude_buf.data);
		if(fk_range_constructor) pfree(fk_range_constructor); if(uk_range_constructor) pfree(uk_range_constructor);
	}

	/* Check for NULLs in UK columns of old_row using cached attnums */
	{
		int i;
		for (i = 0; i < plan_entry->nargs - 2; i++)
		{
			(void) heap_getattr(old_row, plan_entry->param_attnums[i], tupdesc, &isnull);
			if (isnull)
			{
				SPI_finish();
				return PointerGetDatum(rettuple);
			}
		}
	}

	/* Execute validation query */
	{
		Datum values[MAX_FK_COLS + 2];
		char nulls[MAX_FK_COLS + 2];
		int i;

		for (i = 0; i < plan_entry->nargs; i++)
		{
			values[i] = heap_getattr(old_row, plan_entry->param_attnums[i], tupdesc, &isnull);
			nulls[i] = isnull ? 'n' : ' ';
		}
		
		ret = SPI_execute_plan(plan_entry->plan, values, nulls, true, 1);
		if (ret != SPI_OK_SELECT)
			elog(ERROR, "SPI_execute_plan failed");
		
		if (SPI_processed > 0)
		{
			violation = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
			if (isnull) violation = true;
		}
		else
			violation = true;

		if (violation)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
					 errmsg("update or delete on table \"%s.%s\" violates foreign key constraint \"%s\" on table \"%s.%s\"",
							uk_schema_name, uk_table_name, foreign_key_name, fk_schema_name, fk_table_name)));
		}
	}

	SPI_finish();
	return PointerGetDatum(rettuple);
}

Datum
uk_update_check_c(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata;
	HeapTuple	rettuple;
	Relation	rel;
	TupleDesc	tupdesc;
	HeapTuple	old_row;
	HeapTuple	new_row;
	char	  **tgargs;
	char *foreign_key_name;
	char *fk_schema_name;
	char *fk_table_name;
	char *fk_column_names_str;
	char *fk_era_name;
	char *fk_valid_from_column_name;
	char *fk_valid_until_column_name;
	char *uk_schema_name;
	char *uk_table_name;
	char *uk_column_names_str;
	char *uk_era_name;
	char *uk_valid_from_column_name;
	char *uk_valid_until_column_name;

	UkUpdateValidationPlan *plan_entry;
	bool found;
	int ret;
	bool isnull, violation;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "uk_update_check_c: not called by trigger manager");

	trigdata = (TriggerData *) fcinfo->context;
	rettuple = trigdata->tg_newtuple;
	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;
	old_row = trigdata->tg_trigtuple;
	new_row = trigdata->tg_newtuple;

	if (trigdata->tg_trigger->tgnargs != 16)
		elog(ERROR, "uk_update_check_c: expected 16 arguments, got %d", trigdata->tg_trigger->tgnargs);

	tgargs = trigdata->tg_trigger->tgargs;

	foreign_key_name = tgargs[0];
	fk_schema_name = tgargs[1];
	fk_table_name = tgargs[2];
	fk_column_names_str = tgargs[3];
	fk_era_name = tgargs[4];
	fk_valid_from_column_name = tgargs[5];
	fk_valid_until_column_name = tgargs[6];
	uk_schema_name = tgargs[7];
	uk_table_name = tgargs[8];
	uk_column_names_str = tgargs[9];
	uk_era_name = tgargs[10];
	uk_valid_from_column_name = tgargs[11];
	uk_valid_until_column_name = tgargs[12];

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	init_uk_update_plan_cache();
	plan_entry = (UkUpdateValidationPlan *) hash_search(uk_update_plan_cache, foreign_key_name, HASH_ENTER, &found);

	if (!found)
	{
		char *fk_range_constructor;
		char *uk_range_constructor;
		char *query;
		Datum get_range_type_values[3];
		StringInfoData where_buf, exclude_buf, union_buf, select_list_buf, alias_buf, join_buf;
		int i;
		Datum uk_column_names_datum, fk_column_names_datum;
		ArrayType *uk_column_names_array, *fk_column_names_array;
		int num_uk_cols, num_fk_cols;
		Datum *uk_col_datums, *fk_col_datums;
		char *inner_alias = "sub_uk";
		int param_idx = 0;

		/* Get range constructors */
		if (get_range_type_plan == NULL)
		{
			const char *sql = "SELECT range_type::regtype::text FROM sql_saga.era WHERE table_schema = $1 AND table_name = $2 AND era_name = $3";
			Oid plan_argtypes[] = { NAMEOID, NAMEOID, NAMEOID };
			get_range_type_plan = SPI_prepare(sql, 3, plan_argtypes);
			if (get_range_type_plan == NULL)
				elog(ERROR, "SPI_prepare for get_range_type failed: %s", SPI_result_code_string(SPI_result));
			ret = SPI_keepplan(get_range_type_plan);
			if (ret != 0)
				elog(ERROR, "SPI_keepplan for get_range_type failed: %s", SPI_result_code_string(ret));
		}
		get_range_type_values[0] = CStringGetDatum(fk_schema_name);
		get_range_type_values[1] = CStringGetDatum(fk_table_name);
		get_range_type_values[2] = CStringGetDatum(fk_era_name);
		ret = SPI_execute_plan(get_range_type_plan, get_range_type_values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0) elog(ERROR, "could not get range type for fk table");
		fk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		get_range_type_values[0] = CStringGetDatum(uk_schema_name);
		get_range_type_values[1] = CStringGetDatum(uk_table_name);
		get_range_type_values[2] = CStringGetDatum(uk_era_name);
		ret = SPI_execute_plan(get_range_type_plan, get_range_type_values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0) elog(ERROR, "could not get range type for uk table");
		uk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		/* Build clauses and collect param info */
		initStringInfo(&where_buf); initStringInfo(&exclude_buf); initStringInfo(&union_buf);
		initStringInfo(&select_list_buf); initStringInfo(&alias_buf); initStringInfo(&join_buf);

		if (namearray_input_func_oid == InvalidOid)
		{
			/*
			 * We only need the input function and ioparam for NAMEARRAYOID,
			 * so we cache them in static variables to avoid repeated catalog
			 * lookups. Other type data is not needed.
			 */
			int16	typlen;
			bool	typbyval;
			char	typalign;
			char	typdelim;
			get_type_io_data(NAMEARRAYOID, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &namearray_ioparam_oid, &namearray_input_func_oid);
		}

		uk_column_names_datum = OidInputFunctionCall(namearray_input_func_oid, uk_column_names_str, namearray_ioparam_oid, -1);
		uk_column_names_array = DatumGetArrayTypeP(uk_column_names_datum);
		deconstruct_array(uk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &uk_col_datums, NULL, &num_uk_cols);
		fk_column_names_datum = OidInputFunctionCall(namearray_input_func_oid, fk_column_names_str, namearray_ioparam_oid, -1);
		fk_column_names_array = DatumGetArrayTypeP(fk_column_names_datum);
		deconstruct_array(fk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &fk_col_datums, NULL, &num_fk_cols);

		if (num_uk_cols > MAX_FK_COLS) elog(ERROR, "Number of uk columns (%d) exceeds MAX_FK_COLS (%d)", num_uk_cols, MAX_FK_COLS);
		plan_entry->num_uk_cols = num_uk_cols;
		plan_entry->nargs = 2 * num_uk_cols + 4;

		/* Params for old_row */
		appendStringInfoString(&exclude_buf, " AND NOT (");
		for (i = 0; i < num_uk_cols; i++) {
			char *ukc = NameStr(*DatumGetName(uk_col_datums[i])); char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
			int attnum = SPI_fnumber(tupdesc, ukc);
			if (attnum <= 0) elog(ERROR, "column \"%s\" does not exist", ukc);
			if (i > 0) { appendStringInfoString(&where_buf, " AND "); appendStringInfoString(&exclude_buf, " AND "); }
			appendStringInfo(&where_buf, "fk.%s = $%d", quote_identifier(fkc), param_idx + 1);
			appendStringInfo(&exclude_buf, "uk.%s = $%d", quote_identifier(ukc), param_idx + 1);
			plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, attnum);
			plan_entry->param_attnums_old[i] = attnum;
			param_idx++;
		}
		if (num_uk_cols > 0) appendStringInfoString(&exclude_buf, " AND ");
		plan_entry->param_attnums_old[num_uk_cols] = SPI_fnumber(tupdesc, uk_valid_from_column_name);
		plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums_old[num_uk_cols]);
		appendStringInfo(&exclude_buf, "uk.%s = $%d", quote_identifier(uk_valid_from_column_name), param_idx + 1);
		param_idx++;
		appendStringInfoString(&exclude_buf, " AND ");
		plan_entry->param_attnums_old[num_uk_cols+1] = SPI_fnumber(tupdesc, uk_valid_until_column_name);
		plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums_old[num_uk_cols+1]);
		appendStringInfo(&exclude_buf, "uk.%s = $%d", quote_identifier(uk_valid_until_column_name), param_idx + 1);
		param_idx++;
		appendStringInfoChar(&exclude_buf, ')');
		
		/* Params for new_row */
		appendStringInfoString(&union_buf, " UNION ALL SELECT ");
		for (i = 0; i < num_uk_cols; i++) {
			char *ukc = NameStr(*DatumGetName(uk_col_datums[i]));
			int attnum = SPI_fnumber(tupdesc, ukc);
			if (i > 0) appendStringInfoString(&union_buf, ", ");
			appendStringInfo(&union_buf, "$%d", param_idx + 1);
			plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, attnum);
			plan_entry->param_attnums_new[i] = attnum;
			param_idx++;
		}
		appendStringInfoString(&union_buf, ", ");
		plan_entry->param_attnums_new[num_uk_cols] = SPI_fnumber(tupdesc, uk_valid_from_column_name);
		plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums_new[num_uk_cols]);
		appendStringInfo(&union_buf, "$%d", param_idx + 1);
		param_idx++;
		appendStringInfoString(&union_buf, ", ");
		plan_entry->param_attnums_new[num_uk_cols+1] = SPI_fnumber(tupdesc, uk_valid_until_column_name);
		plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums_new[num_uk_cols+1]);
		appendStringInfo(&union_buf, "$%d", param_idx + 1);

		/* Clauses without params */
		appendStringInfo(&alias_buf, " AS %s(", inner_alias);
		for (i = 0; i < num_uk_cols; i++) {
			char *ukc = NameStr(*DatumGetName(uk_col_datums[i])); char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
			if (i > 0) { appendStringInfoString(&select_list_buf, ", "); appendStringInfoString(&alias_buf, ", "); appendStringInfoString(&join_buf, " AND "); }
			appendStringInfo(&select_list_buf, "%s", quote_identifier(ukc));
			appendStringInfo(&alias_buf, "%s", quote_identifier(ukc));
			appendStringInfo(&join_buf, "fk.%s = %s.%s", quote_identifier(fkc), inner_alias, quote_identifier(ukc));
		}
		appendStringInfo(&select_list_buf, ", %s, %s", quote_identifier(uk_valid_from_column_name), quote_identifier(uk_valid_until_column_name));
		appendStringInfo(&alias_buf, ", %s, %s)", quote_identifier(uk_valid_from_column_name), quote_identifier(uk_valid_until_column_name));

		query = psprintf("SELECT EXISTS (SELECT 1 FROM %s.%s AS fk WHERE %s AND COALESCE(NOT ("
			"SELECT sql_saga.covers_without_gaps("
			"%s(%s.%s, %s.%s), "
			"%s(fk.%s, fk.%s) "
			"ORDER BY %s.%s"
			") FROM (SELECT %s FROM %s.%s AS uk WHERE TRUE %s %s) %s WHERE %s), true))",
			quote_identifier(fk_schema_name), quote_identifier(fk_table_name), where_buf.data,
			uk_range_constructor, inner_alias, quote_identifier(uk_valid_from_column_name), inner_alias, quote_identifier(uk_valid_until_column_name),
			fk_range_constructor, quote_identifier(fk_valid_from_column_name), quote_identifier(fk_valid_until_column_name),
			inner_alias, quote_identifier(uk_valid_from_column_name),
			select_list_buf.data, quote_identifier(uk_schema_name), quote_identifier(uk_table_name),
			exclude_buf.data, union_buf.data, alias_buf.data,
			join_buf.data
		);

		plan_entry->plan = SPI_prepare(query, plan_entry->nargs, plan_entry->argtypes);
		if (!plan_entry->plan || SPI_keepplan(plan_entry->plan))
			elog(ERROR, "SPI_prepare/keepplan for validation query failed");
		
		pfree(query); pfree(where_buf.data); pfree(exclude_buf.data); pfree(union_buf.data);
		pfree(select_list_buf.data); pfree(alias_buf.data); pfree(join_buf.data);
		if(fk_range_constructor) pfree(fk_range_constructor); if(uk_range_constructor) pfree(uk_range_constructor);
		pfree(DatumGetPointer(uk_column_names_datum)); if (num_uk_cols > 0) pfree(uk_col_datums);
		pfree(DatumGetPointer(fk_column_names_datum)); if (num_fk_cols > 0) pfree(fk_col_datums);
	}

	/* Check for NULLs in UK columns of old_row using cached attnums */
	{
		int i;
		for (i = 0; i < plan_entry->num_uk_cols; i++)
		{
			(void) heap_getattr(old_row, plan_entry->param_attnums_old[i], tupdesc, &isnull);
			if (isnull)
			{
				SPI_finish();
				return PointerGetDatum(rettuple);
			}
		}
	}

	/* Execute validation query */
	{
		Datum values[MAX_UK_UPDATE_PLAN_ARGS];
		char nulls[MAX_UK_UPDATE_PLAN_ARGS];
		int i, param_idx = 0;

		for (i = 0; i < plan_entry->num_uk_cols + 2; i++) {
			values[param_idx] = heap_getattr(old_row, plan_entry->param_attnums_old[i], tupdesc, &isnull);
			nulls[param_idx] = isnull ? 'n' : ' ';
			param_idx++;
		}
		for (i = 0; i < plan_entry->num_uk_cols + 2; i++) {
			values[param_idx] = heap_getattr(new_row, plan_entry->param_attnums_new[i], tupdesc, &isnull);
			nulls[param_idx] = isnull ? 'n' : ' ';
			param_idx++;
		}
		
		ret = SPI_execute_plan(plan_entry->plan, values, nulls, true, 1);
		if (ret != SPI_OK_SELECT) elog(ERROR, "SPI_execute_plan failed");
		
		if (SPI_processed > 0) {
			violation = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
			if (isnull) violation = true;
		} else {
			violation = true;
		}

		if (violation)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
					 errmsg("update or delete on table \"%s.%s\" violates foreign key constraint \"%s\" on table \"%s.%s\"",
							uk_schema_name, uk_table_name, foreign_key_name, fk_schema_name, fk_table_name)));
		}
	}

	SPI_finish();
	return PointerGetDatum(rettuple);
}

void _PG_init(void) {
}

void _PG_fini(void) {
}

