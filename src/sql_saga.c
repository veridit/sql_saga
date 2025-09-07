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

#if (PG_VERSION_NUM < 120000)
#define table_open(r, l)	heap_open(r, l)
#define table_close(r, l)	heap_close(r, l)
#else
#include "access/table.h"
#endif
#include "access/tupconvert.h"
#include "access/xact.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "utils/date.h"
#if (PG_VERSION_NUM >= 100000)
#include "utils/fmgrprotos.h"
#endif
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "sql_saga.h"

#define NAMEARRAYOID 1003

PG_MODULE_MAGIC;

/* Forward declarations for static functions */
static void cache_cleanup_callback(XactEvent event, void *arg);

/* Define some SQLSTATEs that might not exist */
#if (PG_VERSION_NUM < 100000)
#define ERRCODE_GENERATED_ALWAYS MAKE_SQLSTATE('4','2','8','C','9')
#endif
#define ERRCODE_INVALID_ROW_VERSION MAKE_SQLSTATE('2','2','0','1','H')

/* We use these a lot, so make aliases for them */
#if (PG_VERSION_NUM < 100000)
#define TRANSACTION_TSTZ	TimestampTzGetDatum(GetCurrentTransactionStartTimestamp())
#define TRANSACTION_TS		DirectFunctionCall1(timestamptz_timestamp, TRANSACTION_TSTZ)
#define TRANSACTION_DATE	DirectFunctionCall1(timestamptz_date, TRANSACTION_TSTZ)
#else
#define TRANSACTION_TSTZ	TimestampTzGetDatum(GetCurrentTransactionStartTimestamp())
#define TRANSACTION_TS		DirectFunctionCall1(timestamptz_timestamp, TRANSACTION_TSTZ)
#define TRANSACTION_DATE	DateADTGetDatum(GetSQLCurrentDate())
#endif

#define INFINITE_TSTZ		TimestampTzGetDatum(DT_NOEND)
#define INFINITE_TS			TimestampGetDatum(DT_NOEND)
#define INFINITE_DATE		DateADTGetDatum(DATEVAL_NOEND)

/* Plan caches for inserting into history tables */
static HTAB *InsertHistoryPlanHash = NULL;

typedef struct InsertHistoryPlanEntry
{
	Oid			history_relid;	/* the hash key; must be first */
	char		schemaname[NAMEDATALEN];
	char		tablename[NAMEDATALEN];
	SPIPlanPtr	qplan;
} InsertHistoryPlanEntry;

static HTAB *
CreateInsertHistoryPlanHash(void)
{
	HASHCTL	ctl;

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(InsertHistoryPlanEntry);

	return hash_create("Insert History Hash", 16, &ctl, HASH_ELEM | HASH_BLOBS);
}


#define MAX_FK_COLS 16

typedef struct FkValidationPlan
{
	Oid			trigger_oid;	/* the hash key; must be first */
	SPIPlanPtr	plan;
	int			nargs;
	Oid			argtypes[MAX_FK_COLS + 2]; /* FK cols + range start/end */
	int			param_attnums[MAX_FK_COLS + 2]; /* attnums in heap tuple */
} FkValidationPlan;

static HTAB *fk_plan_cache = NULL;
static HTAB *uk_delete_plan_cache = NULL;
static HTAB *uk_update_plan_cache = NULL;
static bool cache_callback_registered = false;

static void
cache_cleanup_callback(XactEvent event, void *arg)
{
	/*
	 * On transaction end, reset the static pointers to our caches. The memory
	 * holding the hash tables will be freed automatically because it was
	 * allocated in CurTransactionContext. If we don't reset these pointers,
	 * the next transaction will try to use a dangling pointer, leading to a
	 * crash or other undefined behavior.
	 */
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_COMMIT)
	{
		fk_plan_cache = NULL;
		uk_delete_plan_cache = NULL;
		uk_update_plan_cache = NULL;
		cache_callback_registered = false;
	}
}


static void
init_fk_plan_cache(void)
{
	HASHCTL ctl;

	if (fk_plan_cache)
		return;

	if (!cache_callback_registered)
	{
		RegisterXactCallback(cache_cleanup_callback, NULL);
		cache_callback_registered = true;
	}

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(FkValidationPlan);
	/* Lifetime of cache is transaction */
	ctl.hcxt = CurTransactionContext;
	fk_plan_cache = hash_create("sql_saga fk validation plan cache", 16, &ctl, HASH_ELEM | HASH_BLOBS);
}

static void
init_uk_delete_plan_cache(void)
{
	HASHCTL ctl;

	if (uk_delete_plan_cache)
		return;

	if (!cache_callback_registered)
	{
		RegisterXactCallback(cache_cleanup_callback, NULL);
		cache_callback_registered = true;
	}

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(FkValidationPlan); /* Reusing struct */
	ctl.hcxt = CurTransactionContext;
	uk_delete_plan_cache = hash_create("sql_saga uk delete validation plan cache", 16, &ctl, HASH_ELEM | HASH_BLOBS);
}

#define MAX_UK_UPDATE_PLAN_ARGS (2 * MAX_FK_COLS + 4)

typedef struct UkUpdateValidationPlan
{
	Oid			trigger_oid;	/* the hash key; must be first */
	SPIPlanPtr	plan;
	int			nargs;
	Oid			argtypes[MAX_UK_UPDATE_PLAN_ARGS];
	int			num_uk_cols;
	int			param_attnums_old[MAX_FK_COLS + 2];
	int			param_attnums_new[MAX_FK_COLS + 2];
} UkUpdateValidationPlan;

static void
init_uk_update_plan_cache(void)
{
	HASHCTL ctl;

	if (uk_update_plan_cache)
		return;

	if (!cache_callback_registered)
	{
		RegisterXactCallback(cache_cleanup_callback, NULL);
		cache_callback_registered = true;
	}

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(UkUpdateValidationPlan);
	ctl.hcxt = CurTransactionContext;
	uk_update_plan_cache = hash_create("sql_saga uk update validation plan cache", 16, &ctl, HASH_ELEM | HASH_BLOBS);
}

static SPIPlanPtr get_range_type_plan = NULL;

/* For NAMEARRAYOID type IO */
static Oid namearray_input_func_oid = InvalidOid;
static Oid namearray_ioparam_oid = InvalidOid;

static void
GetPeriodColumnNames(Relation rel, char *period_name, char **start_name, char **end_name)
{
	int				ret;
	Datum			values[3];
	SPITupleTable  *tuptable;
	bool			is_null;
	Datum			dat;
	MemoryContext	mcxt = CurrentMemoryContext; /* The context outside of SPI */
    char           *schema_name;
    char           *table_name;

	const char *sql =
		"SELECT e.valid_from_column_name, e.valid_until_column_name "
		"FROM sql_saga.era AS e "
		"WHERE (e.table_schema, e.table_name, e.era_name) = ($1, $2, $3)";
	static SPIPlanPtr qplan = NULL;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Query the sql_saga.era table to get the start and end columns.
	 * Cache the plan if we haven't already.
	 */
	if (qplan == NULL)
	{
		Oid	types[3] = {NAMEOID, NAMEOID, NAMEOID};

		qplan = SPI_prepare(sql, 3, types);
		if (qplan == NULL)
			elog(ERROR, "SPI_prepare returned %s for %s",
				 SPI_result_code_string(SPI_result), sql);

		ret = SPI_keepplan(qplan);
		if (ret != 0)
			elog(ERROR, "SPI_keepplan returned %s", SPI_result_code_string(ret));
	}

    schema_name = get_namespace_name(RelationGetNamespace(rel));
    table_name = RelationGetRelationName(rel);

	values[0] = CStringGetDatum(schema_name);
	values[1] = CStringGetDatum(table_name);
	values[2] = CStringGetDatum(period_name);
	ret = SPI_execute_plan(qplan, values, NULL, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute returned %s", SPI_result_code_string(ret));

    pfree(schema_name);

	/* Make sure we got one */
	if (SPI_processed == 0)
		ereport(ERROR,
				(errmsg("era \"%s\" not found on table \"%s\"",
						period_name,
						table_name)));

	/* There is a unique constraint so there shouldn't be more than 1 row */
	Assert(SPI_processed == 1);

	/*
	 * Get the names from the result tuple.  We copy them into the original
	 * context so they don't get wiped out by SPI_finish().
	 */
	tuptable = SPI_tuptable;

	dat = SPI_getbinval(tuptable->vals[0], tuptable->tupdesc, 1, &is_null);
	*start_name = MemoryContextStrdup(mcxt, NameStr(*(DatumGetName(dat))));

	dat = SPI_getbinval(tuptable->vals[0], tuptable->tupdesc, 2, &is_null);
	*end_name = MemoryContextStrdup(mcxt, NameStr(*(DatumGetName(dat))));

	/* All done with SPI */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}

/*
 * Check if the only columns changed in an UPDATE are columns that the user is
 * excluding from SYSTEM VERSIONING. One possible use case for this is a
 * "last_login timestamptz" column on a user table.  Arguably, this column
 * should be in another table, but users have requested the feature so let's do
 * it.
 */
static bool
OnlyExcludedColumnsChanged(Relation rel, HeapTuple old_row, HeapTuple new_row)
{
	int				ret;
	int				i;
	Datum			values[2];
	TupleDesc		tupdesc = RelationGetDescr(rel);
	Bitmapset	   *excluded_attnums = NULL;
	MemoryContext	mcxt = CurrentMemoryContext; /* The context outside of SPI */
    char           *schema_name;
    char           *table_name;

	const char *sql =
		"SELECT u.name "
		"FROM sql_saga.system_time_era AS ste "
		"CROSS JOIN unnest(ste.excluded_column_names) AS u (name) "
		"WHERE ste.table_schema = $1 AND ste.table_name = $2";
	static SPIPlanPtr qplan = NULL;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Get the excluded column names.
	 * Cache the plan if we haven't already.
	 */
	if (qplan == NULL)
	{
		Oid	types[2] = {NAMEOID, NAMEOID};

		qplan = SPI_prepare(sql, 2, types);
		if (qplan == NULL)
			elog(ERROR, "SPI_prepare returned %s for %s",
				 SPI_result_code_string(SPI_result), sql);

		ret = SPI_keepplan(qplan);
		if (ret != 0)
			elog(ERROR, "SPI_keepplan returned %s", SPI_result_code_string(ret));
	}

    schema_name = get_namespace_name(RelationGetNamespace(rel));
    table_name = RelationGetRelationName(rel);
	values[0] = CStringGetDatum(schema_name);
	values[1] = CStringGetDatum(table_name);
	ret = SPI_execute_plan(qplan, values, NULL, true, 0);
    pfree(schema_name);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute returned %s", SPI_result_code_string(ret));

	/* Construct a bitmap of excluded attnums */
	if (SPI_processed > 0 && SPI_tuptable != NULL)
	{
		TupleDesc	spitupdesc = SPI_tuptable->tupdesc;
		bool		isnull;
		int			i;

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	tuple = SPI_tuptable->vals[i];
			Datum		attdatum;
			char	   *attname;
			int16		attnum;

			/* Get the attnum from the column name */
			attdatum = SPI_getbinval(tuple, spitupdesc, 1, &isnull);
			attname = NameStr(*(DatumGetName(attdatum)));
			attnum = SPI_fnumber(tupdesc, attname);

			/* Make sure it's valid (should always be) */
			if (attnum == SPI_ERROR_NOATTRIBUTE)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" does not exist", attname)));

			/* Just ignore system columns (should never happen) */
			if (attnum < 0)
				continue;

			/* Add it to the bitmap set */
			excluded_attnums = bms_add_member(excluded_attnums, attnum);
		}

		/*
		 * If we have excluded columns, move the bitmapset out of the SPI
		 * context.
		 */
		if (excluded_attnums != NULL)
		{
			MemoryContext spicontext = MemoryContextSwitchTo(mcxt);
			excluded_attnums = bms_copy(excluded_attnums);
			MemoryContextSwitchTo(spicontext);
		}
	}

	/* Don't need SPI anymore */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	/* If there are no excluded columns, then we're done */
	if (excluded_attnums == NULL)
		return false;

	for (i = 1; i <= tupdesc->natts; i++)
	{
		Datum	old_datum, new_datum;
		bool	old_isnull, new_isnull;
		int16	typlen;
		bool	typbyval;

		/* Ignore if deleted column */
		if (TupleDescAttr(tupdesc, i-1)->attisdropped)
			continue;

		/* Ignore if excluded column */
		if (bms_is_member(i, excluded_attnums))
			continue;

		old_datum = SPI_getbinval(old_row, tupdesc, i, &old_isnull);
		new_datum = SPI_getbinval(new_row, tupdesc, i, &new_isnull);

		/*
		 * If one value is NULL and other is not, then they are certainly not
		 * equal.
		 */
		if (old_isnull != new_isnull)
			return false;

		/* If both are NULL, they can be considered equal. */
		if (old_isnull)
			continue;

		/* Do a fairly strict binary comparison of the values */
		typlen = TupleDescAttr(tupdesc, i-1)->attlen;
		typbyval = TupleDescAttr(tupdesc, i-1)->attbyval;
		if (!datumIsEqual(old_datum, new_datum, typbyval, typlen))
			return false;
	}

	return true;
}

static Datum
GetRowStart(Oid typeid)
{
	switch (typeid)
	{
		case TIMESTAMPTZOID:
			return TRANSACTION_TSTZ;
		case TIMESTAMPOID:
			return TRANSACTION_TS;
		case DATEOID:
			return TRANSACTION_DATE;
		default:
			elog(ERROR, "unexpected type: %d", typeid);
			return 0;	/* keep compiler quiet */
	}
}

static Datum
GetRowEnd(Oid typeid)
{
	switch (typeid)
	{
		case TIMESTAMPTZOID:
			return INFINITE_TSTZ;
		case TIMESTAMPOID:
			return INFINITE_TS;
		case DATEOID:
			return INFINITE_DATE;
		default:
			elog(ERROR, "unexpected type: %d", typeid);
			return 0;	/* keep compiler quiet */
	}
}

static Oid
GetHistoryTable(Relation rel)
{
	int		ret;
	Datum	values[2];
	Oid		result;
	SPITupleTable  *tuptable;
	bool			is_null;
    char           *schema_name;
    char           *table_name;

	const char *sql =
		"SELECT hc.oid "
		"FROM sql_saga.system_versioning sv "
		"JOIN pg_catalog.pg_namespace hn ON sv.history_schema_name = hn.nspname "
		"JOIN pg_catalog.pg_class hc ON (hc.relnamespace, hc.relname) = (hn.oid, sv.history_table_name) "
		"WHERE sv.table_schema = $1 AND sv.table_name = $2";
	static SPIPlanPtr qplan = NULL;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	if (qplan == NULL)
	{
		Oid	types[2] = {NAMEOID, NAMEOID};

		qplan = SPI_prepare(sql, 2, types);
		if (qplan == NULL)
			elog(ERROR, "SPI_prepare returned %s for %s",
				 SPI_result_code_string(SPI_result), sql);

		ret = SPI_keepplan(qplan);
		if (ret != 0)
			elog(ERROR, "SPI_keepplan returned %s", SPI_result_code_string(ret));
	}

    schema_name = get_namespace_name(RelationGetNamespace(rel));
    table_name = RelationGetRelationName(rel);
	values[0] = CStringGetDatum(schema_name);
	values[1] = CStringGetDatum(table_name);
	ret = SPI_execute_plan(qplan, values, NULL, true, 0);
    pfree(schema_name);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute returned %s", SPI_result_code_string(ret));

	if (SPI_processed == 0)
	{
		if (SPI_finish() != SPI_OK_FINISH)
			elog(ERROR, "SPI_finish failed");
		return InvalidOid;
	}

	Assert(SPI_processed == 1);

	tuptable = SPI_tuptable;
	result = DatumGetObjectId(SPI_getbinval(tuptable->vals[0], tuptable->tupdesc, 1, &is_null));

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	return result;
}

static int
CompareWithCurrentDatum(Oid typeid, Datum value)
{
	switch (typeid)
	{
		case TIMESTAMPTZOID:
			return DatumGetInt32(DirectFunctionCall2(timestamp_cmp, value, TRANSACTION_TSTZ));

		case TIMESTAMPOID:
			return DatumGetInt32(DirectFunctionCall2(timestamp_cmp, value, TRANSACTION_TS));

		case DATEOID:
			return DatumGetInt32(DirectFunctionCall2(date_cmp, value, TRANSACTION_DATE));

		default:
			elog(ERROR, "unexpected type: %d", typeid);
			return 0;	/* keep compiler quiet */
	}
}

static int
CompareWithInfiniteDatum(Oid typeid, Datum value)
{
	switch (typeid)
	{
		case TIMESTAMPTZOID:
			return DatumGetInt32(DirectFunctionCall2(timestamp_cmp, value, INFINITE_TSTZ));

		case TIMESTAMPOID:
			return DatumGetInt32(DirectFunctionCall2(timestamp_cmp, value, INFINITE_TS));

		case DATEOID:
			return DatumGetInt32(DirectFunctionCall2(date_cmp, value, INFINITE_DATE));

		default:
			elog(ERROR, "unexpected type: %d", typeid);
			return 0;	/* keep compiler quiet */
	}
}

static void
insert_into_history(Relation history_rel, HeapTuple history_tuple)
{
	InsertHistoryPlanEntry   *hentry;
	bool		found;
	char	   *schemaname = get_namespace_name(RelationGetNamespace(history_rel));
	char	   *tablename = RelationGetRelationName(history_rel);
	Oid			history_relid = history_rel->rd_id;
	Datum		value;
	int			ret;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	if (!InsertHistoryPlanHash)
		InsertHistoryPlanHash = CreateInsertHistoryPlanHash();

	hentry = (InsertHistoryPlanEntry *) hash_search(
			InsertHistoryPlanHash,
			&history_relid,
			HASH_ENTER,
			&found);

	if (!found ||
		strcmp(hentry->schemaname, schemaname) != 0 ||
		strcmp(hentry->tablename, tablename) != 0)
	{
		StringInfo	buf = makeStringInfo();
		Oid			type = HeapTupleHeaderGetTypeId(history_tuple->t_data);

		appendStringInfo(buf, "INSERT INTO %s VALUES (($1).*)",
				quote_qualified_identifier(schemaname, tablename));

		hentry->history_relid = history_relid;
		strlcpy(hentry->schemaname, schemaname, sizeof(hentry->schemaname));
		strlcpy(hentry->tablename, tablename, sizeof(hentry->tablename));
		hentry->qplan = SPI_prepare(buf->data, 1, &type);
		if (hentry->qplan == NULL)
			elog(ERROR, "SPI_prepare returned %s for %s",
				 SPI_result_code_string(SPI_result), buf->data);

		ret = SPI_keepplan(hentry->qplan);
		if (ret != 0)
			elog(ERROR, "SPI_keepplan returned %s", SPI_result_code_string(ret));
        pfree(buf->data);
	}

	value = HeapTupleGetDatum(history_tuple);
	ret = SPI_execute_plan(hentry->qplan, &value, NULL, false, 0);
	if (ret != SPI_OK_INSERT)
		elog(ERROR, "SPI_execute returned %s", SPI_result_code_string(ret));

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

    pfree(schemaname);
}


/* Function definitions */

PG_FUNCTION_INFO_V1(fk_insert_check_c);
PG_FUNCTION_INFO_V1(fk_update_check_c);
PG_FUNCTION_INFO_V1(uk_delete_check_c);
PG_FUNCTION_INFO_V1(uk_update_check_c);
PG_FUNCTION_INFO_V1(generated_always_as_row_start_end);
PG_FUNCTION_INFO_V1(write_history);

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
	plan_entry = (FkValidationPlan *) hash_search(fk_plan_cache, &(trigdata->tg_trigger->tgoid), HASH_ENTER, &found);
	
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
	plan_entry = (FkValidationPlan *) hash_search(fk_plan_cache, &(trigdata->tg_trigger->tgoid), HASH_ENTER, &found);
	
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
	char *fk_type;

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

	if (trigdata->tg_trigger->tgnargs != 17)
		elog(ERROR, "uk_delete_check_c: expected 17 arguments, got %d", trigdata->tg_trigger->tgnargs);

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
	/* args 13, 14, 15 are match_type, update_action, delete_action */
	fk_type = tgargs[16];

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	init_uk_delete_plan_cache();
	plan_entry = (FkValidationPlan *) hash_search(uk_delete_plan_cache, &(trigdata->tg_trigger->tgoid), HASH_ENTER, &found);

	if (!found)
	{
		Datum uk_column_names_datum, fk_column_names_datum;
		ArrayType *uk_column_names_array, *fk_column_names_array;
		int num_uk_cols, num_fk_cols;
		Datum *uk_col_datums, *fk_col_datums;
		StringInfoData where_buf;
		char *query;
		int i;

		/* Get column name arrays */
		if (namearray_input_func_oid == InvalidOid)
		{
			int16 typlen; bool typbyval; char typalign, typdelim;
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

		initStringInfo(&where_buf);

		/*
		 * PRINCIPLE OF OPERATION FOR TEMPORAL FK ON DELETE
		 *
		 * This is an AFTER ROW DELETE trigger. As established by the
		 * `58_trigger_visibility.sql` test, the MVCC snapshot visible to this
		 * trigger's queries does NOT include the row that was just deleted.
		 * While the `OLD` row data is available to the trigger function
		 * itself, it is not visible to any SQL queries it executes.
		 *
		 * The trigger's purpose is to ensure this deletion does not "orphan"
		 * any rows in a referencing table. An orphan is a row in the FK
		 * table whose validity period is no longer fully covered by the
		 * timeline of the entity it references in the UK table.
		 *
		 * --- EXAMPLE ---
		 * UK Table: employees(id, name, valid_from, valid_until)
		 *   (1, 'Alice', '2022-01-01', '2023-01-01')
		 *   (1, 'Alice', '2023-01-01', 'infinity')
		 *
		 * FK Table: projects(pid, employee_id, valid_from, valid_until)
		 *   (101, 1, '2022-06-01', '2023-06-01')
		 *
		 * Scenario: A user executes `DELETE FROM employees WHERE id = 1 AND valid_from = '2023-01-01';`
		 * This would leave project 101 uncovered from 2023-01-01 to 2023-06-01.
		 *
		 * --- QUERY VALIDATION LOGIC ---
		 * 1. (Outer Query) Find Potentially Orphaned Rows:
		 *    `SELECT EXISTS (SELECT 1 FROM projects AS fk WHERE fk.employee_id = $1 ...)`
		 *    This finds all FK rows that reference the entity being changed
		 *    (e.g., project 101).
		 *
		 * 2. (Subquery) Check Timeline Coverage:
		 *    `... COALESCE(NOT (SELECT sql_saga.covers_without_gaps(...) ...), true)`
		 *    For each FK row found, this subquery checks if its timeline is
		 *    still covered by the UK entity's timeline *after* the deletion.
		 *
		 *    2.1. Construct Post-Delete State:
		 *         `... FROM employees AS uk WHERE fk.employee_id = uk.id ...`
		 *         The `covers_without_gaps` aggregate is fed all rows for the
		 *         UK entity from the current MVCC snapshot. Since the row has
		 *         already been deleted from the snapshot, this query correctly
		 *         retrieves the "post-delete" state of the timeline.
		 *         - Example: The aggregate receives only `(1, 'Alice', '2022-01-01', '2023-01-01')`.
		 *
		 *    2.2. Perform Coverage Check:
		 *         `covers_without_gaps('[2022-01-01, 2023-01-01)', '[2022-06-01, 2023-06-01)')`
		 *         The project's period `[2022-06-01, 2023-06-01)` is NOT fully
		 *         covered by the remaining UK period `[2022-01-01, 2023-01-01)`.
		 *         The function returns `false`.
		 *
		 * 3. (Boolean Logic) Determine Violation:
		 *    - `NOT(false)` becomes `true`.
		 *    - `COALESCE(true, true)` returns `true`, indicating a violation.
		 *
		 * --- ROLE OF COALESCE(..., true) ---
		 * If `covers_without_gaps` receives zero rows from its subquery (e.g.,
		 * the last timeline segment for a UK entity was deleted), it returns
		 * `NULL`. `COALESCE(NOT(NULL), true)` correctly turns this into a
		 * violation, preventing orphans.
		 */
		if (strcmp(fk_type, "temporal_to_temporal") == 0) /* Temporal FK */
		{
			StringInfoData join_buf, exclude_buf;
			char *fk_range_constructor, *uk_range_constructor;
			Datum get_range_type_values[3];
			int param_idx = 0;

			/* Get range constructors */
			if (get_range_type_plan == NULL)
			{
				const char *sql = "SELECT range_type::regtype::text FROM sql_saga.era WHERE table_schema = $1 AND table_name = $2 AND era_name = $3";
				Oid plan_argtypes[] = { NAMEOID, NAMEOID, NAMEOID };
				get_range_type_plan = SPI_prepare(sql, 3, plan_argtypes);
				if (get_range_type_plan == NULL) elog(ERROR, "SPI_prepare for get_range_type failed: %s", SPI_result_code_string(SPI_result));
				if (SPI_keepplan(get_range_type_plan) != 0) elog(ERROR, "SPI_keepplan for get_range_type failed");
			}
			get_range_type_values[0] = CStringGetDatum(fk_schema_name); get_range_type_values[1] = CStringGetDatum(fk_table_name); get_range_type_values[2] = CStringGetDatum(fk_era_name);
			ret = SPI_execute_plan(get_range_type_plan, get_range_type_values, NULL, true, 1);
			if (ret != SPI_OK_SELECT || SPI_processed == 0) elog(ERROR, "could not get range type for fk table");
			fk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
			get_range_type_values[0] = CStringGetDatum(uk_schema_name); get_range_type_values[1] = CStringGetDatum(uk_table_name); get_range_type_values[2] = CStringGetDatum(uk_era_name);
			ret = SPI_execute_plan(get_range_type_plan, get_range_type_values, NULL, true, 1);
			if (ret != SPI_OK_SELECT || SPI_processed == 0) elog(ERROR, "could not get range type for uk table");
			uk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

			initStringInfo(&join_buf); initStringInfo(&exclude_buf);
			plan_entry->nargs = num_uk_cols + 2;

			appendStringInfoString(&exclude_buf, " AND NOT (");
			for (i = 0; i < num_uk_cols; i++)
			{
				char *ukc = NameStr(*DatumGetName(uk_col_datums[i])); char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
				int attnum = SPI_fnumber(tupdesc, ukc);
				if (attnum <= 0) elog(ERROR, "column \"%s\" does not exist", ukc);
				if (i > 0) { appendStringInfoString(&join_buf, " AND "); appendStringInfoString(&where_buf, " AND "); appendStringInfoString(&exclude_buf, " AND "); }
				appendStringInfo(&join_buf, "fk.%s = uk.%s", quote_identifier(fkc), quote_identifier(ukc));
				appendStringInfo(&where_buf, "fk.%s = $%d", quote_identifier(fkc), param_idx + 1);
				appendStringInfo(&exclude_buf, "uk.%s = $%d", quote_identifier(ukc), param_idx + 1);
				plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, attnum);
				plan_entry->param_attnums[param_idx] = attnum;
				param_idx++;
			}
			
			if (num_uk_cols > 0) appendStringInfoString(&exclude_buf, " AND ");
			plan_entry->param_attnums[param_idx] = SPI_fnumber(tupdesc, uk_valid_from_column_name); plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums[param_idx]);
			appendStringInfo(&exclude_buf, "uk.%s = $%d", quote_identifier(uk_valid_from_column_name), param_idx + 1); param_idx++;
			appendStringInfoString(&exclude_buf, " AND ");
			plan_entry->param_attnums[param_idx] = SPI_fnumber(tupdesc, uk_valid_until_column_name); plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums[param_idx]);
			appendStringInfo(&exclude_buf, "uk.%s = $%d", quote_identifier(uk_valid_until_column_name), param_idx + 1);
			appendStringInfoChar(&exclude_buf, ')');

			query = psprintf(
				"SELECT EXISTS (SELECT 1 FROM %s.%s AS fk WHERE %s AND COALESCE(NOT ("
				"SELECT sql_saga.covers_without_gaps("
				"%s(uk.%s, uk.%s), %s(fk.%s, fk.%s) ORDER BY uk.%s"
				") FROM %s.%s AS uk WHERE %s%s), true))",
				quote_identifier(fk_schema_name), quote_identifier(fk_table_name), where_buf.data,
				uk_range_constructor, quote_identifier(uk_valid_from_column_name), quote_identifier(uk_valid_until_column_name),
				fk_range_constructor, quote_identifier(fk_valid_from_column_name), quote_identifier(fk_valid_until_column_name),
				quote_identifier(uk_valid_from_column_name), quote_identifier(uk_schema_name), quote_identifier(uk_table_name),
				join_buf.data, exclude_buf.data
			);
			pfree(join_buf.data); pfree(exclude_buf.data);
			if (fk_range_constructor) pfree(fk_range_constructor); if (uk_range_constructor) pfree(uk_range_constructor);
		}
		else /* Regular FK */
		{
			int param_idx = 0;
			plan_entry->nargs = num_uk_cols;
			for (i = 0; i < num_uk_cols; i++)
			{
				char *ukc = NameStr(*DatumGetName(uk_col_datums[i]));
				char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
				int attnum = SPI_fnumber(tupdesc, ukc);
				if (attnum <= 0) elog(ERROR, "column \"%s\" does not exist", ukc);
				if (i > 0) appendStringInfoString(&where_buf, " AND ");
				appendStringInfo(&where_buf, "fk.%s = $%d", quote_identifier(fkc), param_idx + 1);
				plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, attnum);
				plan_entry->param_attnums[param_idx] = attnum;
				param_idx++;
			}

			query = psprintf("SELECT EXISTS (SELECT 1 FROM %s.%s AS fk WHERE %s)",
				quote_identifier(fk_schema_name), quote_identifier(fk_table_name), where_buf.data);
		}
		
		plan_entry->plan = SPI_prepare(query, plan_entry->nargs, plan_entry->argtypes);
		if (!plan_entry->plan || SPI_keepplan(plan_entry->plan))
			elog(ERROR, "SPI_prepare/keepplan for validation query failed");

		pfree(query); pfree(where_buf.data);
		pfree(DatumGetPointer(uk_column_names_datum)); if (num_uk_cols > 0) pfree(uk_col_datums);
		pfree(DatumGetPointer(fk_column_names_datum)); if (num_fk_cols > 0) pfree(fk_col_datums);
	}

	/* Check for NULLs in UK columns of old_row using cached attnums */
	{
		int i;
		int num_uk_cols = (strcmp(fk_type, "temporal_to_temporal") == 0) ? plan_entry->nargs - 2 : plan_entry->nargs;

		for (i = 0; i < num_uk_cols; i++)
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
	char *fk_type;

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

	if (trigdata->tg_trigger->tgnargs != 17)
		elog(ERROR, "uk_update_check_c: expected 17 arguments, got %d", trigdata->tg_trigger->tgnargs);

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
	/* args 13, 14, 15 are match_type, update_action, delete_action */
	fk_type = tgargs[16];

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	init_uk_update_plan_cache();
	plan_entry = (UkUpdateValidationPlan *) hash_search(uk_update_plan_cache, &(trigdata->tg_trigger->tgoid), HASH_ENTER, &found);

	if (!found)
	{
		Datum uk_column_names_datum, fk_column_names_datum;
		ArrayType *uk_column_names_array, *fk_column_names_array;
		int num_uk_cols, num_fk_cols;
		Datum *uk_col_datums, *fk_col_datums;
		StringInfoData where_buf;
		char *query;
		int i;

		/* Get column name arrays */
		if (namearray_input_func_oid == InvalidOid)
		{
			int16 typlen; bool typbyval; char typalign, typdelim;
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
		
		initStringInfo(&where_buf);

		/*
		 * PRINCIPLE OF OPERATION FOR TEMPORAL FK ON UPDATE
		 *
		 * This is an AFTER ROW UPDATE trigger. The MVCC snapshot visible to
		 * this trigger's queries includes the NEW version of the updated row,
		 * but does NOT include the OLD version. This is the crucial challenge
		 * this trigger must solve. A simple query against the UK table would
		 * be checking coverage against an incomplete timeline.
		 *
		 * --- EXAMPLE ---
		 * UK Table: employees(id, name, valid_from, valid_until)
		 *   (1, 'Alice', '2022-01-01', '2023-01-01')
		 *   (1, 'Alice', '2023-01-01', 'infinity')  <- This row will be updated
		 *
		 * FK Table: projects(pid, employee_id, valid_from, valid_until)
		 *   (101, 1, '2022-06-01', '2023-06-01')
		 *
		 * Scenario: A user executes `UPDATE employees SET valid_from = '2023-02-01'
		 * WHERE id = 1 AND valid_from = '2023-01-01';` This would create a gap
		 * in Alice's timeline from '2023-01-01' to '2023-02-01'.
		 *
		 * --- QUERY VALIDATION LOGIC ---
		 * The validation query must check coverage against a "simulated"
		 * state of the UK table that represents the complete timeline of the
		 * entity *as if the update had occurred correctly*.
		 *
		 * 1. (Outer Query) Find Potentially Orphaned Rows:
		 *    `SELECT EXISTS (SELECT 1 FROM projects AS fk WHERE fk.employee_id = $1 ...)`
		 *    - Example: Finds project 101.
		 *
		 * 2. (Subquery) Check Timeline Coverage Against Simulated State:
		 *    The core of the logic is the `FROM` clause for the
		 *    `covers_without_gaps` aggregate, which constructs the
		 *    simulated post-update timeline via a UNION.
		 *
		 *    2.1. Get Unchanged Rows from MVCC Snapshot:
		 *         `(SELECT ... FROM employees AS uk WHERE ... AND NOT (uk.id = $1 AND ...))`
		 *         This selects all timeline segments for the entity from the
		 *         current MVCC snapshot, but it explicitly EXCLUDES the OLD
		 *         version of the row being updated (passed in from `old_row`).
		 *         Since the snapshot already contains the NEW version, this
		 *         effectively gathers all rows *not* involved in this specific
		 *         UPDATE.
		 *         - Example: The snapshot contains `(1, 'Alice', '2023-02-01', 'infinity')` (NEW)
		 *           and `(1, 'Alice', '2022-01-01', '2023-01-01')`. The `NOT (...)`
		 *           clause excludes the OLD version (`... valid_from = '2023-01-01' ...`).
		 *           The result of this SELECT is `(1, 'Alice', '2022-01-01', '2023-01-01')`
		 *
		 *    2.2. Add the New Row Version via UNION:
		 *         `... UNION ALL SELECT $4, $5, $6`
		 *         This adds the NEW version of the updated row to the set,
		 *         passed in as parameters from the `new_row` HeapTuple.
		 *         - Example: Adds `(1, 'Alice', '2023-02-01', 'infinity')`.
		 *
		 *    2.3. The Simulated Timeline:
		 *         The result of the UNION is the complete, simulated timeline
		 *         for the entity as it exists after the update.
		 *         - Example: The aggregate receives two rows:
		 *           `(1, 'Alice', '2022-01-01', '2023-01-01')`
		 *           `(1, 'Alice', '2023-02-01', 'infinity')`
		 *         This set correctly represents the timeline with the gap.
		 *
		 *    2.4. Perform Coverage Check:
		 *         The aggregate checks if this simulated timeline covers the
		 *         project's period `[2022-06-01, 2023-06-01)`. It does not.
		 *         The function returns `false`, which is negated to `true` to
		 *         signal a violation.
		 */
		if (strcmp(fk_type, "temporal_to_temporal") == 0) /* Temporal FK */
		{
			StringInfoData exclude_buf, union_buf, select_list_buf, alias_buf, join_buf;
			char *fk_range_constructor, *uk_range_constructor;
			Datum get_range_type_values[3];
			char *inner_alias = "sub_uk";
			int param_idx = 0;
			plan_entry->nargs = 2 * num_uk_cols + 4;

			if (get_range_type_plan == NULL)
			{
				const char *sql = "SELECT range_type::regtype::text FROM sql_saga.era WHERE table_schema = $1 AND table_name = $2 AND era_name = $3";
				Oid plan_argtypes[] = { NAMEOID, NAMEOID, NAMEOID };
				get_range_type_plan = SPI_prepare(sql, 3, plan_argtypes);
				if (get_range_type_plan == NULL) elog(ERROR, "SPI_prepare failed: %s", SPI_result_code_string(SPI_result));
				if (SPI_keepplan(get_range_type_plan) != 0) elog(ERROR, "SPI_keepplan failed");
			}
			get_range_type_values[0] = CStringGetDatum(fk_schema_name); get_range_type_values[1] = CStringGetDatum(fk_table_name); get_range_type_values[2] = CStringGetDatum(fk_era_name);
			ret = SPI_execute_plan(get_range_type_plan, get_range_type_values, NULL, true, 1);
			if (ret != SPI_OK_SELECT || SPI_processed == 0) elog(ERROR, "could not get range type for fk table");
			fk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
			get_range_type_values[0] = CStringGetDatum(uk_schema_name); get_range_type_values[1] = CStringGetDatum(uk_table_name); get_range_type_values[2] = CStringGetDatum(uk_era_name);
			ret = SPI_execute_plan(get_range_type_plan, get_range_type_values, NULL, true, 1);
			if (ret != SPI_OK_SELECT || SPI_processed == 0) elog(ERROR, "could not get range type for uk table");
			uk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

			initStringInfo(&exclude_buf); initStringInfo(&union_buf); initStringInfo(&select_list_buf); initStringInfo(&alias_buf); initStringInfo(&join_buf);
			
			appendStringInfoString(&exclude_buf, " AND NOT (");
			for (i = 0; i < num_uk_cols; i++) {
				char *ukc = NameStr(*DatumGetName(uk_col_datums[i])); char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
				int attnum = SPI_fnumber(tupdesc, ukc);
				if (attnum <= 0) elog(ERROR, "column \"%s\" does not exist", ukc);
				if (i > 0) { appendStringInfoString(&where_buf, " AND "); appendStringInfoString(&exclude_buf, " AND "); }
				appendStringInfo(&where_buf, "fk.%s = $%d", quote_identifier(fkc), param_idx + 1);
				appendStringInfo(&exclude_buf, "uk.%s = $%d", quote_identifier(ukc), param_idx + 1);
				plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, attnum);
				plan_entry->param_attnums_old[i] = attnum; param_idx++;
			}
			if (num_uk_cols > 0) appendStringInfoString(&exclude_buf, " AND ");
			plan_entry->param_attnums_old[num_uk_cols] = SPI_fnumber(tupdesc, uk_valid_from_column_name); plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums_old[num_uk_cols]);
			appendStringInfo(&exclude_buf, "uk.%s = $%d", quote_identifier(uk_valid_from_column_name), param_idx + 1); param_idx++;
			appendStringInfoString(&exclude_buf, " AND ");
			plan_entry->param_attnums_old[num_uk_cols+1] = SPI_fnumber(tupdesc, uk_valid_until_column_name); plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums_old[num_uk_cols+1]);
			appendStringInfo(&exclude_buf, "uk.%s = $%d", quote_identifier(uk_valid_until_column_name), param_idx + 1); param_idx++;
			appendStringInfoChar(&exclude_buf, ')');
			
			appendStringInfoString(&union_buf, " UNION ALL SELECT ");
			for (i = 0; i < num_uk_cols; i++) {
				char *ukc = NameStr(*DatumGetName(uk_col_datums[i])); int attnum = SPI_fnumber(tupdesc, ukc);
				if (i > 0) appendStringInfoString(&union_buf, ", ");
				appendStringInfo(&union_buf, "$%d", param_idx + 1);
				plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, attnum);
				plan_entry->param_attnums_new[i] = attnum; param_idx++;
			}
			appendStringInfoString(&union_buf, ", ");
			plan_entry->param_attnums_new[num_uk_cols] = SPI_fnumber(tupdesc, uk_valid_from_column_name); plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums_new[num_uk_cols]);
			appendStringInfo(&union_buf, "$%d", param_idx + 1); param_idx++;
			appendStringInfoString(&union_buf, ", ");
			plan_entry->param_attnums_new[num_uk_cols+1] = SPI_fnumber(tupdesc, uk_valid_until_column_name); plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, plan_entry->param_attnums_new[num_uk_cols+1]);
			appendStringInfo(&union_buf, "$%d", param_idx + 1);

			appendStringInfo(&alias_buf, " AS %s(", inner_alias);
			for (i = 0; i < num_uk_cols; i++) {
				char *ukc = NameStr(*DatumGetName(uk_col_datums[i])); char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
				if (i > 0) { appendStringInfoString(&select_list_buf, ", "); appendStringInfoString(&alias_buf, ", "); appendStringInfoString(&join_buf, " AND "); }
				appendStringInfo(&select_list_buf, "%s", quote_identifier(ukc)); appendStringInfo(&alias_buf, "%s", quote_identifier(ukc));
				appendStringInfo(&join_buf, "fk.%s = %s.%s", quote_identifier(fkc), inner_alias, quote_identifier(ukc));
			}
			appendStringInfo(&select_list_buf, ", %s, %s", quote_identifier(uk_valid_from_column_name), quote_identifier(uk_valid_until_column_name));
			appendStringInfo(&alias_buf, ", %s, %s)", quote_identifier(uk_valid_from_column_name), quote_identifier(uk_valid_until_column_name));

			query = psprintf("SELECT EXISTS (SELECT 1 FROM %s.%s AS fk WHERE %s AND COALESCE(NOT ("
				"SELECT sql_saga.covers_without_gaps("
				"%s(%s.%s, %s.%s), %s(fk.%s, fk.%s) ORDER BY %s.%s"
				") FROM (SELECT %s FROM %s.%s AS uk WHERE TRUE %s %s) %s WHERE %s), true))",
				quote_identifier(fk_schema_name), quote_identifier(fk_table_name), where_buf.data,
				uk_range_constructor, inner_alias, quote_identifier(uk_valid_from_column_name), inner_alias, quote_identifier(uk_valid_until_column_name),
				fk_range_constructor, quote_identifier(fk_valid_from_column_name), quote_identifier(fk_valid_until_column_name),
				inner_alias, quote_identifier(uk_valid_from_column_name),
				select_list_buf.data, quote_identifier(uk_schema_name), quote_identifier(uk_table_name),
				exclude_buf.data, union_buf.data, alias_buf.data, join_buf.data
			);
			pfree(exclude_buf.data); pfree(union_buf.data); pfree(select_list_buf.data); pfree(alias_buf.data); pfree(join_buf.data);
			if(fk_range_constructor) pfree(fk_range_constructor); if(uk_range_constructor) pfree(uk_range_constructor);
		}
		else /* Regular FK */
		{
			int param_idx = 0;
			plan_entry->nargs = num_uk_cols;
			for (i = 0; i < num_uk_cols; i++)
			{
				char *ukc = NameStr(*DatumGetName(uk_col_datums[i]));
				char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
				int attnum = SPI_fnumber(tupdesc, ukc);
				if (attnum <= 0) elog(ERROR, "column \"%s\" does not exist", ukc);
				if (i > 0) appendStringInfoString(&where_buf, " AND ");
				appendStringInfo(&where_buf, "fk.%s = $%d", quote_identifier(fkc), param_idx + 1);
				plan_entry->argtypes[param_idx] = SPI_gettypeid(tupdesc, attnum);
				plan_entry->param_attnums_old[i] = attnum;
				param_idx++;
			}
			query = psprintf("SELECT EXISTS (SELECT 1 FROM %s.%s AS fk WHERE %s)",
				quote_identifier(fk_schema_name), quote_identifier(fk_table_name), where_buf.data);
		}

		plan_entry->plan = SPI_prepare(query, plan_entry->nargs, plan_entry->argtypes);
		if (!plan_entry->plan || SPI_keepplan(plan_entry->plan))
			elog(ERROR, "SPI_prepare/keepplan for validation query failed");
		
		pfree(query); pfree(where_buf.data);
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
		Datum		values[MAX_UK_UPDATE_PLAN_ARGS];
		char		nulls[MAX_UK_UPDATE_PLAN_ARGS];
		int			i,
					param_idx = 0;

		if (strcmp(fk_type, "temporal_to_temporal") == 0)	/* Temporal FK */
		{
			bool		keys_are_equal = true;

			for (i = 0; i < plan_entry->num_uk_cols + 2; i++)
			{
				Datum		old_val,
							new_val;
				bool		old_isnull,
							new_isnull;

				old_val = heap_getattr(old_row, plan_entry->param_attnums_old[i], tupdesc, &old_isnull);
				new_val = heap_getattr(new_row, plan_entry->param_attnums_old[i], tupdesc, &new_isnull);

				if (old_isnull != new_isnull ||
					(!old_isnull && !datumIsEqual(old_val, new_val, tupdesc->attrs[plan_entry->param_attnums_old[i] - 1].attbyval, tupdesc->attrs[plan_entry->param_attnums_old[i] - 1].attlen)))
				{
					keys_are_equal = false;
					break;
				}
			}
			if (keys_are_equal)
			{
				SPI_finish();
				return PointerGetDatum(rettuple);
			}

			for (i = 0; i < plan_entry->num_uk_cols + 2; i++)
			{
				values[param_idx] = heap_getattr(old_row, plan_entry->param_attnums_old[i], tupdesc, &isnull);
				nulls[param_idx] = isnull ? 'n' : ' ';
				param_idx++;
			}
			for (i = 0; i < plan_entry->num_uk_cols + 2; i++)
			{
				values[param_idx] = heap_getattr(new_row, plan_entry->param_attnums_new[i], tupdesc, &isnull);
				nulls[param_idx] = isnull ? 'n' : ' ';
				param_idx++;
			}
		}
		else	/* Regular FK */
		{
			bool keys_are_equal = true;
			for (i = 0; i < plan_entry->num_uk_cols; i++)
			{
				Datum old_val, new_val;
				bool old_isnull, new_isnull;
				old_val = heap_getattr(old_row, plan_entry->param_attnums_old[i], tupdesc, &old_isnull);
				new_val = heap_getattr(new_row, plan_entry->param_attnums_old[i], tupdesc, &new_isnull);

				if (old_isnull != new_isnull ||
					(!old_isnull && !datumIsEqual(old_val, new_val, tupdesc->attrs[plan_entry->param_attnums_old[i]-1].attbyval, tupdesc->attrs[plan_entry->param_attnums_old[i]-1].attlen)))
				{
					keys_are_equal = false;
					break;
				}
			}
			if (keys_are_equal) { SPI_finish(); return PointerGetDatum(rettuple); }

			for (i = 0; i < plan_entry->num_uk_cols; i++) {
				values[i] = heap_getattr(old_row, plan_entry->param_attnums_old[i], tupdesc, &isnull);
				nulls[i] = isnull ? 'n' : ' ';
			}
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

Datum
generated_always_as_row_start_end(PG_FUNCTION_ARGS)
{
	TriggerData	   *trigdata = (TriggerData *) fcinfo->context;
	const char	   *funcname = "generated_always_as_row_start_end";
	Relation		rel;
	HeapTuple		new_row;
	TupleDesc		new_tupdesc;
	Datum			values[2];
	bool			nulls[2];
	int				columns[2];
	char		   *start_name, *end_name;
	int16			start_num, end_num;
	Oid				typeid;

	/*
	 * Make sure this is being called as an BEFORE ROW trigger.  Note:
	 * translatable error strings are shared with ri_triggers.c, so resist the
	 * temptation to fold the function name into them.
	 */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by trigger manager",
						funcname)));

	if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event) ||
		!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired BEFORE ROW",
						funcname)));

	/* Get Relation information */
	rel = trigdata->tg_relation;
	new_tupdesc = RelationGetDescr(rel);

	/* Get the new data that was inserted/updated */
	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		new_row = trigdata->tg_trigtuple;
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		HeapTuple old_row;

		old_row = trigdata->tg_trigtuple;
		new_row = trigdata->tg_newtuple;

		/* Don't change anything if only excluded columns are being updated. */
		if (OnlyExcludedColumnsChanged(rel, old_row, new_row))
			return PointerGetDatum(new_row);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired for INSERT or UPDATE",
						funcname)));
		new_row = NULL;			/* keep compiler quiet */
	}

	GetPeriodColumnNames(rel, "system_time", &start_name, &end_name);

	/* Get the column numbers and type */
	start_num = SPI_fnumber(new_tupdesc, start_name);
	end_num = SPI_fnumber(new_tupdesc, end_name);
	typeid = SPI_gettypeid(new_tupdesc, start_num);

	columns[0] = start_num;
	values[0] = GetRowStart(typeid);
	nulls[0] = false;
	columns[1] = end_num;
	values[1] = GetRowEnd(typeid);
	nulls[1] = false;
#if (PG_VERSION_NUM < 100000)
	new_row = SPI_modifytuple(rel, new_row, 2, columns, values, nulls);
#else
	new_row = heap_modify_tuple_by_cols(new_row, new_tupdesc, 2, columns, values, nulls);
#endif
    pfree(start_name);
    pfree(end_name);

	return PointerGetDatum(new_row);
}

Datum
write_history(PG_FUNCTION_ARGS)
{
	TriggerData	   *trigdata = (TriggerData *) fcinfo->context;
	const char	   *funcname = "write_history";
	Relation		rel;
	HeapTuple		old_row, new_row;
	TupleDesc		tupledesc;
	char		   *start_name, *end_name;
	int16			start_num, end_num;
	Oid				typeid;
	bool			is_null;
	Oid				history_id;
	int				cmp;
	bool			only_excluded_changed = false;

	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by trigger manager",
						funcname)));

	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) ||
		!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired AFTER ROW",
						funcname)));

	rel = trigdata->tg_relation;
	tupledesc = RelationGetDescr(rel);

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
	{
		old_row = NULL;
		new_row = trigdata->tg_trigtuple;
	}
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		old_row = trigdata->tg_trigtuple;
		new_row = trigdata->tg_newtuple;
		only_excluded_changed = OnlyExcludedColumnsChanged(rel, old_row, new_row);
	}
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		old_row = trigdata->tg_trigtuple;
		new_row = NULL;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired for INSERT or UPDATE or DELETE",
						funcname)));
		old_row = NULL;
		new_row = NULL;
	}

	GetPeriodColumnNames(rel, "system_time", &start_name, &end_name);

	start_num = SPI_fnumber(tupledesc, start_name);
	end_num = SPI_fnumber(tupledesc, end_name);
	typeid = SPI_gettypeid(tupledesc, start_num);

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event) ||
		(TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) && !only_excluded_changed))
	{
		Datum	start_datum = SPI_getbinval(new_row, tupledesc, start_num, &is_null);
		Datum	end_datum = SPI_getbinval(new_row, tupledesc, end_num, &is_null);

		if (CompareWithCurrentDatum(typeid, start_datum) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_GENERATED_ALWAYS),
					 errmsg("cannot insert or update column \"%s\"", start_name),
					 errdetail("Column \"%s\" is GENERATED ALWAYS AS ROW START", start_name)));

		if (CompareWithInfiniteDatum(typeid, end_datum) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_GENERATED_ALWAYS),
					 errmsg("cannot insert or update column \"%s\"", end_name),
					 errdetail("Column \"%s\" is GENERATED ALWAYS AS ROW END", end_name)));

		if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
        {
            pfree(start_name);
            pfree(end_name);
			return PointerGetDatum(NULL);
        }
	}

	if (only_excluded_changed)
    {
        pfree(start_name);
        pfree(end_name);
		return PointerGetDatum(NULL);
    }

	cmp = CompareWithCurrentDatum(typeid, SPI_getbinval(old_row, tupledesc, start_num, &is_null));

	if (cmp == 0)
    {
        pfree(start_name);
        pfree(end_name);
		return PointerGetDatum(NULL);
    }

	if (cmp > 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ROW_VERSION),
				 errmsg("invalid row version"),
				 errdetail("The row being updated or deleted was created after this transaction started."),
				 errhint("The transaction might succeed if retried.")));

	history_id = GetHistoryTable(rel);
	if (OidIsValid(history_id))
	{
		Relation	history_rel;
		TupleDesc	history_tupledesc;
		HeapTuple	history_tuple;
		int16		history_end_num;
		TupleConversionMap   *map;
		Datum	   *values;
		bool	   *nulls;

		history_rel = table_open(history_id, RowExclusiveLock);
		history_tupledesc = RelationGetDescr(history_rel);
		history_end_num = SPI_fnumber(history_tupledesc, end_name);

#if (PG_VERSION_NUM < 130000)
		map = convert_tuples_by_name(tupledesc, history_tupledesc, gettext_noop("could not convert row type"));
#else
		map = convert_tuples_by_name(tupledesc, history_tupledesc);
#endif
		if (map != NULL)
		{
#if (PG_VERSION_NUM < 120000)
			history_tuple = do_convert_tuple(old_row, map);
#else
			history_tuple = execute_attr_map_tuple(old_row, map);
#endif
			free_conversion_map(map);
		}
		else
		{
			history_tuple = old_row;
			history_tupledesc = tupledesc;
		}

		values = (Datum *) palloc(history_tupledesc->natts * sizeof(Datum));
		nulls = (bool *) palloc(history_tupledesc->natts * sizeof(bool));

		heap_deform_tuple(history_tuple, history_tupledesc, values, nulls);
		values[history_end_num-1] = GetRowStart(typeid);
		nulls[history_end_num-1] = false;
		history_tuple = heap_form_tuple(history_tupledesc, values, nulls);

		pfree(values);
		pfree(nulls);

		insert_into_history(history_rel, history_tuple);

		table_close(history_rel, NoLock);
	}
    pfree(start_name);
    pfree(end_name);

	return PointerGetDatum(NULL);
}


void _PG_init(void) {
}

void _PG_fini(void) {
}

