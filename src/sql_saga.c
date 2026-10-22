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
#include "catalog/pg_proc.h"
#include "catalog/namespace.h"
#include "commands/event_trigger.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/regproc.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "access/stratnum.h"
#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "catalog/pg_operator.h"
#include "commands/defrem.h"
#include "utils/acl.h"
#include "catalog/pg_class.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"

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
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_func.h"
#include "tcop/deparse_utility.h"
#include "utils/date.h"
#if (PG_VERSION_NUM >= 100000)
#include "utils/fmgrprotos.h"
#endif
#include "utils/hsearch.h"
#include "utils/rangetypes.h"
#include "utils/timestamp.h"
#include "access/relation.h"
#include "catalog/pg_namespace.h"
#include "rewrite/rewriteHandler.h"
#include "utils/ruleutils.h"

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
	SPIPlanPtr	key_plan;	/* regular FK, referenced side: does the key still exist in the parent? */
	SPIPlanPtr	cascade_plan;	/* regular FK, ON DELETE CASCADE: delete the children of a vanished key */
} FkValidationPlan;

/*
 * FK/UK plan caches — transaction-scoped with explicit cleanup.
 *
 * These HTAB caches store SPI plans for FK validation and UK enforcement queries,
 * keyed by trigger OID. Plans are created via SPI_prepare() + SPI_keepplan() on
 * first use, then reused for subsequent trigger firings within the same transaction.
 *
 * Lifecycle: the caches live in TopMemoryContext and the plans in CacheMemoryContext (SPI_keepplan).
 * cache_cleanup_callback, registered once per backend, frees every plan and destroys the caches at the end of
 * each top-level transaction (commit, abort or prepare). Up to 1.3 the plans were never freed and the callback was
 * registered again in every transaction, so a long-lived session grew with every transaction that fired a trigger.
 * An entry is complete when its main plan is set (it is kept last); one left incomplete by an error caught in a
 * subtransaction is rebuilt (plan_cache_entry_usable).
 * See: src/backend/access/transam/xact.c — CallXactCallbacks() invocation points.
 *
 * Why transaction-scoped (not session-scoped): FK/UK triggers reference user tables
 * whose columns and constraints can change between transactions. Rebuilding plans
 * each transaction ensures correctness without needing relcache invalidation hooks.
 * The cost is one SPI_prepare per distinct trigger OID per transaction — negligible
 * because SPI_prepare is fast (~microseconds) and trigger sets are small.
 *
 * Note: The static SPIPlanPtr variables (get_range_type_plan in OnlyExcludedColumnsChanged,
 * qplan in GetHistoryTable) are session-scoped and have NO invalidation mechanism.
 * These query sql_saga catalog tables (sql_saga.era, sql_saga.system_versioning) whose
 * schema is defined by the extension and does not change during normal operation.
 * An ALTER on these catalog tables would require DROP/CREATE EXTENSION, which destroys
 * the backend's cached plans anyway.
 */
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
	SPIPlanPtr	key_plan;	/* regular FK: does the old key still exist in the parent? */
} UkUpdateValidationPlan;

static HTAB *fk_plan_cache = NULL;
static HTAB *uk_delete_plan_cache = NULL;
static HTAB *uk_update_plan_cache = NULL;
static bool cache_callback_registered = false;

/* Free the kept plans of a transaction's cache and the cache itself. */
static void
fk_plan_cache_destroy(HTAB **cache, bool update_entries)
{
	HASH_SEQ_STATUS status;
	void	   *entry;

	if (*cache == NULL)
		return;
	hash_seq_init(&status, *cache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		SPIPlanPtr	plans[3];
		int			i;

		if (update_entries)
		{
			plans[0] = ((UkUpdateValidationPlan *) entry)->plan;
			plans[1] = ((UkUpdateValidationPlan *) entry)->key_plan;
			plans[2] = NULL;
		}
		else
		{
			plans[0] = ((FkValidationPlan *) entry)->plan;
			plans[1] = ((FkValidationPlan *) entry)->key_plan;
			plans[2] = ((FkValidationPlan *) entry)->cascade_plan;
		}
		for (i = 0; i < 3; i++)
			if (plans[i] != NULL)
				SPI_freeplan(plans[i]);
	}
	hash_destroy(*cache);
	*cache = NULL;
}

static void
cache_cleanup_callback(XactEvent event, void *arg)
{
	/*
	 * At transaction end, free the plans the triggers kept for the transaction, and the caches. The caches live in
	 * TopMemoryContext, not in CurTransactionContext: a cache first created in a subtransaction must survive that
	 * subtransaction's abort, and the kept plans (in CacheMemoryContext) must be freed explicitly, or a long-lived
	 * session grows by them with every transaction. The callback is registered once per backend.
	 */
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_PREPARE:
			fk_plan_cache_destroy(&fk_plan_cache, false);
			fk_plan_cache_destroy(&uk_delete_plan_cache, false);
			fk_plan_cache_destroy(&uk_update_plan_cache, true);
			break;
		default:
			break;
	}
}

/*
 * Called right after hash_search(HASH_ENTER): true when the entry holds a complete build. A new entry, or one whose
 * build failed part way (an error caught by a subtransaction; the main plan is kept last), is reset, freeing what
 * it did keep, so the caller builds it (again). cascade_plan may be NULL for entries without one.
 */
static bool
plan_cache_entry_usable(bool found, SPIPlanPtr *plan, SPIPlanPtr *key_plan, SPIPlanPtr *cascade_plan)
{
	if (found && *plan != NULL)
		return true;
	if (found)
	{
		if (*key_plan != NULL)
			SPI_freeplan(*key_plan);
		if (cascade_plan != NULL && *cascade_plan != NULL)
			SPI_freeplan(*cascade_plan);
	}
	*plan = NULL;
	*key_plan = NULL;
	if (cascade_plan != NULL)
		*cascade_plan = NULL;
	return false;
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
	/* Emptied and destroyed at transaction end (cache_cleanup_callback) */
	ctl.hcxt = TopMemoryContext;
	fk_plan_cache = hash_create("sql_saga fk validation plan cache", 16, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
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
	ctl.hcxt = TopMemoryContext;
	uk_delete_plan_cache = hash_create("sql_saga uk delete validation plan cache", 16, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

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
	ctl.hcxt = TopMemoryContext;
	uk_update_plan_cache = hash_create("sql_saga uk update validation plan cache", 16, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static SPIPlanPtr get_range_type_plan = NULL;

/* For NAMEARRAYOID type IO */
static Oid namearray_input_func_oid = InvalidOid;
static Oid namearray_ioparam_oid = InvalidOid;


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


/*
 * Columns of a system-versioned table and its history correspond by NAME, never by position
 * (tmp/design_manual_schema_changes.md). The helpers below find live columns by name, refuse a
 * history that cannot take every base value, and build the history row.
 */
static AttrNumber
FindLiveAttributeByName(TupleDesc desc, const char *name)
{
	int			i;

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);

		if (!attr->attisdropped && strcmp(NameStr(attr->attname), name) == 0)
			return attr->attnum;
	}
	return InvalidAttrNumber;
}

/*
 * A column type as written in DDL. Use the same SQL helper as rebuild-time validation so type
 * qualification and COLLATE rendering are deterministic and independent of the caller's search_path.
 */
static char *
FormatColumnType(Form_pg_attribute attr)
{
	Oid			argtypes[3] = {OIDOID, INT4OID, OIDOID};
	Oid			function_oid;
	Datum		result;

	function_oid = LookupFuncName(list_make2(makeString("sql_saga"),
										 makeString("__internal_column_type_sql")),
							 3, argtypes, false);
	result = OidFunctionCall3(function_oid,
						  ObjectIdGetDatum(attr->atttypid),
						  Int32GetDatum(attr->atttypmod),
						  ObjectIdGetDatum(attr->attcollation));
	return TextDatumGetCString(result);
}

/* Relation name as the caller's search_path shows it (as regclass output does). */
static char *
VisibleRelationName(Relation rel)
{
	return DatumGetCString(DirectFunctionCall1(regclassout, ObjectIdGetDatum(RelationGetRelid(rel))));
}

/* Schema-qualified relation name, for DDL in hints that must work whatever the search_path. */
static char *
QualifiedRelationName(Relation rel)
{
	return quote_qualified_identifier(get_namespace_name(RelationGetNamespace(rel)),
									  RelationGetRelationName(rel));
}

static void
CheckHistoryTupleCompatibility(Relation base_rel, Relation history_rel)
{
	TupleDesc	base_desc = RelationGetDescr(base_rel);
	TupleDesc	history_desc = RelationGetDescr(history_rel);
	int			i;

	for (i = 0; i < base_desc->natts; i++)
	{
		Form_pg_attribute battr = TupleDescAttr(base_desc, i);
		Form_pg_attribute hattr;
		AttrNumber	history_attnum;
		char	   *base_type;
		char	   *rebuild_call;

		if (battr->attisdropped)
			continue;

		history_attnum = FindLiveAttributeByName(history_desc, NameStr(battr->attname));
		if (history_attnum != InvalidAttrNumber)
		{
			hattr = TupleDescAttr(history_desc, history_attnum - 1);
			if (hattr->atttypid == battr->atttypid &&
				hattr->atttypmod == battr->atttypmod &&
				hattr->attcollation == battr->attcollation)
				continue;
		}
		else
			hattr = NULL;

		base_type = FormatColumnType(battr);
		rebuild_call = psprintf("SELECT sql_saga.rebuild_system_versioning_views(%s);",
								quote_literal_cstr(QualifiedRelationName(base_rel)));

		if (hattr == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("sql_saga: column %s of base table %s is missing from history table %s",
							quote_identifier(NameStr(battr->attname)),
							VisibleRelationName(base_rel), VisibleRelationName(history_rel)),
					 errhint("Run: ALTER TABLE %s ADD COLUMN %s %s; %s",
							 QualifiedRelationName(history_rel),
							 quote_identifier(NameStr(battr->attname)), base_type, rebuild_call)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("sql_saga: column %s has type %s in base table %s but type %s in history table %s",
							quote_identifier(NameStr(battr->attname)), base_type,
							VisibleRelationName(base_rel), FormatColumnType(hattr),
							VisibleRelationName(history_rel)),
					 errhint("Run: ALTER TABLE %s ALTER COLUMN %s TYPE %s; %s",
							 QualifiedRelationName(history_rel),
							 quote_identifier(NameStr(battr->attname)), base_type, rebuild_call)));
	}
}

/*
 * The history row for a base row: each live history column takes the base column of the same
 * name, history-only columns are NULL (no default is applied), the range column gets range_value.
 * Assumes CheckHistoryTupleCompatibility passed.
 */
static HeapTuple
ProjectBaseTupleToHistoryByName(HeapTuple base_tuple, TupleDesc base_desc, TupleDesc history_desc,
								AttrNumber history_range_num, Datum range_value)
{
	Datum	   *values;
	bool	   *nulls;
	int			i;
	HeapTuple	result;

	values = (Datum *) palloc0(history_desc->natts * sizeof(Datum));
	nulls = (bool *) palloc(history_desc->natts * sizeof(bool));

	for (i = 0; i < history_desc->natts; i++)
	{
		Form_pg_attribute hattr = TupleDescAttr(history_desc, i);
		AttrNumber	base_attnum;

		nulls[i] = true;
		if (hattr->attisdropped)
			continue;
		base_attnum = FindLiveAttributeByName(base_desc, NameStr(hattr->attname));
		if (base_attnum != InvalidAttrNumber)
			values[i] = heap_getattr(base_tuple, base_attnum, base_desc, &nulls[i]);
	}

	values[history_range_num - 1] = range_value;
	nulls[history_range_num - 1] = false;

	result = heap_form_tuple(history_desc, values, nulls);
	pfree(values);
	pfree(nulls);
	return result;
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
PG_FUNCTION_INFO_V1(regular_fk_check_c);
PG_FUNCTION_INFO_V1(uk_truncate_check_c);
PG_FUNCTION_INFO_V1(generated_always_as_row_start_end);
PG_FUNCTION_INFO_V1(write_history);
PG_FUNCTION_INFO_V1(alter_table_changes_column_shape);
PG_FUNCTION_INFO_V1(refuse_managed_relation_identity_change);
PG_FUNCTION_INFO_V1(record_alter_function_resolved);

/* ---- 1.3 A3: managed updatable views follow DROP COLUMN (see src/2o_updatable_view_column_follow.sql) ---- */
/*
 * Runs one SPI statement with the identity of the owner of the calling (event trigger) function, the
 * extension owner, as record_alter_function_resolved does. The internal functions the event triggers
 * call are not executable by PUBLIC, and DDL by any role (on any table) must not depend on such grants.
 * The caller has done SPI_connect.
 */
static int
spi_execute_as_function_owner(FunctionCallInfo fcinfo, const char *sql, int nargs,
							  Oid *argtypes, Datum *values, const char *nulls, bool read_only)
{
	HeapTuple	proc_tuple;
	Oid			function_owner;
	Oid			saved_userid;
	int			saved_sec_context;
	int			ret = 0;

	proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fcinfo->flinfo->fn_oid));
	if (!HeapTupleIsValid(proc_tuple))
		elog(ERROR, "cache lookup failed for function %u", fcinfo->flinfo->fn_oid);
	function_owner = ((Form_pg_proc) GETSTRUCT(proc_tuple))->proowner;
	ReleaseSysCache(proc_tuple);

	GetUserIdAndSecContext(&saved_userid, &saved_sec_context);
	SetUserIdAndSecContext(function_owner,
						   saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		if (nargs == 0)
			ret = SPI_execute(sql, read_only, 0);
		else
			ret = SPI_execute_with_args(sql, nargs, argtypes, values, nulls, read_only, 0);
	}
	PG_FINALLY();
	{
		SetUserIdAndSecContext(saved_userid, saved_sec_context);
	}
	PG_END_TRY();
	return ret;
}

/*
 * ddl_command_start of ALTER TABLE ... DROP COLUMN: hand the table and the dropped column names to
 * sql_saga.__internal_updatable_view_prepare_drop_column, which records and drops the managed views
 * that show those columns (recreated at ddl_command_end), so that PostgreSQL lets the column go.
 * Does nothing when the installed SQL version has no such function (older extension scripts loaded
 * with this library).
 */
static void
prepare_updatable_view_drop_column(FunctionCallInfo fcinfo, AlterTableStmt *stmt)
{
	ListCell   *cell;
	List	   *column_names = NIL;
	Oid			relation_oid;
	Datum	   *elems;
	ArrayType  *names_array;
	Oid			argtypes[] = {OIDOID, TEXTARRAYOID, BOOLOID};
	Datum		values[3];
	char		nulls[] = {' ', ' ', ' '};
	int			n;
	int			i;
	int			ret;

	if (stmt->objtype != OBJECT_TABLE || stmt->relation == NULL)
		return;

	foreach(cell, stmt->cmds)
	{
		AlterTableCmd *alter_cmd = lfirst_node(AlterTableCmd, cell);

		if (alter_cmd->subtype == AT_DropColumn && alter_cmd->name != NULL)
			column_names = lappend(column_names, alter_cmd->name);
	}
	if (column_names == NIL)
		return;

	relation_oid = RangeVarGetRelid(stmt->relation, NoLock, true);
	if (!OidIsValid(relation_oid))
		return;

	/*
	 * ddl_command_start runs before PostgreSQL checks ownership: leave a caller who may not alter the
	 * table to that check, so nothing is done on its behalf as the extension owner.
	 */
	if (!object_ownercheck(RelationRelationId, relation_oid, GetUserId()))
		return;

	if (!OidIsValid(LookupFuncName(list_make2(makeString("sql_saga"),
											  makeString("__internal_updatable_view_prepare_drop_column")),
								   3, argtypes, true)))
		return;

	n = list_length(column_names);
	elems = (Datum *) palloc(sizeof(Datum) * n);
	i = 0;
	foreach(cell, column_names)
		elems[i++] = CStringGetTextDatum((char *) lfirst(cell));
	names_array = construct_array(elems, n, TEXTOID, -1, false, TYPALIGN_INT);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	values[0] = ObjectIdGetDatum(relation_oid);
	values[1] = PointerGetDatum(names_array);
	values[2] = BoolGetDatum(stmt->relation->inh);
	ret = spi_execute_as_function_owner(fcinfo,
		"SELECT sql_saga.__internal_updatable_view_prepare_drop_column($1, $2, $3)",
		3, argtypes, values, nulls, false);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute returned %s", SPI_result_code_string(ret));

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}
/* ---- end 1.3 A3 ---- */

/* ---- 1.3 TYPE: managed updatable views follow ALTER COLUMN ... TYPE (see src/2o_updatable_view_column_follow.sql) ---- */
/*
 * ddl_command_start of ALTER TABLE ... ALTER COLUMN ... TYPE: hand the table, the retyped column names
 * and the column names the same statement drops to sql_saga.__internal_updatable_view_prepare_alter_type.
 * It refuses type changes of era-bound and foreign key columns, and records and drops the managed views
 * that show a retyped column (recreated at ddl_command_end), so that PostgreSQL lets the type change.
 * Does nothing when the installed SQL version has no such function. Returns true when the statement
 * has a type change (then the DROP COLUMN preparation is part of it and must not run separately).
 */
static bool
prepare_updatable_view_alter_column_type(FunctionCallInfo fcinfo, AlterTableStmt *stmt)
{
	ListCell   *cell;
	List	   *type_names = NIL;
	List	   *drop_names = NIL;
	Oid			relation_oid;
	Datum	   *elems;
	ArrayType  *type_array;
	ArrayType  *drop_array;
	Oid			argtypes[] = {OIDOID, TEXTARRAYOID, TEXTARRAYOID, BOOLOID};
	Datum		values[4];
	char		nulls[] = {' ', ' ', ' ', ' '};
	int			n;
	int			i;
	int			ret;

	if (stmt->objtype != OBJECT_TABLE || stmt->relation == NULL)
		return false;

	foreach(cell, stmt->cmds)
	{
		AlterTableCmd *alter_cmd = lfirst_node(AlterTableCmd, cell);

		if (alter_cmd->subtype == AT_AlterColumnType && alter_cmd->name != NULL)
			type_names = lappend(type_names, alter_cmd->name);
		else if (alter_cmd->subtype == AT_DropColumn && alter_cmd->name != NULL)
			drop_names = lappend(drop_names, alter_cmd->name);
	}
	if (type_names == NIL)
		return false;

	relation_oid = RangeVarGetRelid(stmt->relation, NoLock, true);
	if (!OidIsValid(relation_oid))
		return true;

	/* As for DROP COLUMN: leave a caller who may not alter the table to PostgreSQL's own check. */
	if (!object_ownercheck(RelationRelationId, relation_oid, GetUserId()))
		return true;

	if (!OidIsValid(LookupFuncName(list_make2(makeString("sql_saga"),
											  makeString("__internal_updatable_view_prepare_alter_type")),
								   4, argtypes, true)))
		return false;

	n = list_length(type_names);
	elems = (Datum *) palloc(sizeof(Datum) * n);
	i = 0;
	foreach(cell, type_names)
		elems[i++] = CStringGetTextDatum((char *) lfirst(cell));
	type_array = construct_array(elems, n, TEXTOID, -1, false, TYPALIGN_INT);

	n = list_length(drop_names);
	elems = (Datum *) palloc(sizeof(Datum) * (n > 0 ? n : 1));
	i = 0;
	foreach(cell, drop_names)
		elems[i++] = CStringGetTextDatum((char *) lfirst(cell));
	drop_array = construct_array(elems, n, TEXTOID, -1, false, TYPALIGN_INT);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	values[0] = ObjectIdGetDatum(relation_oid);
	values[1] = PointerGetDatum(type_array);
	values[2] = PointerGetDatum(drop_array);
	/* A type change recurses to the descendants unless ONLY was given (PostgreSQL then refuses it). */
	values[3] = BoolGetDatum(stmt->relation->inh);
	ret = spi_execute_as_function_owner(fcinfo,
		"SELECT sql_saga.__internal_updatable_view_prepare_alter_type($1, $2, $3, $4)",
		4, argtypes, values, nulls, false);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute returned %s", SPI_result_code_string(ret));

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
	return true;
}
/* ---- end 1.3 TYPE ---- */

Datum
refuse_managed_relation_identity_change(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata;
	RangeVar   *relation = NULL;
	char	   *schema_name = NULL;
	bool		missing_ok = false;
	Oid		relation_oid;
	Oid		argtypes[] = {OIDOID};
	Oid		schema_argtypes[] = {TEXTOID};
	Datum		values[1];
	Datum		schema_values[1];
	char		nulls[] = {' '};
	int		ret;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "refuse_managed_relation_identity_change: not called by event trigger manager");

	trigdata = (EventTriggerData *) fcinfo->context;
	if (IsA(trigdata->parsetree, RenameStmt))
	{
		RenameStmt *stmt = castNode(RenameStmt, trigdata->parsetree);

		if (stmt->renameType == OBJECT_SCHEMA)
		{
			if (stmt->subname == NULL)
				PG_RETURN_NULL();
			schema_name = stmt->subname;
		}
		else if (stmt->renameType != OBJECT_TABLE && stmt->renameType != OBJECT_VIEW)
			PG_RETURN_NULL();
		else
		{
			relation = stmt->relation;
			missing_ok = stmt->missing_ok;
		}
	}
	else if (IsA(trigdata->parsetree, AlterObjectSchemaStmt))
	{
		AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, trigdata->parsetree);

		if (stmt->objectType != OBJECT_TABLE && stmt->objectType != OBJECT_VIEW)
			PG_RETURN_NULL();
		relation = stmt->relation;
		missing_ok = stmt->missing_ok;
	}
	else if (IsA(trigdata->parsetree, AlterTableStmt))
	{
		if (!prepare_updatable_view_alter_column_type(fcinfo, castNode(AlterTableStmt, trigdata->parsetree)))	/* 1.3 TYPE */
			prepare_updatable_view_drop_column(fcinfo, castNode(AlterTableStmt, trigdata->parsetree));
		PG_RETURN_NULL();
	}
	else
		PG_RETURN_NULL();

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	if (schema_name != NULL)
	{
		schema_values[0] = CStringGetTextDatum(schema_name);
		ret = spi_execute_as_function_owner(fcinfo,
			"SELECT sql_saga.__internal_refuse_managed_schema_rename($1)",
			1, schema_argtypes, schema_values, nulls, false);
	}
	else
	{
		if (relation == NULL)
		{
			SPI_finish();
			PG_RETURN_NULL();
		}

		relation_oid = RangeVarGetRelid(relation, NoLock, missing_ok);
		if (!OidIsValid(relation_oid))
		{
			SPI_finish();
			PG_RETURN_NULL();
		}

		values[0] = ObjectIdGetDatum(relation_oid);
		ret = spi_execute_as_function_owner(fcinfo,
			"SELECT sql_saga.__internal_refuse_managed_relation_identity_change($1)",
			1, argtypes, values, nulls, false);
	}
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute returned %s", SPI_result_code_string(ret));

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	PG_RETURN_NULL();
}

Datum
record_alter_function_resolved(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata;
	ObjectWithArgs *function = NULL;
	ObjectType object_type = OBJECT_FUNCTION;
	Oid function_oid = InvalidOid;
	Oid argtypes[] = {TEXTOID};
	Datum values[1];
	char nulls[] = {'n'};
	char *function_identity = NULL;
	int ret;
	HeapTuple proc_tuple;
	Oid function_owner;
	Oid saved_userid;
	int saved_sec_context;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "record_alter_function_resolved: not called by event trigger manager");

	trigdata = (EventTriggerData *) fcinfo->context;
	if (IsA(trigdata->parsetree, RenameStmt))
	{
		RenameStmt *stmt = castNode(RenameStmt, trigdata->parsetree);

		object_type = stmt->renameType;
		function = castNode(ObjectWithArgs, stmt->object);
	}
	else if (IsA(trigdata->parsetree, AlterObjectSchemaStmt))
	{
		AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, trigdata->parsetree);

		object_type = stmt->objectType;
		function = castNode(ObjectWithArgs, stmt->object);
	}
	else if (IsA(trigdata->parsetree, AlterOwnerStmt))
	{
		AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, trigdata->parsetree);

		object_type = stmt->objectType;
		function = castNode(ObjectWithArgs, stmt->object);
	}
	else if (IsA(trigdata->parsetree, AlterFunctionStmt))
	{
		AlterFunctionStmt *stmt = castNode(AlterFunctionStmt, trigdata->parsetree);

		object_type = stmt->objtype;
		function = stmt->func;
	}

	if (function != NULL)
		function_oid = LookupFuncWithArgs(object_type, function, true);
	if (OidIsValid(function_oid))
	{
		function_identity = format_procedure_qualified(function_oid);
		values[0] = CStringGetTextDatum(function_identity);
		nulls[0] = ' ';
	}

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fcinfo->flinfo->fn_oid));
	if (!HeapTupleIsValid(proc_tuple))
		elog(ERROR, "cache lookup failed for function %u", fcinfo->flinfo->fn_oid);
	function_owner = ((Form_pg_proc) GETSTRUCT(proc_tuple))->proowner;
	ReleaseSysCache(proc_tuple);

	GetUserIdAndSecContext(&saved_userid, &saved_sec_context);
	SetUserIdAndSecContext(function_owner,
						 saved_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	PG_TRY();
	{
		ret = SPI_execute_with_args(
				"SELECT sql_saga.__internal_set_alter_function_resolved($1)",
				1, argtypes, values, nulls, false, 0);
	}
	PG_FINALLY();
	{
		SetUserIdAndSecContext(saved_userid, saved_sec_context);
	}
	PG_END_TRY();
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute returned %s", SPI_result_code_string(ret));

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	PG_RETURN_NULL();
}

Datum
alter_table_changes_column_shape(PG_FUNCTION_ARGS)
{
	CollectedCommand *cmd = (CollectedCommand *) PG_GETARG_POINTER(0);
	ListCell   *cell;

	if (cmd->type != SCT_AlterTable)
		PG_RETURN_BOOL(false);

	foreach(cell, cmd->d.alterTable.subcmds)
	{
		CollectedATSubcmd *sub = lfirst(cell);
		AlterTableCmd *alter_cmd = castNode(AlterTableCmd, sub->parsetree);

		if (alter_cmd->subtype == AT_AddColumn ||
			alter_cmd->subtype == AT_DropColumn)
			PG_RETURN_BOOL(true);
	}

	PG_RETURN_BOOL(false);
}

/*
 * Regular (non-temporal) to temporal foreign keys (design B, tmp/fable_audit_v13.md section 2.3).
 *
 * The referencing side is a constraint trigger (regular_fk_check_c) and the referenced side the
 * uk_update / uk_delete constraint triggers. All three take the same 17 arguments
 * (sql_saga.__internal_regular_fk_trigger_args): foreign key name, child schema, child table, child
 * columns, three empty temporal child arguments, parent schema, parent table, parent key columns,
 * parent era, its valid_from and valid_until columns, match type, update action, delete action and
 * 'regular_to_temporal'. Child and parent columns are paired by position.
 *
 * The contract of a regular reference: the referenced key exists in the parent at some time, that
 * is, in at least one parent row. The lookup "does the key exist" is
 *     SELECT 1 FROM parent AS uk WHERE uk.c1 OPERATOR(ns.=) $1 AND ... FOR KEY SHARE OF uk
 * with the equality operator of each column's default btree operator class (the semantics a native
 * foreign key compares with, e.g. citext case-insensitive). FOR KEY SHARE locks the parent rows of
 * the key, so a concurrent transaction deleting them (or changing their key) waits for this one and
 * then sees the reference, as native referential integrity does. The lookups run as the owner of the
 * C function (the extension owner) with row level security not forced, so whoever writes the child
 * needs no rights on the parent and policies cannot hide parent rows (native foreign keys behave the
 * same way).
 */

/*
 * The owner of a table, the identity native referential integrity runs its queries as (ri_PlanCheck,
 * ri_PerformCheck): the referenced table's owner for a lookup in the referenced table, the referencing table's owner
 * for a query on the referencing table (does a child reference the key, the ON DELETE CASCADE delete). A role that
 * owns the queried table needs no rights through the caller, and the caller needs none on it (also not USAGE on its
 * schema). Neither the extension owner nor the caller lends its rights.
 */
static Oid
regular_fk_table_owner(const char *schema_name, const char *table_name)
{
	Oid			nsp = get_namespace_oid(schema_name, false);
	Oid			relid = get_relname_relid(table_name, nsp);
	HeapTuple	tup;
	Oid			owner;

	if (!OidIsValid(relid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("relation \"%s.%s\" does not exist", schema_name, table_name)));
	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	owner = ((Form_pg_class) GETSTRUCT(tup))->relowner;
	ReleaseSysCache(tup);
	return owner;
}

/* SPI_prepare + SPI_keepplan as the given role (the owner of the table the query reads or changes). */
static SPIPlanPtr
regular_fk_prepare_as(Oid query_owner, const char *sql, int nargs, Oid *argtypes)
{
	Oid			saved_userid;
	int			saved_sec_context;
	SPIPlanPtr	plan = NULL;

	GetUserIdAndSecContext(&saved_userid, &saved_sec_context);
	SetUserIdAndSecContext(query_owner,
						   saved_sec_context | SECURITY_LOCAL_USERID_CHANGE | SECURITY_NOFORCE_RLS);
	PG_TRY();
	{
		plan = SPI_prepare(sql, nargs, argtypes);
	}
	PG_FINALLY();
	{
		SetUserIdAndSecContext(saved_userid, saved_sec_context);
	}
	PG_END_TRY();
	if (plan == NULL)
		elog(ERROR, "SPI_prepare for regular foreign key query failed: %s", SPI_result_code_string(SPI_result));
	if (SPI_keepplan(plan))
		elog(ERROR, "SPI_keepplan for regular foreign key query failed");
	return plan;
}

/* "OPERATOR(schema.op)" of the default btree equality operator for a column type. */
static char *
regular_fk_equality_operator(Oid typid)
{
	Oid			basetype = getBaseType(typid);
	Oid			opclass = GetDefaultOpClass(basetype, BTREE_AM_OID);
	Oid			opfamily;
	Oid			opcintype;
	Oid			opr;
	HeapTuple	tup;
	Form_pg_operator oprform;
	char	   *result;

	if (!OidIsValid(opclass))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("type %s has no default btree operator class, so a foreign key cannot compare it",
						format_type_be(typid))));
	opfamily = get_opclass_family(opclass);
	opcintype = get_opclass_input_type(opclass);
	opr = get_opfamily_member(opfamily, opcintype, opcintype, BTEqualStrategyNumber);
	if (!OidIsValid(opr))
		elog(ERROR, "no equality operator in the default btree operator class of type %s", format_type_be(typid));

	tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opr));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for operator %u", opr);
	oprform = (Form_pg_operator) GETSTRUCT(tup);
	result = psprintf("OPERATOR(%s.%s)",
					  quote_identifier(get_namespace_name(oprform->oprnamespace)),
					  NameStr(oprform->oprname));
	ReleaseSysCache(tup);
	return result;
}

/*
 * Prepare (and keep) "does this key exist in the parent" for the given parent key columns, whose
 * parameter types are argtypes[0 .. ncols-1] (the types of the paired columns, which
 * add_regular_foreign_key requires to be equal on both sides).
 */
static SPIPlanPtr
regular_fk_prepare_key_plan(const char *uk_schema_name, const char *uk_table_name,
							Datum *uk_col_datums, int ncols, Oid *argtypes)
{
	StringInfoData buf;
	SPIPlanPtr	plan;
	int			i;

	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT 1 FROM ONLY %s.%s AS uk WHERE ",
					 quote_identifier(uk_schema_name), quote_identifier(uk_table_name));
	for (i = 0; i < ncols; i++)
	{
		char	   *ukc = NameStr(*DatumGetName(uk_col_datums[i]));

		if (i > 0)
			appendStringInfoString(&buf, " AND ");
		appendStringInfo(&buf, "uk.%s %s $%d",
						 quote_identifier(ukc), regular_fk_equality_operator(argtypes[i]), i + 1);
	}
	appendStringInfoString(&buf, " LIMIT 1 FOR KEY SHARE OF uk");

	/* Planned as the referenced table's owner, who also runs it (regular_fk_table_owner). */
	plan = regular_fk_prepare_as(regular_fk_table_owner(uk_schema_name, uk_table_name), buf.data, ncols, argtypes);
	pfree(buf.data);
	return plan;
}

/*
 * Execute a prepared plan as query_owner (the owner of the table the query reads or changes, see
 * regular_fk_table_owner), with row level security not forced on its tables. Returns SPI_processed.
 *
 * As ri_PerformCheck: fire_triggers is false, so AFTER triggers of the ON DELETE CASCADE delete (the child's own
 * triggers, system versioning, its own foreign keys) are queued to the end of the outer query instead of firing
 * inside this trigger, and fire as their usual identity.
 *
 * detect_new_rows: as PostgreSQL's ri_PerformCheck does for the referenced side of a foreign key. In
 * REPEATABLE READ and SERIALIZABLE the transaction snapshot cannot see a referencing row committed
 * after it was taken, so the lookup runs with the latest snapshot as test snapshot and the transaction
 * snapshot as crosscheck snapshot (after CommandCounterIncrement, so this transaction's own changes are
 * included too). In READ COMMITTED each lookup takes a fresh snapshot anyway.
 */
static uint64
regular_fk_execute_as_ex(Oid query_owner, SPIPlanPtr plan, Datum *values, const char *nulls,
						 bool detect_new_rows, int expected_ret, long tcount)
{
	Oid			saved_userid;
	int			saved_sec_context;
	int			ret = 0;
	Snapshot	test_snapshot = InvalidSnapshot;
	Snapshot	crosscheck_snapshot = InvalidSnapshot;

	if (detect_new_rows && IsolationUsesXactSnapshot())
	{
		CommandCounterIncrement();
		test_snapshot = GetLatestSnapshot();
		crosscheck_snapshot = GetTransactionSnapshot();
	}

	GetUserIdAndSecContext(&saved_userid, &saved_sec_context);
	SetUserIdAndSecContext(query_owner,
						   saved_sec_context | SECURITY_LOCAL_USERID_CHANGE | SECURITY_NOFORCE_RLS);
	PG_TRY();
	{
		ret = SPI_execute_snapshot(plan, values, nulls, test_snapshot, crosscheck_snapshot,
								   false, false, tcount);
	}
	PG_FINALLY();
	{
		SetUserIdAndSecContext(saved_userid, saved_sec_context);
	}
	PG_END_TRY();

	if (ret != expected_ret)
		elog(ERROR, "SPI_execute_snapshot for regular foreign key %s returned %s",
			 expected_ret == SPI_OK_DELETE ? "cascade" : "lookup", SPI_result_code_string(ret));
	return SPI_processed;
}

static uint64
regular_fk_execute_as(Oid query_owner, SPIPlanPtr plan, Datum *values, const char *nulls, bool detect_new_rows)
{
	return regular_fk_execute_as_ex(query_owner, plan, values, nulls, detect_new_rows, SPI_OK_SELECT, 1);
}

/*
 * Before a regular foreign key trigger reads or changes anything with its owner's rights, check that it fires as
 * the trigger sql_saga created for a registered foreign key (1.4, review finding F1). Its lookups and the ON DELETE
 * CASCADE delete run as the extension owner from the table and column names in the trigger arguments, and any role
 * can create a trigger calling these functions on a table of its own. Without this check, made-up arguments read
 * (existence oracle) or deleted rows of tables the role has no rights on.
 *
 * Accepted only when sql_saga.foreign_keys has a regular_to_temporal row named by tgargs[0], the firing trigger is
 * not internal, is on the catalogued table (the parent for 'parent', the child for 'child'), has the catalogued
 * trigger name for its kind, and its arguments equal __internal_regular_fk_trigger_args() byte for byte. The column
 * names in the arguments are then those of the registered foreign key. Called after SPI_connect. Runs the catalog
 * query as the function owner, so it does not depend on the firing role's rights.
 */
typedef enum RegularFkTriggerKind
{
	REGULAR_FK_CHILD,			/* regular_fk_check_c on the child: fk_check_constraint */
	REGULAR_FK_PARENT_UPDATE,	/* uk_update_check_c on the parent: uk_update_trigger */
	REGULAR_FK_PARENT_DELETE,	/* uk_delete_check_c on the parent: uk_delete_trigger */
	REGULAR_FK_PARENT_TRUNCATE	/* uk_truncate_check_c on the parent: uk_truncate_trigger */
} RegularFkTriggerKind;

static SPIPlanPtr regular_fk_verify_plan = NULL;

static void
regular_fk_verify_trigger(FunctionCallInfo fcinfo, RegularFkTriggerKind kind)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Trigger    *trigger = trigdata->tg_trigger;
	Datum		values[4];
	char		nulls[4] = {' ', ' ', ' ', ' '};
	HeapTuple	proc_tuple;
	Oid			function_owner;
	Oid			saved_userid;
	int			saved_sec_context;
	int			ret = 0;
	bool		ok = false;
	static const char *const trigger_column[] = {
		"fk_check_constraint", "uk_update_trigger", "uk_delete_trigger", "uk_truncate_trigger"
	};

	if (trigger->tgisinternal)
		goto refuse;

	if (regular_fk_verify_plan == NULL)
	{
		/*
		 * $1 foreign key name, $2 firing table, $3 trigger name, $4 kind (0 child, 1 update, 2 delete, 3 truncate).
		 * The trigger is looked up by table and name, and its arguments compared with the canonical ones.
		 */
		const char *sql =
			"SELECT EXISTS ("
			" SELECT FROM sql_saga.foreign_keys AS fk"
			" JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name OPERATOR(pg_catalog.=) fk.unique_key_name"
			" JOIN pg_catalog.pg_trigger AS t ON t.tgrelid OPERATOR(pg_catalog.=) $2 AND t.tgname OPERATOR(pg_catalog.=) $3"
			" WHERE fk.foreign_key_name OPERATOR(pg_catalog.=) $1 AND fk.type OPERATOR(pg_catalog.=) 'regular_to_temporal'"
			"   AND NOT t.tgisinternal"
			"   AND $2 OPERATOR(pg_catalog.=) CASE WHEN $4 OPERATOR(pg_catalog.=) 0"
			"         THEN pg_catalog.to_regclass(pg_catalog.format('%I.%I', fk.table_schema, fk.table_name))"
			"         ELSE pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name)) END"
			"   AND $3 OPERATOR(pg_catalog.=) CASE $4 WHEN 0 THEN fk.fk_check_constraint WHEN 1 THEN fk.uk_update_trigger"
			"         WHEN 2 THEN fk.uk_delete_trigger ELSE fk.uk_truncate_trigger END"
			"   AND t.tgargs OPERATOR(pg_catalog.=) sql_saga.__internal_regular_fk_trigger_args(fk.foreign_key_name))";
		Oid			argtypes[4] = {NAMEOID, OIDOID, NAMEOID, INT4OID};

		regular_fk_verify_plan = SPI_prepare(sql, 4, argtypes);
		if (regular_fk_verify_plan == NULL)
			elog(ERROR, "SPI_prepare for regular foreign key trigger verification failed: %s",
				 SPI_result_code_string(SPI_result));
		if (SPI_keepplan(regular_fk_verify_plan))
			elog(ERROR, "SPI_keepplan for regular foreign key trigger verification failed");
	}

	if (trigger->tgnargs != 17)
		goto refuse;
	values[0] = DirectFunctionCall1(namein, CStringGetDatum(trigger->tgargs[0]));
	values[1] = ObjectIdGetDatum(RelationGetRelid(trigdata->tg_relation));
	values[2] = DirectFunctionCall1(namein, CStringGetDatum(trigger->tgname));
	values[3] = Int32GetDatum((int32) kind);

	proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fcinfo->flinfo->fn_oid));
	if (!HeapTupleIsValid(proc_tuple))
		elog(ERROR, "cache lookup failed for function %u", fcinfo->flinfo->fn_oid);
	function_owner = ((Form_pg_proc) GETSTRUCT(proc_tuple))->proowner;
	ReleaseSysCache(proc_tuple);

	GetUserIdAndSecContext(&saved_userid, &saved_sec_context);
	SetUserIdAndSecContext(function_owner,
						   saved_sec_context | SECURITY_LOCAL_USERID_CHANGE | SECURITY_NOFORCE_RLS);
	PG_TRY();
	{
		ret = SPI_execute_plan(regular_fk_verify_plan, values, nulls, true, 1);
		if (ret == SPI_OK_SELECT && SPI_processed == 1)
		{
			bool		isnull;
			Datum		d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);

			ok = !isnull && DatumGetBool(d);
		}
	}
	PG_FINALLY();
	{
		SetUserIdAndSecContext(saved_userid, saved_sec_context);
	}
	PG_END_TRY();
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "regular foreign key trigger verification returned %s", SPI_result_code_string(ret));
	if (ok)
		return;

refuse:
	ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			 errmsg("trigger \"%s\" on table \"%s\" is not a sql_saga foreign key trigger",
					trigger->tgname, RelationGetRelationName(trigdata->tg_relation)),
			 errdetail("%s may only fire as the %s trigger that sql_saga created for a registered regular foreign key, with its arguments.",
					   get_func_name(fcinfo->flinfo->fn_oid), trigger_column[kind])));
}

static void
regular_fk_parse_columns(const char *str, Datum **datums, int *count)
{
	Datum		array_datum;

	if (namearray_input_func_oid == InvalidOid)
	{
		int16		typlen;
		bool		typbyval;
		char		typalign;
		char		typdelim;

		get_type_io_data(NAMEARRAYOID, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim,
						 &namearray_ioparam_oid, &namearray_input_func_oid);
	}
	array_datum = OidInputFunctionCall(namearray_input_func_oid, (char *) str, namearray_ioparam_oid, -1);
	deconstruct_array(DatumGetArrayTypeP(array_datum), NAMEOID, NAMEDATALEN, false, 'c', datums, NULL, count);
}

/*
 * "Does a child row reference this key" for the referenced side of a regular foreign key, comparing
 * with the same equality operators as the referencing side.
 */
static char *
regular_fk_child_reference_query(const char *fk_schema_name, const char *fk_table_name,
								 Datum *fk_col_datums, int ncols, Oid *argtypes)
{
	StringInfoData buf;
	int			i;

	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT 1 FROM ONLY %s.%s AS fk WHERE ",
					 quote_identifier(fk_schema_name), quote_identifier(fk_table_name));
	for (i = 0; i < ncols; i++)
	{
		char	   *fkc = NameStr(*DatumGetName(fk_col_datums[i]));

		if (i > 0)
			appendStringInfoString(&buf, " AND ");
		appendStringInfo(&buf, "fk.%s %s $%d",
						 quote_identifier(fkc), regular_fk_equality_operator(argtypes[i]), i + 1);
	}
	appendStringInfoString(&buf, " LIMIT 1 FOR KEY SHARE OF fk");
	return buf.data;
}

/*
 * ON DELETE CASCADE for the referenced side of a regular foreign key: delete every child row that
 * references the key, comparing with the same equality operators as the referencing side. Uses the
 * foreign key's index on the child columns, like the reference lookup.
 */
static SPIPlanPtr
regular_fk_prepare_cascade_plan(const char *fk_schema_name, const char *fk_table_name,
								Datum *fk_col_datums, int ncols, Oid *argtypes)
{
	StringInfoData buf;
	SPIPlanPtr	plan;
	int			i;

	initStringInfo(&buf);
	appendStringInfo(&buf, "DELETE FROM ONLY %s.%s AS fk WHERE ",
					 quote_identifier(fk_schema_name), quote_identifier(fk_table_name));
	for (i = 0; i < ncols; i++)
	{
		char	   *fkc = NameStr(*DatumGetName(fk_col_datums[i]));

		if (i > 0)
			appendStringInfoString(&buf, " AND ");
		appendStringInfo(&buf, "fk.%s %s $%d",
						 quote_identifier(fkc), regular_fk_equality_operator(argtypes[i]), i + 1);
	}

	/* Planned and run as the referencing table's owner, as native ON DELETE CASCADE (ri_triggers.c). */
	plan = regular_fk_prepare_as(regular_fk_table_owner(fk_schema_name, fk_table_name), buf.data, ncols, argtypes);
	pfree(buf.data);
	return plan;
}

/*
 * The referenced side of a regular foreign key, after OLD's key went away (deleted, or changed by an
 * update): refuse when a child row references OLD's key and no parent row has that key any more.
 * values/nulls hold OLD's key columns.
 *
 * With ON DELETE CASCADE (cascade_plan not NULL) the child rows of the vanished key are deleted
 * instead of refusing, under the same rule: only when no parent row has the key any more. The delete
 * runs as the child table's owner, as native ON DELETE CASCADE does, so the parent's writer needs no rights on
 * the child and the child's triggers fire with no more rights than its owner has.
 */
static void
regular_fk_check_referenced_key(SPIPlanPtr reference_plan, SPIPlanPtr key_plan,
								SPIPlanPtr cascade_plan, Datum *values, const char *nulls, char **tgargs)
{
	/*
	 * As native referential integrity: the child is read (and the ON DELETE CASCADE delete runs) as the child
	 * table's owner, the parent is read as the parent table's owner. The parent's writer needs no rights on the
	 * child. Both lookups detect rows committed after the transaction snapshot (ri_PerformCheck's detectNewRows):
	 * a child row another transaction committed meanwhile, and parent rows deleted meanwhile.
	 */
	Oid			child_owner = regular_fk_table_owner(tgargs[1], tgargs[2]);
	Oid			parent_owner = regular_fk_table_owner(tgargs[7], tgargs[8]);

	if (regular_fk_execute_as(child_owner, reference_plan, values, nulls, true) == 0)
		return;

	if (regular_fk_execute_as(parent_owner, key_plan, values, nulls, true) > 0)
		return;

	if (cascade_plan != NULL)
	{
		regular_fk_execute_as_ex(child_owner, cascade_plan, values, nulls, true, SPI_OK_DELETE, 0);
		return;
	}

	ereport(ERROR,
			(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
			 errmsg("update or delete on table \"%s.%s\" violates foreign key constraint \"%s\" on table \"%s.%s\"",
					tgargs[7], tgargs[8], tgargs[0], tgargs[1], tgargs[2]),
			 errdetail("The key is still referenced from table \"%s.%s\" and no other row of \"%s.%s\" has it.",
					   tgargs[1], tgargs[2], tgargs[7], tgargs[8])));
}

/*
 * regular_fk_check_c -- the referencing side of a regular foreign key: AFTER INSERT OR UPDATE OF the
 * foreign key columns, FOR EACH ROW, DEFERRABLE INITIALLY IMMEDIATE.
 *
 * Like PostgreSQL's RI_FKey_check, a row version that is no longer live when the check runs (deleted,
 * or updated again, before a deferred check) is skipped: the live version is checked by its own event.
 * All NULL: accepted. Partly NULL: accepted for MATCH SIMPLE, refused for MATCH FULL. MATCH PARTIAL is
 * refused when the foreign key is created. Otherwise the key must exist in at least one parent row.
 */
Datum
regular_fk_check_c(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata;
	Relation	rel;
	TupleDesc	tupdesc;
	HeapTuple	new_row;
	TupleTableSlot *slot;
	char	  **tgargs;
	char	   *foreign_key_name;
	char	   *match_type;
	FkValidationPlan *plan_entry;
	bool		found;
	bool		has_nulls = false;
	bool		all_nulls = true;
	Datum		values[MAX_FK_COLS];
	char		nulls[MAX_FK_COLS];
	int			i;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "regular_fk_check_c: not called by trigger manager");

	trigdata = (TriggerData *) fcinfo->context;
	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) || !TRIGGER_FIRED_FOR_ROW(trigdata->tg_event)
		|| !(TRIGGER_FIRED_BY_INSERT(trigdata->tg_event) || TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event)))
		elog(ERROR, "regular_fk_check_c: must be fired AFTER INSERT OR UPDATE FOR EACH ROW");

	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;
	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		new_row = trigdata->tg_newtuple;
		slot = trigdata->tg_newslot;
	}
	else
	{
		new_row = trigdata->tg_trigtuple;
		slot = trigdata->tg_trigslot;
	}

	if (trigdata->tg_trigger->tgnargs != 17)
		elog(ERROR, "regular_fk_check_c: expected 17 arguments, got %d", trigdata->tg_trigger->tgnargs);
	tgargs = trigdata->tg_trigger->tgargs;
	foreign_key_name = tgargs[0];
	match_type = tgargs[13];

	/* A row version that is no longer live is not checked (its successor, if any, is). */
	if (!table_tuple_satisfies_snapshot(rel, slot, SnapshotSelf))
		return PointerGetDatum(NULL);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	regular_fk_verify_trigger(fcinfo, REGULAR_FK_CHILD);

	init_fk_plan_cache();
	plan_entry = (FkValidationPlan *) hash_search(fk_plan_cache, &(trigdata->tg_trigger->tgoid), HASH_ENTER, &found);
	if (!plan_cache_entry_usable(found, &plan_entry->plan, &plan_entry->key_plan, &plan_entry->cascade_plan))
	{
		Datum	   *uk_col_datums;
		Datum	   *fk_col_datums;
		int			num_uk_cols;
		int			num_fk_cols;

		regular_fk_parse_columns(tgargs[9], &uk_col_datums, &num_uk_cols);
		regular_fk_parse_columns(tgargs[3], &fk_col_datums, &num_fk_cols);
		if (num_fk_cols != num_uk_cols || num_fk_cols < 1 || num_fk_cols > MAX_FK_COLS)
			elog(ERROR, "regular_fk_check_c: foreign key \"%s\" pairs %d child columns with %d referenced columns",
				 foreign_key_name, num_fk_cols, num_uk_cols);
		plan_entry->nargs = num_fk_cols;
		for (i = 0; i < num_fk_cols; i++)
		{
			char	   *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
			int			attnum = SPI_fnumber(tupdesc, fkc);

			if (attnum <= 0)
				elog(ERROR, "column \"%s\" does not exist in table \"%s\"", fkc, RelationGetRelationName(rel));
			plan_entry->param_attnums[i] = attnum;
			plan_entry->argtypes[i] = SPI_gettypeid(tupdesc, attnum);
		}
		plan_entry->plan = regular_fk_prepare_key_plan(tgargs[7], tgargs[8], uk_col_datums, num_uk_cols,
													   plan_entry->argtypes);
	}

	for (i = 0; i < plan_entry->nargs; i++)
	{
		bool		isnull;

		values[i] = heap_getattr(new_row, plan_entry->param_attnums[i], tupdesc, &isnull);
		nulls[i] = isnull ? 'n' : ' ';
		if (isnull)
			has_nulls = true;
		else
			all_nulls = false;
	}

	if (all_nulls || (has_nulls && strcmp(match_type, "SIMPLE") == 0))
	{
		SPI_finish();
		return PointerGetDatum(NULL);
	}
	if (has_nulls)
	{
		if (strcmp(match_type, "FULL") == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
					 errmsg("insert or update on table \"%s.%s\" violates foreign key constraint \"%s\"",
							tgargs[1], tgargs[2], foreign_key_name),
					 errdetail("MATCH FULL does not allow mixing of null and nonnull key values.")));
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("MATCH PARTIAL is not supported for foreign key \"%s\"", foreign_key_name)));
	}

	/* The key is looked up in the referenced table as that table's owner, as native RI_FKey_check does. */
	if (regular_fk_execute_as(regular_fk_table_owner(tgargs[7], tgargs[8]), plan_entry->plan, values, nulls, false) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
				 errmsg("insert or update on table \"%s.%s\" violates foreign key constraint \"%s\"",
						tgargs[1], tgargs[2], foreign_key_name),
				 errdetail("The referenced key is not present in table \"%s.%s\".", tgargs[7], tgargs[8])));

	SPI_finish();
	return PointerGetDatum(NULL);
}

/*
 * fk_check_c -- shared implementation for fk_insert_check_c / fk_update_check_c.
 *
 * The only difference between INSERT and UPDATE is which tuple to validate:
 *   INSERT: tg_trigtuple  (the newly inserted row)
 *   UPDATE: tg_newtuple   (the post-update row)
 */
static Datum
fk_check_c(FunctionCallInfo fcinfo, bool is_update)
{
	const char *funcname = is_update ? "fk_update_check_c" : "fk_insert_check_c";
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
		elog(ERROR, "%s: not called by trigger manager", funcname);

	trigdata = (TriggerData *) fcinfo->context;
	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;

	if (is_update)
	{
		rettuple = trigdata->tg_newtuple;
		new_row = trigdata->tg_newtuple;
	}
	else
	{
		rettuple = trigdata->tg_trigtuple;
		new_row = trigdata->tg_trigtuple;
	}

	if (trigdata->tg_trigger->tgnargs != 16)
		elog(ERROR, "%s: expected 16 arguments, got %d", funcname, trigdata->tg_trigger->tgnargs);

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

	if (!plan_cache_entry_usable(found, &plan_entry->plan, &plan_entry->key_plan, &plan_entry->cascade_plan))
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
fk_insert_check_c(PG_FUNCTION_ARGS)
{
	return fk_check_c(fcinfo, false);
}

Datum
fk_update_check_c(PG_FUNCTION_ARGS)
{
	return fk_check_c(fcinfo, true);
}

/*
 * uk_truncate_check_c -- the referenced side of a regular foreign key on TRUNCATE (1.4): AFTER TRUNCATE FOR EACH
 * STATEMENT on the parent, with the same 17 arguments as the other regular foreign key triggers.
 *
 * TRUNCATE fires no row triggers, so the uk_delete check never sees it. PostgreSQL refuses TRUNCATE of a table
 * that a native foreign key references unless the referencing table is truncated in the same statement, whatever
 * the foreign key's delete action. This does the same for a regular foreign key: an AFTER TRUNCATE trigger runs
 * once every table of the statement is truncated, so the child is empty here exactly when it was truncated too
 * (or had no reference). A child row references the parent when its key columns are all non-NULL (MATCH SIMPLE)
 * or any of them is (MATCH FULL), as in regular_fk_check_c. The error has the native message, DETAIL and HINT,
 * and rolls the TRUNCATE back.
 */
Datum
uk_truncate_check_c(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata;
	char	  **tgargs;
	Datum	   *fk_col_datums;
	int			num_fk_cols;
	StringInfoData buf;
	SPIPlanPtr	plan;
	int			i;
	bool		match_full;
	uint64		referencing;
	Oid			child_owner;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "uk_truncate_check_c: not called by trigger manager");
	trigdata = (TriggerData *) fcinfo->context;
	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) || !TRIGGER_FIRED_BY_TRUNCATE(trigdata->tg_event))
		elog(ERROR, "uk_truncate_check_c: must be fired AFTER TRUNCATE FOR EACH STATEMENT");
	if (trigdata->tg_trigger->tgnargs != 17)
		elog(ERROR, "uk_truncate_check_c: expected 17 arguments, got %d", trigdata->tg_trigger->tgnargs);
	tgargs = trigdata->tg_trigger->tgargs;
	match_full = strcmp(tgargs[13], "FULL") == 0;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");
	regular_fk_verify_trigger(fcinfo, REGULAR_FK_PARENT_TRUNCATE);

	regular_fk_parse_columns(tgargs[3], &fk_col_datums, &num_fk_cols);
	if (num_fk_cols < 1 || num_fk_cols > MAX_FK_COLS)
		elog(ERROR, "uk_truncate_check_c: foreign key \"%s\" has %d child columns", tgargs[0], num_fk_cols);

	/* SELECT 1 FROM ONLY child AS fk WHERE (fk.c1 IS NOT NULL AND|OR ...) LIMIT 1 */
	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT 1 FROM ONLY %s.%s AS fk WHERE ",
					 quote_identifier(tgargs[1]), quote_identifier(tgargs[2]));
	for (i = 0; i < num_fk_cols; i++)
	{
		if (i > 0)
			appendStringInfoString(&buf, match_full ? " OR " : " AND ");
		appendStringInfo(&buf, "fk.%s IS NOT NULL", quote_identifier(NameStr(*DatumGetName(fk_col_datums[i]))));
	}
	appendStringInfoString(&buf, " LIMIT 1");

	/* Not cached: TRUNCATE is rare. Planned and run as the referencing table's owner, as native RI. */
	child_owner = regular_fk_table_owner(tgargs[1], tgargs[2]);
	plan = regular_fk_prepare_as(child_owner, buf.data, 0, NULL);
	referencing = regular_fk_execute_as(child_owner, plan, NULL, NULL, true);
	SPI_freeplan(plan);
	pfree(buf.data);
	SPI_finish();

	if (referencing > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot truncate a table referenced in a foreign key constraint"),
				 errdetail("Table \"%s\" references \"%s\".", tgargs[2], tgargs[8]),
				 errhint("Truncate table \"%s\" at the same time.", tgargs[2])));

	return PointerGetDatum(NULL);
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
	/* Only the trigger sql_saga created for a registered regular foreign key (temporal FKs are native since 1.3). */
	regular_fk_verify_trigger(fcinfo, REGULAR_FK_PARENT_DELETE);

	init_uk_delete_plan_cache();
	plan_entry = (FkValidationPlan *) hash_search(uk_delete_plan_cache, &(trigdata->tg_trigger->tgoid), HASH_ENTER, &found);

	if (!plan_cache_entry_usable(found, &plan_entry->plan, &plan_entry->key_plan, &plan_entry->cascade_plan))
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

			/* A child row referencing the old key ... */
			query = regular_fk_child_reference_query(fk_schema_name, fk_table_name, fk_col_datums,
													 num_uk_cols, plan_entry->argtypes);
			/* ... is a violation only when no parent row has the key any more. */
			plan_entry->key_plan = regular_fk_prepare_key_plan(uk_schema_name, uk_table_name, uk_col_datums,
															   num_uk_cols, plan_entry->argtypes);
			/* tgargs[15] is delete_action: with CASCADE the vanished key's children are deleted instead. */
			plan_entry->cascade_plan = strcmp(tgargs[15], "CASCADE") == 0
				? regular_fk_prepare_cascade_plan(fk_schema_name, fk_table_name, fk_col_datums,
												  num_uk_cols, plan_entry->argtypes)
				: NULL;
		}
		
		if (strcmp(fk_type, "temporal_to_temporal") != 0)
			/* The child reference lookup, planned as the referencing table's owner who runs it. */
			plan_entry->plan = regular_fk_prepare_as(regular_fk_table_owner(fk_schema_name, fk_table_name),
													 query, plan_entry->nargs, plan_entry->argtypes);
		else
		{
			plan_entry->plan = SPI_prepare(query, plan_entry->nargs, plan_entry->argtypes);
			if (!plan_entry->plan || SPI_keepplan(plan_entry->plan))
				elog(ERROR, "SPI_prepare/keepplan for validation query failed");
		}

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

		if (strcmp(fk_type, "temporal_to_temporal") != 0)
		{
			regular_fk_check_referenced_key(plan_entry->plan, plan_entry->key_plan, plan_entry->cascade_plan,
											values, nulls, tgargs);
			SPI_finish();
			return PointerGetDatum(rettuple);
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
	/* Only the trigger sql_saga created for a registered regular foreign key (temporal FKs are native since 1.3). */
	regular_fk_verify_trigger(fcinfo, REGULAR_FK_PARENT_UPDATE);

	init_uk_update_plan_cache();
	plan_entry = (UkUpdateValidationPlan *) hash_search(uk_update_plan_cache, &(trigdata->tg_trigger->tgoid), HASH_ENTER, &found);

	if (!plan_cache_entry_usable(found, &plan_entry->plan, &plan_entry->key_plan, NULL))
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
			query = regular_fk_child_reference_query(fk_schema_name, fk_table_name, fk_col_datums,
													 num_uk_cols, plan_entry->argtypes);
			plan_entry->key_plan = regular_fk_prepare_key_plan(uk_schema_name, uk_table_name, uk_col_datums,
															   num_uk_cols, plan_entry->argtypes);
		}

		if (strcmp(fk_type, "temporal_to_temporal") != 0)
			/* The child reference lookup, planned as the referencing table's owner who runs it. */
			plan_entry->plan = regular_fk_prepare_as(regular_fk_table_owner(fk_schema_name, fk_table_name),
													 query, plan_entry->nargs, plan_entry->argtypes);
		else
		{
			plan_entry->plan = SPI_prepare(query, plan_entry->nargs, plan_entry->argtypes);
			if (!plan_entry->plan || SPI_keepplan(plan_entry->plan))
				elog(ERROR, "SPI_prepare/keepplan for validation query failed");
		}
		
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
					(!old_isnull && !datumIsEqual(old_val, new_val, TupleDescAttr(tupdesc, plan_entry->param_attnums_old[i] - 1)->attbyval, TupleDescAttr(tupdesc, plan_entry->param_attnums_old[i] - 1)->attlen)))
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
					(!old_isnull && !datumIsEqual(old_val, new_val, TupleDescAttr(tupdesc, plan_entry->param_attnums_old[i]-1)->attbyval, TupleDescAttr(tupdesc, plan_entry->param_attnums_old[i]-1)->attlen)))
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
			/* ON UPDATE CASCADE is not supported: an update of the referenced key is checked, never cascaded. */
			regular_fk_check_referenced_key(plan_entry->plan, plan_entry->key_plan, NULL, values, nulls, tgargs);
			SPI_finish();
			return PointerGetDatum(rettuple);
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
	Datum			values[1];
	bool			nulls[1];
	int				columns[1];
	char		   *range_column_name;
	int16			range_num;
	Oid				range_type_oid;
	TypeCacheEntry *range_cache;
	RangeBound		lower, upper;

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

	if (trigdata->tg_trigger->tgnargs != 1)
		elog(ERROR, "%s: expected 1 argument (range_column_name), got %d",
			 funcname, trigdata->tg_trigger->tgnargs);
	range_column_name = trigdata->tg_trigger->tgargs[0];

	rel = trigdata->tg_relation;
	new_tupdesc = RelationGetDescr(rel);

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		new_row = trigdata->tg_trigtuple;
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		HeapTuple old_row = trigdata->tg_trigtuple;
		new_row = trigdata->tg_newtuple;
		if (OnlyExcludedColumnsChanged(rel, old_row, new_row))
			return PointerGetDatum(new_row);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" must be fired for INSERT or UPDATE",
						funcname)));
		new_row = NULL;
	}

	range_num = SPI_fnumber(new_tupdesc, range_column_name);
	if (range_num == SPI_ERROR_NOATTRIBUTE)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
						errmsg("column \"%s\" does not exist in table \"%s\"",
							   range_column_name, RelationGetRelationName(rel))));

	range_type_oid = SPI_gettypeid(new_tupdesc, range_num);
	if (range_type_oid != TSTZRANGEOID)
		elog(ERROR, "system time range column must be of type tstzrange");

	range_cache = lookup_type_cache(range_type_oid, TYPECACHE_RANGE_INFO);
	if (range_cache->rngelemtype->type_id != TIMESTAMPTZOID)
		elog(ERROR, "system time range column must be of subtype timestamptz");

	lower.val = TRANSACTION_TSTZ;
	lower.infinite = false;
	lower.inclusive = true;
	lower.lower = true;

	upper.val = (Datum) 0;
	upper.infinite = true;
	upper.inclusive = false;
	upper.lower = false;

	columns[0] = range_num;
	values[0] = PointerGetDatum(range_serialize(range_cache, &lower, &upper, false, NULL));
	nulls[0] = false;

	new_row = heap_modify_tuple_by_cols(new_row, new_tupdesc, 1, columns, values, nulls);

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
	char		   *range_column_name;
	int16			range_num;
	Oid				range_type_oid;
	TypeCacheEntry *range_cache;
	bool			is_null;
	Oid				history_id;
	int				cmp;
	bool			only_excluded_changed = false;
	RangeType	   *range;
	RangeBound		lower, upper;

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

	if (trigdata->tg_trigger->tgnargs != 1)
		elog(ERROR, "%s: expected 1 argument (range_column_name), got %d",
			 funcname, trigdata->tg_trigger->tgnargs);
	range_column_name = trigdata->tg_trigger->tgargs[0];

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
				 errmsg("function \"%s\" must be fired for INSERT, UPDATE or DELETE",
						funcname)));
		old_row = NULL;
		new_row = NULL;
	}

	range_num = SPI_fnumber(tupledesc, range_column_name);
	if (range_num == SPI_ERROR_NOATTRIBUTE)
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
						errmsg("column \"%s\" does not exist in table \"%s\"",
							   range_column_name, RelationGetRelationName(rel))));
	range_type_oid = SPI_gettypeid(tupledesc, range_num);
	range_cache = lookup_type_cache(range_type_oid, TYPECACHE_RANGE_INFO);

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event) ||
		(TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) && !only_excluded_changed))
	{
		Datum range_datum = SPI_getbinval(new_row, tupledesc, range_num, &is_null);

		range = DatumGetRangeTypeP(range_datum);
		range_deserialize(range_cache, range, &lower, &upper, &is_null);

		if (lower.infinite ||
			DatumGetInt32(DirectFunctionCall2(timestamp_cmp, lower.val, TRANSACTION_TSTZ)) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_GENERATED_ALWAYS),
					 errmsg("cannot insert or update column \"%s\"", range_column_name),
					 errdetail("Column \"%s\" is GENERATED ALWAYS AS ROW START", range_column_name)));

		if (!upper.infinite)
			ereport(ERROR,
					(errcode(ERRCODE_GENERATED_ALWAYS),
					 errmsg("cannot insert or update column \"%s\"", range_column_name),
					 errdetail("Column \"%s\" is GENERATED ALWAYS AS ROW END", range_column_name)));

		if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
			return PointerGetDatum(NULL);
	}

	if (only_excluded_changed)
		return PointerGetDatum(NULL);

	range = DatumGetRangeTypeP(SPI_getbinval(old_row, tupledesc, range_num, &is_null));
	range_deserialize(range_cache, range, &lower, &upper, &is_null);
	cmp = DatumGetInt32(DirectFunctionCall2(timestamp_cmp, lower.val, TRANSACTION_TSTZ));

	if (cmp == 0)
		return PointerGetDatum(NULL);

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
		int16		history_range_num;
		RangeBound	hist_lower, hist_upper;

		history_rel = table_open(history_id, RowExclusiveLock);
		history_tupledesc = RelationGetDescr(history_rel);
		history_range_num = SPI_fnumber(history_tupledesc, range_column_name);
		if (history_range_num == SPI_ERROR_NOATTRIBUTE)
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
							errmsg("column \"%s\" does not exist in history table \"%s\"",
								   range_column_name, RelationGetRelationName(history_rel))));

		/*
		 * Columns correspond by name (tmp/design_manual_schema_changes.md). Every base column
		 * must be in the history with the same type, typmod and collation; otherwise fail before
		 * writing anything, never leaving a base value out. History-only columns get NULL.
		 */
		CheckHistoryTupleCompatibility(rel, history_rel);

		hist_lower = lower;
		hist_upper.val = TRANSACTION_TSTZ;
		hist_upper.infinite = false;
		hist_upper.inclusive = false;
		hist_upper.lower = false;

		history_tuple = ProjectBaseTupleToHistoryByName(old_row, tupledesc, history_tupledesc,
			history_range_num,
			PointerGetDatum(range_serialize(range_cache, &hist_lower, &hist_upper, false, NULL)));

		insert_into_history(history_rel, history_tuple);

		table_close(history_rel, NoLock);
	}

	return PointerGetDatum(NULL);
}


void _PG_init(void) {
}

/* ------------------------------------------------------------------------------------------------------------
 * Current-view clock binding.
 *
 * A current view is "SELECT * FROM base WHERE <range> @> <clock>". PostgreSQL resolved <clock> (CURRENT_DATE,
 * now() or a custom function given to add_current_view) when the view was created and keeps that exact binding
 * in the view's rule. sql_saga.__internal_current_view_clock returns that bound expression as SQL text,
 * deparsed by PostgreSQL. It is called with search_path = pg_catalog, pg_temp, so every function, operator and
 * cast outside pg_catalog is printed schema-qualified: re-parsing the text under that same path binds the same
 * objects. Used by view rebuilds (src/2o) and by current_view_trigger (src/3c), never re-resolving the
 * registered name text on any other search_path. No OID is stored anywhere.
 * ------------------------------------------------------------------------------------------------------------ */
PG_FUNCTION_INFO_V1(current_view_clock);

Datum
current_view_clock(PG_FUNCTION_ARGS)
{
	Oid			view_oid = PG_GETARG_OID(0);
	Relation	rel;
	Query	   *query;
	Node	   *quals;
	OpExpr	   *op;
	char	   *op_name;
	char	   *clock;

	rel = try_relation_open(view_oid, AccessShareLock);
	if (rel == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("sql_saga: current view with OID %u does not exist", view_oid)));
	if (rel->rd_rel->relkind != RELKIND_VIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("sql_saga: \"%s\" is not a view", RelationGetRelationName(rel))));

	query = get_view_query(rel);
	quals = query->jointree != NULL ? query->jointree->quals : NULL;
	op = (quals != NULL && IsA(quals, OpExpr)) ? (OpExpr *) quals : NULL;
	op_name = op != NULL ? get_opname(op->opno) : NULL;
	if (op == NULL || op_name == NULL || strcmp(op_name, "@>") != 0
		|| get_func_namespace(op->opfuncid) != PG_CATALOG_NAMESPACE
		|| list_length(op->args) != 2 || !IsA(linitial(op->args), Var))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("sql_saga: view \"%s\" is not a current view of the form <range> @> <clock>",
						RelationGetRelationName(rel))));

	clock = deparse_expression((Node *) lsecond(op->args), NIL, false, false);
	relation_close(rel, AccessShareLock);
	PG_RETURN_TEXT_P(cstring_to_text(clock));
}

/*
 * __internal_require_owner(table_oid regclass, purpose text) -- refuse unless the role that called sql_saga's API
 * owns the table (1.4, review finding F3).
 *
 * sql_saga's API functions are SECURITY DEFINER, so inside them current_user is the extension owner. The role that
 * called them is the outer user (session user, or the role chosen by SET ROLE), which SQL code cannot change from
 * inside a SECURITY DEFINER function. PostgreSQL requires ownership for ALTER TABLE, CREATE TRIGGER and constraints;
 * this is the same rule for the changes sql_saga makes on the caller's behalf: the outer user must be a member of the
 * table owner's role (has_privs_of_role, as pg_has_role(..., 'USAGE')). Superusers pass, as natively.
 */
PG_FUNCTION_INFO_V1(require_owner);
Datum
require_owner(PG_FUNCTION_ARGS)
{
	Oid			relid;
	Oid			caller = GetOuterUserId();
	HeapTuple	tup;
	Form_pg_class classform;
	char	   *relname;
	Oid			owner;

	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("no table name specified")));
	relid = PG_GETARG_OID(0);
	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tup))
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("relation with OID %u does not exist", relid)));
	classform = (Form_pg_class) GETSTRUCT(tup);
	owner = classform->relowner;
	relname = pstrdup(NameStr(classform->relname));
	ReleaseSysCache(tup);

	if (!has_privs_of_role(caller, owner))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be owner of table %s", relname),
				 PG_ARGISNULL(1) ? 0 : errdetail("%s changes the table, which needs the rights of its owner.",
												 text_to_cstring(PG_GETARG_TEXT_PP(1)))));
	PG_RETURN_VOID();
}

/*
 * __internal_require_references(table_oid regclass) -- refuse unless the role that called sql_saga's API has
 * REFERENCES on the table, as ALTER TABLE ... ADD FOREIGN KEY requires for the referenced table.
 */
PG_FUNCTION_INFO_V1(require_references);
Datum
require_references(PG_FUNCTION_ARGS)
{
	Oid			relid;

	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("no table name specified")));
	relid = PG_GETARG_OID(0);
	if (pg_class_aclcheck(relid, GetOuterUserId(), ACL_REFERENCES) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for table %s", get_rel_name(relid)),
				 errdetail("A foreign key needs the REFERENCES privilege on the referenced table.")));
	PG_RETURN_VOID();
}
