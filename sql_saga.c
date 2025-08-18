/**
 * sql_saga.c -
 * TODO:
 * Install a hook so we can get called with a table/column is dropped/renamed,
 * so that we can drop/update our constraints as necessary.
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
	char *fk_start_after_column_name;
	char *fk_stop_on_column_name;
	char *uk_table_oid_str;
	char *uk_schema_name;
	char *uk_table_name;
	char *uk_column_names_str;
	char *uk_era_name;
	char *uk_start_after_column_name;
	char *uk_stop_on_column_name;
	char *match_type;

	/* For get_type_io_data */
	Oid			typinput_func_oid;
	Oid			typioparam_oid;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char		typdelim;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "fk_insert_check_c: not called by trigger manager");

	trigdata = (TriggerData *) fcinfo->context;
	rettuple = trigdata->tg_trigtuple;
	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;
	new_row = trigdata->tg_trigtuple;

	if (trigdata->tg_trigger->tgnargs != 18)
		elog(ERROR, "fk_insert_check_c: expected 18 arguments, got %d", trigdata->tg_trigger->tgnargs);

	tgargs = trigdata->tg_trigger->tgargs;

	foreign_key_name = tgargs[0];
	fk_column_names_str = tgargs[4];
	fk_start_after_column_name = tgargs[6];
	fk_stop_on_column_name = tgargs[7];
	uk_table_oid_str = tgargs[8];
	uk_schema_name = tgargs[9];
	uk_table_name = tgargs[10];
	uk_column_names_str = tgargs[11];
	uk_era_name = tgargs[12];
	uk_start_after_column_name = tgargs[13];
	uk_stop_on_column_name = tgargs[14];
	match_type = tgargs[15];

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* Check for NULLs in FK columns */
	{
		Datum		fk_column_names_datum;
		ArrayType  *fk_column_names_array;
		int			num_fk_cols;
		Datum	   *fk_col_datums;
		Oid			fk_col_elem_type;
		int16		fk_col_elem_len;
		bool		fk_col_elem_byval;
		char		fk_col_elem_align;
		bool		has_nulls = false;
		bool		all_nulls = true;
		int i;

		get_type_io_data(NAMEARRAYOID, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &typioparam_oid, &typinput_func_oid);
		fk_column_names_datum = OidInputFunctionCall(typinput_func_oid, fk_column_names_str, typioparam_oid, -1);
		fk_column_names_array = DatumGetArrayTypeP(fk_column_names_datum);
		fk_col_elem_type = ARR_ELEMTYPE(fk_column_names_array);

		get_typlenbyvalalign(fk_col_elem_type, &fk_col_elem_len, &fk_col_elem_byval, &fk_col_elem_align);
		deconstruct_array(fk_column_names_array, fk_col_elem_type, fk_col_elem_len, fk_col_elem_byval, fk_col_elem_align, &fk_col_datums, NULL, &num_fk_cols);

		for (i = 0; i < num_fk_cols; i++)
		{
			char *col_name = NameStr(*DatumGetName(fk_col_datums[i]));
			int attnum = SPI_fnumber(tupdesc, col_name);
			bool isnull;

			if (attnum <= 0)
				elog(ERROR, "column \"%s\" does not exist in table \"%s\"", col_name, RelationGetRelationName(rel));

			(void) heap_getattr(new_row, attnum, tupdesc, &isnull);
			if (isnull)
				has_nulls = true;
			else
				all_nulls = false;
		}

		pfree(DatumGetPointer(fk_column_names_datum));
		if (num_fk_cols > 0)
			pfree(fk_col_datums);

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
			{
				ereport(ERROR, (errmsg("MATCH PARTIAL is not implemented")));
			}
			else if (strcmp(match_type, "FULL") == 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
						 errmsg("insert or update on table \"%s\" violates foreign key constraint \"%s\" (MATCH FULL with NULLs)",
								RelationGetRelationName(rel), foreign_key_name)));
			}
		}
	}

	/* Build and execute validation query */
	{
		char *fk_range_constructor;
		char *uk_range_constructor;
		char *q, *query;
		int ret;
		bool isnull, okay;
		char *uk_where_clause;
		Oid argtypes[] = { REGCLASSOID, NAMEOID };
		Datum values[2];
		StringInfoData where_buf;
		Datum uk_column_names_datum;
		ArrayType *uk_column_names_array;
		int num_uk_cols;
		Datum *uk_col_datums;
		Datum fk_column_names_datum;
		ArrayType *fk_column_names_array;
		int num_fk_cols;
		Datum *fk_col_datums;
		int i;
		char *fk_start_val_str;
		char *fk_end_val_str;
		char *quoted_start;
		char *quoted_end;

		/* Get range constructor types from sql_saga.era */
		q = "SELECT range_type::regtype::text FROM sql_saga.era WHERE table_oid = $1 AND era_name = $2";
		
		values[0] = ObjectIdGetDatum(RelationGetRelid(rel));
		values[1] = CStringGetDatum(tgargs[5]);
		
		ret = SPI_execute_with_args(q, 2, argtypes, values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for foreign key table %s era %s", RelationGetRelationName(rel), tgargs[5]);
		fk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		values[0] = DirectFunctionCall1(regclassin, CStringGetDatum(uk_table_oid_str));
		values[1] = CStringGetDatum(uk_era_name);
		ret = SPI_execute_with_args(q, 2, argtypes, values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for unique key table %s era %s", uk_table_oid_str, uk_era_name);
		uk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		/* Build uk_where_clause */
		initStringInfo(&where_buf);

		get_type_io_data(NAMEARRAYOID, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &typioparam_oid, &typinput_func_oid);
		uk_column_names_datum = OidInputFunctionCall(typinput_func_oid, uk_column_names_str, typioparam_oid, -1);

		uk_column_names_array = DatumGetArrayTypeP(uk_column_names_datum);
		deconstruct_array(uk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &uk_col_datums, NULL, &num_uk_cols);

		fk_column_names_datum = OidInputFunctionCall(typinput_func_oid, fk_column_names_str, typioparam_oid, -1);
		fk_column_names_array = DatumGetArrayTypeP(fk_column_names_datum);
		deconstruct_array(fk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &fk_col_datums, NULL, &num_fk_cols);

		for (i = 0; i < num_uk_cols; i++)
		{
			char *ukc = NameStr(*DatumGetName(uk_col_datums[i]));
			char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
			int attnum = SPI_fnumber(tupdesc, fkc);
			char *val_str;
			char *quoted_val;

			if (attnum <= 0)
				elog(ERROR, "column \"%s\" does not exist in table \"%s\"", fkc, RelationGetRelationName(rel));

			val_str = SPI_getvalue(new_row, tupdesc, attnum);

			if (i > 0)
				appendStringInfoString(&where_buf, " AND ");
			
			quoted_val = quote_literal_cstr(val_str);
			appendStringInfo(&where_buf, "uk.%s = %s", quote_identifier(ukc), quoted_val);
			pfree(quoted_val);

			if (val_str) pfree(val_str);
		}
		uk_where_clause = where_buf.data;

		pfree(DatumGetPointer(uk_column_names_datum));
		if (num_uk_cols > 0) pfree(uk_col_datums);
		pfree(DatumGetPointer(fk_column_names_datum));
		if (num_fk_cols > 0) pfree(fk_col_datums);

		/* Get values for fk range */
		fk_start_val_str = SPI_getvalue(new_row, tupdesc, SPI_fnumber(tupdesc, fk_start_after_column_name));
		fk_end_val_str = SPI_getvalue(new_row, tupdesc, SPI_fnumber(tupdesc, fk_stop_on_column_name));
		quoted_start = quote_literal_cstr(fk_start_val_str);
		quoted_end = quote_literal_cstr(fk_end_val_str);

		query = psprintf(
			"SELECT COALESCE(("
			"  SELECT sql_saga.covers_without_gaps("
			"    %s(uk.%s, uk.%s, '(]'),"
			"    %s(%s, %s, '(]')"
			"    ORDER BY uk.%s"
			"  )"
			"  FROM %s.%s AS uk"
			"  WHERE %s"
			"), false)",
			uk_range_constructor,
			quote_identifier(uk_start_after_column_name),
			quote_identifier(uk_stop_on_column_name),
			fk_range_constructor,
			quoted_start,
			quoted_end,
			quote_identifier(uk_start_after_column_name),
			quote_identifier(uk_schema_name),
			quote_identifier(uk_table_name),
			uk_where_clause
		);
		pfree(quoted_start);
		pfree(quoted_end);

		ret = SPI_execute(query, true, 1);
		if (ret != SPI_OK_SELECT)
			elog(ERROR, "SPI_execute failed: %s", query);
		
		if (SPI_processed > 0)
		{
			okay = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
			if (isnull)
				okay = false;
		}
		else
		{
			okay = false;
		}

		pfree(query);
		pfree(uk_where_clause);
		if(fk_range_constructor) pfree(fk_range_constructor);
		if(uk_range_constructor) pfree(uk_range_constructor);
		if(fk_start_val_str) pfree(fk_start_val_str);
		if(fk_end_val_str) pfree(fk_end_val_str);

		if (!okay)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
					 errmsg("insert or update on table \"%s.%s\" violates foreign key constraint \"%s\"",
							tgargs[2], tgargs[3], foreign_key_name)));
		}
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
	char *fk_start_after_column_name;
	char *fk_stop_on_column_name;
	char *uk_table_oid_str;
	char *uk_schema_name;
	char *uk_table_name;
	char *uk_column_names_str;
	char *uk_era_name;
	char *uk_start_after_column_name;
	char *uk_stop_on_column_name;
	char *match_type;

	/* For get_type_io_data */
	Oid			typinput_func_oid;
	Oid			typioparam_oid;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char		typdelim;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "fk_update_check_c: not called by trigger manager");

	trigdata = (TriggerData *) fcinfo->context;
	rettuple = trigdata->tg_newtuple;
	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;
	new_row = trigdata->tg_newtuple;

	if (trigdata->tg_trigger->tgnargs != 18)
		elog(ERROR, "fk_update_check_c: expected 18 arguments, got %d", trigdata->tg_trigger->tgnargs);

	tgargs = trigdata->tg_trigger->tgargs;

	foreign_key_name = tgargs[0];
	fk_column_names_str = tgargs[4];
	fk_start_after_column_name = tgargs[6];
	fk_stop_on_column_name = tgargs[7];
	uk_table_oid_str = tgargs[8];
	uk_schema_name = tgargs[9];
	uk_table_name = tgargs[10];
	uk_column_names_str = tgargs[11];
	uk_era_name = tgargs[12];
	uk_start_after_column_name = tgargs[13];
	uk_stop_on_column_name = tgargs[14];
	match_type = tgargs[15];

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* Check for NULLs in FK columns */
	{
		Datum		fk_column_names_datum;
		ArrayType  *fk_column_names_array;
		int			num_fk_cols;
		Datum	   *fk_col_datums;
		Oid			fk_col_elem_type;
		int16		fk_col_elem_len;
		bool		fk_col_elem_byval;
		char		fk_col_elem_align;
		bool		has_nulls = false;
		bool		all_nulls = true;
		int i;

		get_type_io_data(NAMEARRAYOID, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &typioparam_oid, &typinput_func_oid);
		fk_column_names_datum = OidInputFunctionCall(typinput_func_oid, fk_column_names_str, typioparam_oid, -1);
		fk_column_names_array = DatumGetArrayTypeP(fk_column_names_datum);
		fk_col_elem_type = ARR_ELEMTYPE(fk_column_names_array);

		get_typlenbyvalalign(fk_col_elem_type, &fk_col_elem_len, &fk_col_elem_byval, &fk_col_elem_align);
		deconstruct_array(fk_column_names_array, fk_col_elem_type, fk_col_elem_len, fk_col_elem_byval, fk_col_elem_align, &fk_col_datums, NULL, &num_fk_cols);

		for (i = 0; i < num_fk_cols; i++)
		{
			char *col_name = NameStr(*DatumGetName(fk_col_datums[i]));
			int attnum = SPI_fnumber(tupdesc, col_name);
			bool isnull;

			if (attnum <= 0)
				elog(ERROR, "column \"%s\" does not exist in table \"%s\"", col_name, RelationGetRelationName(rel));

			(void) heap_getattr(new_row, attnum, tupdesc, &isnull);
			if (isnull)
				has_nulls = true;
			else
				all_nulls = false;
		}

		pfree(DatumGetPointer(fk_column_names_datum));
		if (num_fk_cols > 0)
			pfree(fk_col_datums);

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
			{
				ereport(ERROR, (errmsg("MATCH PARTIAL is not implemented")));
			}
			else if (strcmp(match_type, "FULL") == 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
						 errmsg("insert or update on table \"%s\" violates foreign key constraint \"%s\" (MATCH FULL with NULLs)",
								RelationGetRelationName(rel), foreign_key_name)));
			}
		}
	}

	/* Build and execute validation query */
	{
		char *fk_range_constructor;
		char *uk_range_constructor;
		char *q, *query;
		int ret;
		bool isnull, okay;
		char *uk_where_clause;
		Oid argtypes[] = { REGCLASSOID, NAMEOID };
		Datum values[2];
		StringInfoData where_buf;
		Datum uk_column_names_datum;
		ArrayType *uk_column_names_array;
		int num_uk_cols;
		Datum *uk_col_datums;
		Datum fk_column_names_datum;
		ArrayType *fk_column_names_array;
		int num_fk_cols;
		Datum *fk_col_datums;
		int i;
		char *fk_start_val_str;
		char *fk_end_val_str;
		char *quoted_start;
		char *quoted_end;

		/* Get range constructor types from sql_saga.era */
		q = "SELECT range_type::regtype::text FROM sql_saga.era WHERE table_oid = $1 AND era_name = $2";
		
		values[0] = ObjectIdGetDatum(RelationGetRelid(rel));
		values[1] = CStringGetDatum(tgargs[5]);
		
		ret = SPI_execute_with_args(q, 2, argtypes, values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for foreign key table %s era %s", RelationGetRelationName(rel), tgargs[5]);
		fk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		values[0] = DirectFunctionCall1(regclassin, CStringGetDatum(uk_table_oid_str));
		values[1] = CStringGetDatum(uk_era_name);
		ret = SPI_execute_with_args(q, 2, argtypes, values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for unique key table %s era %s", uk_table_oid_str, uk_era_name);
		uk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		/* Build uk_where_clause */
		initStringInfo(&where_buf);

		get_type_io_data(NAMEARRAYOID, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &typioparam_oid, &typinput_func_oid);
		uk_column_names_datum = OidInputFunctionCall(typinput_func_oid, uk_column_names_str, typioparam_oid, -1);

		uk_column_names_array = DatumGetArrayTypeP(uk_column_names_datum);
		deconstruct_array(uk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &uk_col_datums, NULL, &num_uk_cols);

		fk_column_names_datum = OidInputFunctionCall(typinput_func_oid, fk_column_names_str, typioparam_oid, -1);
		fk_column_names_array = DatumGetArrayTypeP(fk_column_names_datum);
		deconstruct_array(fk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &fk_col_datums, NULL, &num_fk_cols);

		for (i = 0; i < num_uk_cols; i++)
		{
			char *ukc = NameStr(*DatumGetName(uk_col_datums[i]));
			char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
			int attnum = SPI_fnumber(tupdesc, fkc);
			char *val_str;
			char *quoted_val;

			if (attnum <= 0)
				elog(ERROR, "column \"%s\" does not exist in table \"%s\"", fkc, RelationGetRelationName(rel));

			val_str = SPI_getvalue(new_row, tupdesc, attnum);

			if (i > 0)
				appendStringInfoString(&where_buf, " AND ");
			
			quoted_val = quote_literal_cstr(val_str);
			appendStringInfo(&where_buf, "uk.%s = %s", quote_identifier(ukc), quoted_val);
			pfree(quoted_val);

			if (val_str) pfree(val_str);
		}
		uk_where_clause = where_buf.data;

		pfree(DatumGetPointer(uk_column_names_datum));
		if (num_uk_cols > 0) pfree(uk_col_datums);
		pfree(DatumGetPointer(fk_column_names_datum));
		if (num_fk_cols > 0) pfree(fk_col_datums);

		/* Get values for fk range */
		fk_start_val_str = SPI_getvalue(new_row, tupdesc, SPI_fnumber(tupdesc, fk_start_after_column_name));
		fk_end_val_str = SPI_getvalue(new_row, tupdesc, SPI_fnumber(tupdesc, fk_stop_on_column_name));
		quoted_start = quote_literal_cstr(fk_start_val_str);
		quoted_end = quote_literal_cstr(fk_end_val_str);

		query = psprintf(
			"SELECT COALESCE(("
			"  SELECT sql_saga.covers_without_gaps("
			"    %s(uk.%s, uk.%s, '(]'),"
			"    %s(%s, %s, '(]')"
			"    ORDER BY uk.%s"
			"  )"
			"  FROM %s.%s AS uk"
			"  WHERE %s"
			"), false)",
			uk_range_constructor,
			quote_identifier(uk_start_after_column_name),
			quote_identifier(uk_stop_on_column_name),
			fk_range_constructor,
			quoted_start,
			quoted_end,
			quote_identifier(uk_start_after_column_name),
			quote_identifier(uk_schema_name),
			quote_identifier(uk_table_name),
			uk_where_clause
		);
		pfree(quoted_start);
		pfree(quoted_end);

		ret = SPI_execute(query, true, 1);
		if (ret != SPI_OK_SELECT)
			elog(ERROR, "SPI_execute failed: %s", query);
		
		if (SPI_processed > 0)
		{
			okay = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
			if (isnull)
				okay = false;
		}
		else
		{
			okay = false;
		}

		pfree(query);
		pfree(uk_where_clause);
		if(fk_range_constructor) pfree(fk_range_constructor);
		if(uk_range_constructor) pfree(uk_range_constructor);
		if(fk_start_val_str) pfree(fk_start_val_str);
		if(fk_end_val_str) pfree(fk_end_val_str);

		if (!okay)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
					 errmsg("insert or update on table \"%s.%s\" violates foreign key constraint \"%s\"",
							tgargs[2], tgargs[3], foreign_key_name)));
		}
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
	char *fk_start_after_column_name;
	char *fk_stop_on_column_name;
	Oid uk_table_oid;
	char *uk_schema_name;
	char *uk_table_name;
	char *uk_column_names_str;
	char *uk_era_name;
	char *uk_start_after_column_name;
	char *uk_stop_on_column_name;

	/* For get_type_io_data */
	Oid			typinput_func_oid;
	Oid			typioparam_oid;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char		typdelim;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "uk_delete_check_c: not called by trigger manager");

	trigdata = (TriggerData *) fcinfo->context;
	rettuple = trigdata->tg_trigtuple;
	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;
	old_row = trigdata->tg_trigtuple;
	uk_table_oid = RelationGetRelid(rel);

	if (trigdata->tg_trigger->tgnargs != 18)
		elog(ERROR, "uk_delete_check_c: expected 18 arguments, got %d", trigdata->tg_trigger->tgnargs);

	tgargs = trigdata->tg_trigger->tgargs;

	foreign_key_name = tgargs[0];
	/* fk_table_oid_str = tgargs[1]; */
	fk_schema_name = tgargs[2];
	fk_table_name = tgargs[3];
	fk_column_names_str = tgargs[4];
	fk_era_name = tgargs[5];
	fk_start_after_column_name = tgargs[6];
	fk_stop_on_column_name = tgargs[7];
	uk_schema_name = tgargs[9];
	uk_table_name = tgargs[10];
	uk_column_names_str = tgargs[11];
	uk_era_name = tgargs[12];
	uk_start_after_column_name = tgargs[13];
	uk_stop_on_column_name = tgargs[14];

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* Check for NULLs in UK columns of old_row */
	{
		Datum		uk_column_names_datum;
		ArrayType  *uk_column_names_array;
		int			num_uk_cols;
		Datum	   *uk_col_datums;
		int i;

		get_type_io_data(NAMEARRAYOID, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &typioparam_oid, &typinput_func_oid);
		uk_column_names_datum = OidInputFunctionCall(typinput_func_oid, uk_column_names_str, typioparam_oid, -1);
		uk_column_names_array = DatumGetArrayTypeP(uk_column_names_datum);
		deconstruct_array(uk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &uk_col_datums, NULL, &num_uk_cols);

		for (i = 0; i < num_uk_cols; i++)
		{
			char *col_name = NameStr(*DatumGetName(uk_col_datums[i]));
			int attnum = SPI_fnumber(tupdesc, col_name);
			bool isnull;

			if (attnum <= 0)
				elog(ERROR, "column \"%s\" does not exist in table \"%s\"", col_name, RelationGetRelationName(rel));

			(void) heap_getattr(old_row, attnum, tupdesc, &isnull);
			if (isnull)
			{
				pfree(DatumGetPointer(uk_column_names_datum));
				if (num_uk_cols > 0) pfree(uk_col_datums);
				SPI_finish();
				return PointerGetDatum(rettuple);
			}
		}

		pfree(DatumGetPointer(uk_column_names_datum));
		if (num_uk_cols > 0) pfree(uk_col_datums);
	}

	/* Build and execute validation query */
	{
		char *fk_range_constructor;
		char *uk_range_constructor;
		char *q, *query;
		int ret;
		bool isnull, violation;
		char *join_on_clause;
		char *where_clause;
		char *exclude_old_row_clause;
		Oid argtypes[] = { REGCLASSOID, NAMEOID };
		Datum values[2];
		StringInfoData join_buf;
		StringInfoData where_buf;
		StringInfoData exclude_buf;
		int i;
		Datum uk_column_names_datum;
		ArrayType *uk_column_names_array;
		int num_uk_cols;
		Datum *uk_col_datums;
		Datum fk_column_names_datum;
		ArrayType *fk_column_names_array;
		int num_fk_cols;
		Datum *fk_col_datums;
		Oid fk_table_oid;

		/* Get range constructor types from sql_saga.era */
		q = "SELECT range_type::regtype::text FROM sql_saga.era WHERE table_oid = $1 AND era_name = $2";

		fk_table_oid = DirectFunctionCall1(regclassin, CStringGetDatum(psprintf("%s.%s", quote_identifier(fk_schema_name), quote_identifier(fk_table_name))));
		
		values[0] = ObjectIdGetDatum(fk_table_oid);
		values[1] = CStringGetDatum(fk_era_name);
		
		ret = SPI_execute_with_args(q, 2, argtypes, values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for foreign key table %s.%s era %s", fk_schema_name, fk_table_name, fk_era_name);
		fk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		values[0] = ObjectIdGetDatum(uk_table_oid);
		values[1] = CStringGetDatum(uk_era_name);
		ret = SPI_execute_with_args(q, 2, argtypes, values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for unique key table %s era %s", quote_identifier(RelationGetRelationName(rel)), uk_era_name);
		uk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		/* Build join_on, where, and exclude clauses */
		initStringInfo(&join_buf);
		initStringInfo(&where_buf);
		initStringInfo(&exclude_buf);

		uk_column_names_datum = OidInputFunctionCall(typinput_func_oid, uk_column_names_str, typioparam_oid, -1);
		uk_column_names_array = DatumGetArrayTypeP(uk_column_names_datum);
		deconstruct_array(uk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &uk_col_datums, NULL, &num_uk_cols);

		fk_column_names_datum = OidInputFunctionCall(typinput_func_oid, fk_column_names_str, typioparam_oid, -1);
		fk_column_names_array = DatumGetArrayTypeP(fk_column_names_datum);
		deconstruct_array(fk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &fk_col_datums, NULL, &num_fk_cols);

		appendStringInfoString(&exclude_buf, " AND NOT (");

		for (i = 0; i < num_uk_cols; i++)
		{
			char *ukc = NameStr(*DatumGetName(uk_col_datums[i]));
			char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
			int attnum = SPI_fnumber(tupdesc, ukc);
			char *val_str;
			char *quoted_val;

			if (attnum <= 0)
				elog(ERROR, "column \"%s\" does not exist in table \"%s\"", ukc, RelationGetRelationName(rel));

			val_str = SPI_getvalue(old_row, tupdesc, attnum);
			quoted_val = quote_literal_cstr(val_str);

			if (i > 0)
			{
				appendStringInfoString(&join_buf, " AND ");
				appendStringInfoString(&where_buf, " AND ");
				appendStringInfoString(&exclude_buf, " AND ");
			}
			appendStringInfo(&join_buf, "fk.%s = uk.%s", quote_identifier(fkc), quote_identifier(ukc));
			appendStringInfo(&where_buf, "fk.%s = %s", quote_identifier(fkc), quoted_val);
			appendStringInfo(&exclude_buf, "uk.%s = %s", quote_identifier(ukc), quoted_val);
			
			pfree(quoted_val);
			if (val_str) pfree(val_str);
		}
		
		/* Add era columns to exclude clause */
		{
			int attnum;
			char *val_str;
			char *quoted_val;
			
			if (num_uk_cols > 0)
				appendStringInfoString(&exclude_buf, " AND ");

			attnum = SPI_fnumber(tupdesc, uk_start_after_column_name);
			val_str = SPI_getvalue(old_row, tupdesc, attnum);
			quoted_val = quote_literal_cstr(val_str);
			appendStringInfo(&exclude_buf, "uk.%s = %s", quote_identifier(uk_start_after_column_name), quoted_val);
			pfree(quoted_val);
			if (val_str) pfree(val_str);

			attnum = SPI_fnumber(tupdesc, uk_stop_on_column_name);
			val_str = SPI_getvalue(old_row, tupdesc, attnum);
			quoted_val = quote_literal_cstr(val_str);
			appendStringInfo(&exclude_buf, " AND uk.%s = %s", quote_identifier(uk_stop_on_column_name), quoted_val);
			pfree(quoted_val);
			if (val_str) pfree(val_str);
		}
		appendStringInfoChar(&exclude_buf, ')');
		
		join_on_clause = join_buf.data;
		where_clause = where_buf.data;
		exclude_old_row_clause = exclude_buf.data;

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
			"      %s(uk.%s, uk.%s, '(]'),"
			"      %s(fk.%s, fk.%s, '(]')"
			"      ORDER BY uk.%s"
			"    )"
			"    FROM %s.%s AS uk"
			"    WHERE %s%s"
			"  ), true)"
			")",
			quote_identifier(fk_schema_name),
			quote_identifier(fk_table_name),
			where_clause,
			uk_range_constructor,
			quote_identifier(uk_start_after_column_name),
			quote_identifier(uk_stop_on_column_name),
			fk_range_constructor,
			quote_identifier(fk_start_after_column_name),
			quote_identifier(fk_stop_on_column_name),
			quote_identifier(uk_start_after_column_name),
			quote_identifier(uk_schema_name),
			quote_identifier(uk_table_name),
			join_on_clause,
			exclude_old_row_clause
		);

		ret = SPI_execute(query, true, 1);
		if (ret != SPI_OK_SELECT)
			elog(ERROR, "SPI_execute failed: %s", query);
		
		if (SPI_processed > 0)
		{
			violation = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
			if (isnull)
				violation = true; /* Should not happen with EXISTS */
		}
		else
		{
			violation = true; /* Should not happen with EXISTS */
		}

		pfree(query);
		pfree(join_on_clause);
		pfree(where_clause);
		pfree(exclude_old_row_clause);
		if(fk_range_constructor) pfree(fk_range_constructor);
		if(uk_range_constructor) pfree(uk_range_constructor);

		if (violation)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
					 errmsg("update or delete on table \"%s.%s\" violates foreign key constraint \"%s\" on table \"%s\"",
							uk_schema_name, uk_table_name, foreign_key_name, tgargs[1])));
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
	char *fk_start_after_column_name;
	char *fk_stop_on_column_name;
	Oid uk_table_oid;
	char *uk_schema_name;
	char *uk_table_name;
	char *uk_column_names_str;
	char *uk_era_name;
	char *uk_start_after_column_name;
	char *uk_stop_on_column_name;
	bool keys_changed = false;

	/* For get_type_io_data */
	Oid			typinput_func_oid;
	Oid			typioparam_oid;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	char		typdelim;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "uk_update_check_c: not called by trigger manager");

	trigdata = (TriggerData *) fcinfo->context;
	rettuple = trigdata->tg_newtuple;
	rel = trigdata->tg_relation;
	tupdesc = rel->rd_att;
	old_row = trigdata->tg_trigtuple;
	new_row = trigdata->tg_newtuple;
	uk_table_oid = RelationGetRelid(rel);

	if (trigdata->tg_trigger->tgnargs != 18)
		elog(ERROR, "uk_update_check_c: expected 18 arguments, got %d", trigdata->tg_trigger->tgnargs);

	tgargs = trigdata->tg_trigger->tgargs;

	foreign_key_name = tgargs[0];
	fk_schema_name = tgargs[2];
	fk_table_name = tgargs[3];
	fk_column_names_str = tgargs[4];
	fk_era_name = tgargs[5];
	fk_start_after_column_name = tgargs[6];
	fk_stop_on_column_name = tgargs[7];
	uk_schema_name = tgargs[9];
	uk_table_name = tgargs[10];
	uk_column_names_str = tgargs[11];
	uk_era_name = tgargs[12];
	uk_start_after_column_name = tgargs[13];
	uk_stop_on_column_name = tgargs[14];

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* Check for NULLs in UK columns of old_row and if key columns changed */
	{
		Datum		uk_column_names_datum;
		ArrayType  *uk_column_names_array;
		int			num_uk_cols;
		Datum	   *uk_col_datums;
		int i;

		get_type_io_data(NAMEARRAYOID, IOFunc_input, &typlen, &typbyval, &typalign, &typdelim, &typioparam_oid, &typinput_func_oid);
		uk_column_names_datum = OidInputFunctionCall(typinput_func_oid, uk_column_names_str, typioparam_oid, -1);
		uk_column_names_array = DatumGetArrayTypeP(uk_column_names_datum);
		deconstruct_array(uk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &uk_col_datums, NULL, &num_uk_cols);

		for (i = 0; i < num_uk_cols; i++)
		{
			char *col_name = NameStr(*DatumGetName(uk_col_datums[i]));
			int attnum = SPI_fnumber(tupdesc, col_name);
			bool isnull_old, isnull_new;
			Datum old_val, new_val;
			Oid typid;
			int16 typlen_val;
			bool typbyval_val;

			if (attnum <= 0)
				elog(ERROR, "column \"%s\" does not exist in table \"%s\"", col_name, RelationGetRelationName(rel));

			old_val = heap_getattr(old_row, attnum, tupdesc, &isnull_old);
			if (isnull_old)
			{
				pfree(DatumGetPointer(uk_column_names_datum));
				if (num_uk_cols > 0) pfree(uk_col_datums);
				SPI_finish();
				return PointerGetDatum(rettuple);
			}

			new_val = heap_getattr(new_row, attnum, tupdesc, &isnull_new);
			if (isnull_new) /* Should be prevented by NOT NULL constraint */
			{
				keys_changed = true;
				break;
			}
			
			typid = SPI_gettypeid(tupdesc, attnum);
			get_typlenbyval(typid, &typlen_val, &typbyval_val);
			if (!datumIsEqual(old_val, new_val, typbyval_val, typlen_val))
			{
				keys_changed = true;
				break;
			}
		}

		pfree(DatumGetPointer(uk_column_names_datum));
		if (num_uk_cols > 0) pfree(uk_col_datums);
	}

	/* Build and execute validation query */
	{
		char *fk_range_constructor;
		char *uk_range_constructor;
		char *q, *query;
		int ret;
		bool isnull, violation;
		char *join_on_clause;
		char *where_clause;
		char *exclude_old_row_clause;
		char *union_new_row_clause = "";
		char *sub_query_select_list = "*";
		char *sub_query_alias = "";
		char *inner_alias;
		Oid argtypes[] = { REGCLASSOID, NAMEOID };
		Datum values[2];
		StringInfoData join_buf;
		StringInfoData where_buf;
		StringInfoData exclude_buf;
		StringInfoData union_buf;
		StringInfoData select_list_buf;
		StringInfoData alias_buf;
		int i;
		Datum uk_column_names_datum;
		ArrayType *uk_column_names_array;
		int num_uk_cols;
		Datum *uk_col_datums;
		Datum fk_column_names_datum;
		ArrayType *fk_column_names_array;
		int num_fk_cols;
		Datum *fk_col_datums;
		Oid fk_table_oid;

		/* Get range constructor types from sql_saga.era */
		q = "SELECT range_type::regtype::text FROM sql_saga.era WHERE table_oid = $1 AND era_name = $2";

		fk_table_oid = DirectFunctionCall1(regclassin, CStringGetDatum(psprintf("%s.%s", quote_identifier(fk_schema_name), quote_identifier(fk_table_name))));
		values[0] = ObjectIdGetDatum(fk_table_oid);
		values[1] = CStringGetDatum(fk_era_name);
		ret = SPI_execute_with_args(q, 2, argtypes, values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for foreign key table %s.%s era %s", fk_schema_name, fk_table_name, fk_era_name);
		fk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		values[0] = ObjectIdGetDatum(uk_table_oid);
		values[1] = CStringGetDatum(uk_era_name);
		ret = SPI_execute_with_args(q, 2, argtypes, values, NULL, true, 1);
		if (ret != SPI_OK_SELECT || SPI_processed == 0)
			elog(ERROR, "could not get range type for unique key table %s era %s", quote_identifier(RelationGetRelationName(rel)), uk_era_name);
		uk_range_constructor = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		/* Build clauses */
		initStringInfo(&join_buf);
		initStringInfo(&where_buf);
		initStringInfo(&exclude_buf);
		initStringInfo(&alias_buf);
		
		uk_column_names_datum = OidInputFunctionCall(typinput_func_oid, uk_column_names_str, typioparam_oid, -1);
		uk_column_names_array = DatumGetArrayTypeP(uk_column_names_datum);
		deconstruct_array(uk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &uk_col_datums, NULL, &num_uk_cols);

		fk_column_names_datum = OidInputFunctionCall(typinput_func_oid, fk_column_names_str, typioparam_oid, -1);
		fk_column_names_array = DatumGetArrayTypeP(fk_column_names_datum);
		deconstruct_array(fk_column_names_array, NAMEOID, NAMEDATALEN, false, 'c', &fk_col_datums, NULL, &num_fk_cols);

		appendStringInfoString(&exclude_buf, " AND NOT (");
		for (i = 0; i < num_uk_cols; i++)
		{
			char *ukc = NameStr(*DatumGetName(uk_col_datums[i]));
			char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
			int attnum = SPI_fnumber(tupdesc, ukc);
			char *val_str, *quoted_val;
			if (attnum <= 0) elog(ERROR, "column \"%s\" does not exist", ukc);
			val_str = SPI_getvalue(old_row, tupdesc, attnum);
			quoted_val = quote_literal_cstr(val_str);
			if (i > 0) { appendStringInfoString(&join_buf, " AND "); appendStringInfoString(&where_buf, " AND "); appendStringInfoString(&exclude_buf, " AND "); }
			appendStringInfo(&join_buf, "fk.%s = uk.%s", quote_identifier(fkc), quote_identifier(ukc));
			appendStringInfo(&where_buf, "fk.%s = %s", quote_identifier(fkc), quoted_val);
			appendStringInfo(&exclude_buf, "uk.%s = %s", quote_identifier(ukc), quoted_val);
			pfree(quoted_val); if (val_str) pfree(val_str);
		}
		
		{ /* Add era columns to exclude clause */
			int attnum; char *val_str, *quoted_val;
			if (num_uk_cols > 0) appendStringInfoString(&exclude_buf, " AND ");
			attnum = SPI_fnumber(tupdesc, uk_start_after_column_name); val_str = SPI_getvalue(old_row, tupdesc, attnum); quoted_val = quote_literal_cstr(val_str);
			appendStringInfo(&exclude_buf, "uk.%s = %s", quote_identifier(uk_start_after_column_name), quoted_val);
			pfree(quoted_val); if (val_str) pfree(val_str);
			attnum = SPI_fnumber(tupdesc, uk_stop_on_column_name); val_str = SPI_getvalue(old_row, tupdesc, attnum); quoted_val = quote_literal_cstr(val_str);
			appendStringInfo(&exclude_buf, " AND uk.%s = %s", quote_identifier(uk_stop_on_column_name), quoted_val);
			pfree(quoted_val); if (val_str) pfree(val_str);
		}
		appendStringInfoChar(&exclude_buf, ')');
		
		where_clause = where_buf.data;
		exclude_old_row_clause = exclude_buf.data;

		if (!keys_changed)
		{
			inner_alias = "sub_uk";
			initStringInfo(&union_buf);
			initStringInfo(&select_list_buf);
			appendStringInfoString(&union_buf, " UNION ALL SELECT ");
			appendStringInfo(&alias_buf, " AS %s(", inner_alias);
			for (i = 0; i < num_uk_cols; i++) {
				char *ukc = NameStr(*DatumGetName(uk_col_datums[i]));
				int attnum = SPI_fnumber(tupdesc, ukc);
				char *val_str = SPI_getvalue(new_row, tupdesc, attnum);
				char *quoted_val = quote_literal_cstr(val_str);
				if (i > 0) { appendStringInfoString(&union_buf, ", "); appendStringInfoString(&select_list_buf, ", "); appendStringInfoString(&alias_buf, ", "); }
				appendStringInfo(&select_list_buf, "%s", quote_identifier(ukc));
				appendStringInfo(&alias_buf, "%s", quote_identifier(ukc));
				appendStringInfo(&union_buf, "%s", quoted_val);
				pfree(quoted_val); if (val_str) pfree(val_str);
			}
			appendStringInfo(&select_list_buf, ", %s, %s", quote_identifier(uk_start_after_column_name), quote_identifier(uk_stop_on_column_name));
			appendStringInfo(&alias_buf, ", %s, %s", quote_identifier(uk_start_after_column_name), quote_identifier(uk_stop_on_column_name));
			{
				char *val_str, *quoted_val;
				appendStringInfoString(&union_buf, ", ");
				val_str = SPI_getvalue(new_row, tupdesc, SPI_fnumber(tupdesc, uk_start_after_column_name)); quoted_val = quote_literal_cstr(val_str);
				appendStringInfo(&union_buf, "%s", quoted_val); pfree(quoted_val); if(val_str) pfree(val_str);
				appendStringInfoString(&union_buf, ", ");
				val_str = SPI_getvalue(new_row, tupdesc, SPI_fnumber(tupdesc, uk_stop_on_column_name)); quoted_val = quote_literal_cstr(val_str);
				appendStringInfo(&union_buf, "%s", quoted_val); pfree(quoted_val); if(val_str) pfree(val_str);
			}
			appendStringInfoChar(&alias_buf, ')');
			union_new_row_clause = union_buf.data;
			sub_query_select_list = select_list_buf.data;
			sub_query_alias = alias_buf.data;
			resetStringInfo(&join_buf);
			for (i = 0; i < num_uk_cols; i++) {
				char *ukc = NameStr(*DatumGetName(uk_col_datums[i])); char *fkc = NameStr(*DatumGetName(fk_col_datums[i]));
				if (i > 0) appendStringInfoString(&join_buf, " AND ");
				appendStringInfo(&join_buf, "fk.%s = %s.%s", quote_identifier(fkc), inner_alias, quote_identifier(ukc));
			}
			join_on_clause = join_buf.data;
		}
		else
		{
			inner_alias = "uk";
			appendStringInfo(&alias_buf, " AS %s", inner_alias);
			sub_query_alias = alias_buf.data;
			join_on_clause = join_buf.data;
		}

		query = psprintf(
			"SELECT EXISTS ("
			"  SELECT 1"
			"  FROM %s.%s AS fk"
			"  WHERE %s AND COALESCE(NOT ("
			"    SELECT sql_saga.covers_without_gaps("
			"      %s(%s.%s, %s.%s, '(]'),"
			"      %s(fk.%s, fk.%s, '(]')"
			"      ORDER BY %s.%s"
			"    )"
			"    FROM (SELECT %s FROM %s.%s AS uk WHERE TRUE %s %s) %s"
			"    WHERE %s"
			"  ), true)"
			")",
			quote_identifier(fk_schema_name), quote_identifier(fk_table_name), where_clause,
			uk_range_constructor, inner_alias, quote_identifier(uk_start_after_column_name), inner_alias, quote_identifier(uk_stop_on_column_name),
			fk_range_constructor, quote_identifier(fk_start_after_column_name), quote_identifier(fk_stop_on_column_name),
			inner_alias, quote_identifier(uk_start_after_column_name),
			sub_query_select_list, quote_identifier(uk_schema_name), quote_identifier(uk_table_name),
			exclude_old_row_clause, union_new_row_clause, sub_query_alias,
			join_on_clause
		);

		ret = SPI_execute(query, true, 1);
		if (ret != SPI_OK_SELECT) elog(ERROR, "SPI_execute failed: %s", query);
		
		if (SPI_processed > 0) {
			violation = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
			if (isnull) violation = true;
		} else {
			violation = true;
		}

		pfree(query);
		pfree(where_clause);
		pfree(exclude_old_row_clause);
		if (!keys_changed) { 
			pfree(union_new_row_clause);
			pfree(sub_query_select_list);
		}
		pfree(sub_query_alias);
		pfree(join_on_clause);
		if(fk_range_constructor) pfree(fk_range_constructor); if(uk_range_constructor) pfree(uk_range_constructor);
		pfree(DatumGetPointer(uk_column_names_datum)); if (num_uk_cols > 0) pfree(uk_col_datums);
		pfree(DatumGetPointer(fk_column_names_datum)); if (num_fk_cols > 0) pfree(fk_col_datums);

		if (violation)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
					 errmsg("update or delete on table \"%s.%s\" violates foreign key constraint \"%s\" on table \"%s\"",
							uk_schema_name, uk_table_name, foreign_key_name, tgargs[1])));
		}
	}

	SPI_finish();
	return PointerGetDatum(rettuple);
}

void _PG_init(void) {
}

void _PG_fini(void) {
}

