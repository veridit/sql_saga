#ifndef SQL_SAGA_H
#define SQL_SAGA_H

#include "postgres.h"
#include "fmgr.h"

Datum fk_insert_check_c(PG_FUNCTION_ARGS);
Datum fk_update_check_c(PG_FUNCTION_ARGS);
Datum uk_delete_check_c(PG_FUNCTION_ARGS);
Datum uk_update_check_c(PG_FUNCTION_ARGS);
Datum regular_fk_check_c(PG_FUNCTION_ARGS);
Datum uk_truncate_check_c(PG_FUNCTION_ARGS);
Datum require_owner(PG_FUNCTION_ARGS);
Datum api_trampoline(PG_FUNCTION_ARGS);
Datum sql_saga_event_trampoline(PG_FUNCTION_ARGS);
Datum api_caller_sql(PG_FUNCTION_ARGS);
Datum api_caller_is_superuser_sql(PG_FUNCTION_ARGS);
Datum api_caller_search_path_sql(PG_FUNCTION_ARGS);
Datum truncate_as_owner(PG_FUNCTION_ARGS);
Datum require_references(PG_FUNCTION_ARGS);

Datum generated_always_as_row_start_end(PG_FUNCTION_ARGS);
Datum write_history(PG_FUNCTION_ARGS);

#endif /* SQL_SAGA_H */
