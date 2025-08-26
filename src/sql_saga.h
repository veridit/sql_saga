#ifndef SQL_SAGA_H
#define SQL_SAGA_H

#include "postgres.h"
#include "fmgr.h"

Datum fk_insert_check_c(PG_FUNCTION_ARGS);
Datum fk_update_check_c(PG_FUNCTION_ARGS);
Datum uk_delete_check_c(PG_FUNCTION_ARGS);
Datum uk_update_check_c(PG_FUNCTION_ARGS);

Datum generated_always_as_row_start_end(PG_FUNCTION_ARGS);
Datum write_history(PG_FUNCTION_ARGS);

#endif /* SQL_SAGA_H */
