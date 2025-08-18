#ifndef SQL_SAGA_H
#define SQL_SAGA_H

#include "postgres.h"
#include "fmgr.h"

Datum fk_insert_check_c(PG_FUNCTION_ARGS);

#endif /* SQL_SAGA_H */
