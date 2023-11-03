MODULE_big = sql_saga
EXTENSION = sql_saga
EXTENSION_VERSION = 1.0
DATA = $(EXTENSION)--$(EXTENSION_VERSION).sql

DOCS = README.md
#README.html: README.md
#	jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < README.md | curl --data @- https://api.github.com/markdown > README.html

SQL_FILES = $(wildcard sql/[0-9]*_*.sql)

REGRESS = $(patsubst sql/%.sql,%,$(SQL_FILES))

OBJS = sql_saga.o periods.o completely_covers.o $(WIN32RES)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

#release:
#	git archive --format zip --prefix=$(EXTENSION)-$(EXTENSION_VERSION)/ --output $(EXTENSION)-$(EXTENSION_VERSION).zip master
#
#.PHONY: release
