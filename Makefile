MODULE_big = sql_saga
EXTENSION = sql_saga
EXTENSION_VERSION = 1.0
DATA = $(EXTENSION)--$(EXTENSION_VERSION).sql

DOCS = README.md
#README.html: README.md
#	jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < README.md | curl --data @- https://api.github.com/markdown > README.html

SQL_FILES = $(wildcard sql/[0-9]*_*.sql)

REGRESS = $(if $(TESTS),$(TESTS),$(patsubst sql/%.sql,%,$(SQL_FILES)))

OBJS = sql_saga.o periods.o no_gaps.o $(WIN32RES)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# New target to run vimdiff for the first failing test
vimdiff-fail-first:
	@first_fail=$$(grep 'not ok' regression.out | awk 'BEGIN { FS = "[[:space:]]+" } {print $$5}' | head -n 1); \
	if [ -n "$$first_fail" ]; then \
		echo "Running vimdiff for test: $$first_fail"; \
		vim -d results/$$first_fail.out expected/$$first_fail.out < /dev/tty; \
	else \
		echo "No failing tests found."; \
	fi

# New target to run vimdiff for all failing tests
vimdiff-fail-all:
	@grep 'not ok' regression.out | awk 'BEGIN { FS = "[[:space:]]+" } {print $$5}' | while read test; do \
		echo "Running vimdiff for test: $$test"; \
		vim -d results/$$test.out expected/$$test.out < /dev/tty; \
	done

#release:
#	git archive --format zip --prefix=$(EXTENSION)-$(EXTENSION_VERSION)/ --output $(EXTENSION)-$(EXTENSION_VERSION).zip master
#
#.PHONY: release
