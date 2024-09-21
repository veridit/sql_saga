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
		if command -v nvim >/dev/null 2>&1; then \
			nvim -d results/$$first_fail.out expected/$$first_fail.out; \
			echo "Press any key to continue..."; \
			read -n 1 -s; \
			echo "Press C to continue or s to stop..."; \
			read -n 1 -s input; \
			if [ "$$input" = "s" ]; then \
				break; \
			fi; \
			echo "Press C to continue or s to stop..."; \
			read -n 1 -s input; \
			if [ "$$input" = "s" ]; then \
				break; \
			fi; \
		else \
			vim -d results/$$first_fail.out expected/$$first_fail.out < /dev/tty; \
		fi; \
	else \
		echo "No failing tests found."; \
	fi
# New target to run vimdiff for all failing tests
vimdiff-fail-all:
	@grep 'not ok' regression.out | awk 'BEGIN { FS = "[[:space:]]+" } {print $$5}' | while read test; do \
		echo "Running vimdiff for test: $$test"; \
		if command -v nvim >/dev/null 2>&1; then \
			nvim -d results/$$test.out expected/$$test.out; \
		else \
			vim -d results/$$test.out expected/$$test.out < /dev/tty; \
		fi; \
	done
	@grep 'not ok' regression.out | awk 'BEGIN { FS = "[[:space:]]+" } {print $$5}' | while read test; do \
		echo "Running diff for test: $$test"; \
		diff results/$$test.out expected/$$test.out || true; \
	done

#release:
#	git archive --format zip --prefix=$(EXTENSION)-$(EXTENSION_VERSION)/ --output $(EXTENSION)-$(EXTENSION_VERSION).zip master
#
#.PHONY: release
