MODULE_big = sql_saga
EXTENSION = sql_saga
EXTVERSION = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

# The main extension script, built from the source files.
# It is listed in DATA so that `make install` will copy it.
DATA = $(EXTENSION)--$(EXTVERSION).sql

# Add the generated script to EXTRA_CLEAN so it's removed on `make clean`.
EXTRA_CLEAN = $(EXTENSION)--$(EXTVERSION).sql

DOCS = README.md
#README.html: README.md
#	jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < README.md | curl --data @- https://api.github.com/markdown > README.html

SQL_FILES = $(wildcard sql/[0-9]*_*.sql)

REGRESS_ALL = $(patsubst sql/%.sql,%,$(SQL_FILES))
BENCHMARK_TESTS = $(foreach test,$(REGRESS_ALL),$(if $(findstring benchmark,$(test)),$(test)))
REGRESS_FAST_LIST = $(filter-out $(BENCHMARK_TESTS),$(REGRESS_ALL))

# By default, run all tests. If 'fast' is a command line goal, run the fast subset.
REGRESS_TO_RUN = $(REGRESS_ALL)
ifeq (fast,$(filter fast,$(MAKECMDGOALS)))
	REGRESS_TO_RUN = $(REGRESS_FAST_LIST)
endif

REGRESS = $(if $(TESTS),$(patsubst sql/%,%,$(TESTS)),$(REGRESS_TO_RUN))
REGRESS_OPTS := --create-role=sql_saga_regress --dbname=sql_saga_regress

OBJS = src/sql_saga.o src/covers_without_gaps.o $(WIN32RES)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Redefine the `all` target to build both the C library (SHLIB) and our
# generated SQL script. PGXS provides the rule to build SHLIB.
all: $(SHLIB) $(EXTENSION)--$(EXTVERSION).sql

# Add the generated SQL file as a direct dependency of the `install` target.
# This ensures it is always rebuilt if its source files have changed before
# tests are run.
install: $(EXTENSION)--$(EXTVERSION).sql

# Build the main extension script from component files.
# The source files are numbered to ensure correct concatenation order.
$(EXTENSION)--$(EXTVERSION).sql: $(wildcard src/[0-9][0-9]_*.sql)
	cat $^ > $@

# New target for benchmark regression test. It depends on `install` to ensure
# the extension is built and installed before the test is run.
benchmark: install
	@$(MAKE) installcheck REGRESS="43_benchmark"

# test is a convenient alias for installcheck.
# To run all tests: `make test`
# To run fast tests (excluding benchmarks): `make test fast`
# To run a single test: `make test TESTS=21_init`
# To run a subset of tests: `make test TESTS="21_init 22_covers_without_gaps_test"`
.PHONY: test setup_test_files
test: setup_test_files installcheck

# Create empty expected files for new tests if they don't exist.
setup_test_files:
	@mkdir -p expected
	@for test in $(REGRESS); do \
		if [ ! -f expected/$$test.out ]; then \
			mkdir -p `dirname "expected/$$test.out"`; \
			touch "expected/$$test.out"; \
		fi; \
	done

# The 'fast' target is a dummy. Its presence in `make test fast` is used to
# trigger the conditional logic that selects the fast test suite.
.PHONY: fast
fast:
	@:

# Target to show diff for failing tests. Use with 'vim' for vimdiff.
# `make diff-fail-all`: shows all failures.
# `make diff-fail-first`: shows the first failure.
.PHONY: diff-fail-all diff-fail-first vim
diff-fail-all diff-fail-first:
	@FAILED_TESTS=`grep 'not ok' regression.out 2>/dev/null | awk 'BEGIN { FS = "[[:space:]]+" } {print $$5}'`; \
	if [ "$@" = "diff-fail-first" ]; then \
		FAILED_TESTS=`echo "$$FAILED_TESTS" | head -n 1`; \
	fi; \
	if [ "$(filter vim,$(MAKECMDGOALS))" = "vim" ]; then \
		for test in $$FAILED_TESTS; do \
			echo "Next test: $$test"; \
			echo "Press C to continue, s to skip, or b to break (default: C)"; \
			read -n 1 -s input < /dev/tty; \
			if [ "$$input" = "b" ]; then \
				break; \
			elif [ "$$input" = "s" ]; then \
				continue; \
			fi; \
			echo "Running vimdiff for test: $$test"; \
			vim -d "expected/$$test.out" "results/$$test.out" < /dev/tty; \
		done; \
	else \
		for test in $$FAILED_TESTS; do \
			echo "Showing diff for test: $$test"; \
			diff -u "expected/$$test.out" "results/$$test.out" || true; \
		done; \
	fi; \
	if [ -n "$$FAILED_TESTS" ]; then exit 1; fi

vim:
	@:

#release:
#	git archive --format zip --prefix=$(EXTENSION)-$(EXTENSION_VERSION)/ --output $(EXTENSION)-$(EXTENSION_VERSION).zip master
#
#.PHONY: release
