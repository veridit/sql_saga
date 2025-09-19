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

SQL_FILES = $(wildcard sql/[0-9][0-9][0-9]_*.sql)

REGRESS_ALL = $(patsubst sql/%.sql,%,$(SQL_FILES))
BENCHMARK_TESTS = $(foreach test,$(REGRESS_ALL),$(if $(findstring benchmark,$(test)),$(test)))
REGRESS_FAST_LIST = $(filter-out $(BENCHMARK_TESTS),$(REGRESS_ALL))

# By default, run all tests. If 'fast' or 'benchmark' are goals, run the respective subset.
REGRESS_TO_RUN = $(REGRESS_ALL)
ifeq (fast,$(filter fast,$(MAKECMDGOALS)))
	REGRESS_TO_RUN = $(REGRESS_FAST_LIST)
endif
ifeq (benchmark,$(filter benchmark,$(MAKECMDGOALS)))
	REGRESS_TO_RUN = $(BENCHMARK_TESTS)
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

# test is a convenient alias for installcheck.
# To run all tests: `make test`
# To run fast tests (excluding benchmarks): `make test fast`
# To run benchmark tests: `make test benchmark`
# To run a single test: `make test TESTS=001_install`
# To run a subset of tests: `make test TESTS="001_install 002_era"`
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

# 'fast' and 'benchmark' are dummy targets. Their presence in the command line
# (e.g., `make test fast`) is used to trigger the conditional logic that
# selects the desired test suite.
.PHONY: fast benchmark
fast:
	@:
benchmark:
	@:

# Target to show diff for failing tests. Use with 'vim' for vimdiff.
# `make diff-fail-all`: shows all failures.
# `make diff-fail-first`: shows the first failure.
.PHONY: diff-fail-all diff-fail-first vim vimo
diff-fail-all diff-fail-first:
	@FAILED_TESTS=`grep 'not ok' regression.out 2>/dev/null | awk 'BEGIN { FS = "[[:space:]]+" } {print $$5}'`; \
	if [ "$@" = "diff-fail-first" ]; then \
		FAILED_TESTS=`echo "$$FAILED_TESTS" | head -n 1`; \
	fi; \
	if [ -n "$(filter vim vimo,$(MAKECMDGOALS))" ]; then \
		VIM_CMD="vim -d"; \
		if [ "$(filter vimo,$(MAKECMDGOALS))" = "vimo" ]; then \
			VIM_CMD="vim -d -o"; \
		fi; \
		for test in $$FAILED_TESTS; do \
			if [ "$@" = "diff-fail-all" ]; then \
				echo "Next test: $$test"; \
				echo "Press C to continue, s to skip, or b to break (default: C)"; \
				read -n 1 -s input < /dev/tty; \
				if [ "$$input" = "b" ]; then \
					break; \
				elif [ "$$input" = "s" ]; then \
					continue; \
				fi; \
			fi; \
			echo "Running $$VIM_CMD for test: $$test"; \
			$$VIM_CMD "expected/$$test.out" "results/$$test.out" < /dev/tty; \
		done; \
	else \
		for test in $$FAILED_TESTS; do \
			echo "Showing diff for test: $$test"; \
			diff -u "expected/$$test.out" "results/$$test.out" || true; \
		done; \
	fi; \
	if [ -n "$$FAILED_TESTS" ]; then exit 1; fi

vim vimo:
	@:

#release:
#	git archive --format zip --prefix=$(EXTENSION)-$(EXTENSION_VERSION)/ --output $(EXTENSION)-$(EXTENSION_VERSION).zip master
#
#.PHONY: release
