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
override CONTRIB_TESTDB = sql_saga_regress
REGRESS_OPTS += --create-role=$(CONTRIB_TESTDB)

# Conditionally add pg_stat_monitor if it's available.
# We connect to template1 as it's guaranteed to exist.
# The `psql` command will return an empty string if the extension is not found or if psql fails,
# in which case the option will not be added.
PG_STAT_MONITOR_AVAILABLE = $(shell psql -d template1 --quiet -t -c "SELECT 1 FROM pg_available_extensions WHERE name = 'pg_stat_monitor'" 2>/dev/null | grep -q 1 && echo yes)
ifeq ($(PG_STAT_MONITOR_AVAILABLE),yes)
	REGRESS_OPTS += --load-extension=pg_stat_monitor
endif

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



# expected updates the .out files for the last run test suite.
# It parses regression.out to find which tests were run, or uses TESTS if provided.
# Usage: `make test [fast|benchmark|TESTS=...]; make expected`
#    or: `make expected TESTS="test1 test2"`
.PHONY: expected
expected:
	@if [ -n "$(TESTS)" ]; then \
		TESTS_TO_UPDATE="$(TESTS)"; \
	elif [ ! -f regression.out ]; then \
		echo "All tests passed. Nothing to update."; \
		exit 0; \
	else \
		TESTS_TO_UPDATE=$$(awk -F ' - ' '/^(not )?ok/ {print $$2}' regression.out | awk '{print $$1}'); \
	fi; \
	if [ -z "$$TESTS_TO_UPDATE" ]; then \
		echo "No tests found in regression.out. Nothing to update."; \
		exit 0; \
	fi; \
	for test in $$TESTS_TO_UPDATE; do \
		if [ -f "results/$$test.out" ]; then \
			echo "Updating expected output for: $$test"; \
			cp "results/$$test.out" "expected/$$test.out"; \
		else \
			echo "Warning: result file for '$$test' not found. Skipping."; \
		fi; \
	done

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
.PHONY: diff-fail-all diff-fail-first vim vimo nvim nvimo
diff-fail-all diff-fail-first:
	@FAILED_TESTS=$$(grep 'not ok' regression.out 2>/dev/null | awk -F ' - ' '{print $$2}' | awk '{print $$1}'); \
	if [ "$@" = "diff-fail-first" ]; then \
		FAILED_TESTS=`echo "$$FAILED_TESTS" | head -n 1`; \
	fi; \
	if [ -n "$(filter vim vimo nvim nvimo,$(MAKECMDGOALS))" ]; then \
		EDITOR_CMD="vim"; \
		if [ -n "$(filter nvim nvimo,$(MAKECMDGOALS))" ]; then \
			EDITOR_CMD="nvim"; \
		fi; \
		EDITOR_OPTS="-d"; \
		if [ -n "$(filter vimo nvimo,$(MAKECMDGOALS))" ]; then \
			EDITOR_OPTS="-d -o"; \
		fi; \
		VIM_CMD="$$EDITOR_CMD $$EDITOR_OPTS"; \
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

vim vimo nvim nvimo:
	@:

#release:
#	git archive --format zip --prefix=$(EXTENSION)-$(EXTENSION_VERSION)/ --output $(EXTENSION)-$(EXTENSION_VERSION).zip master
#
#.PHONY: release
