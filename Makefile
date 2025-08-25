MODULE_big = sql_saga
EXTENSION = sql_saga
DATA = sql_saga--1.0.sql

DOCS = README.md
#README.html: README.md
#	jq --slurp --raw-input '{"text": "\(.)", "mode": "markdown"}' < README.md | curl --data @- https://api.github.com/markdown > README.html

SQL_FILES = $(wildcard sql/[0-9]*_*.sql)

REGRESS_ALL = $(patsubst sql/%.sql,%,$(SQL_FILES))
REGRESS_FAST_LIST = $(filter-out 43_benchmark,$(REGRESS_ALL))

# By default, run all tests. If 'fast' is a command line goal, run the fast subset.
REGRESS_TO_RUN = $(REGRESS_ALL)
ifeq (fast,$(filter fast,$(MAKECMDGOALS)))
	REGRESS_TO_RUN = $(REGRESS_FAST_LIST)
endif

REGRESS = $(if $(TESTS),$(TESTS),$(REGRESS_TO_RUN))
REGRESS_OPTS := --create-role=sql_saga_regress --dbname=sql_saga_regress

# New target for benchmark regression test
benchmark:
	$(MAKE) installcheck REGRESS="43_benchmark"

OBJS = sql_saga.o covers_without_gaps.o $(WIN32RES)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

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
			touch expected/$$test.out; \
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
