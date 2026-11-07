#!/usr/bin/env python3
"""Disposable PostgreSQL experiment, using only Python's standard library and psql.

Example: python3 devops/test_deferred_temporal_checks.py --database sql_saga_defer_proto
Measured run: add --performance --rows 8000 --rounds 5. Setup is excluded.
Never creates/drops databases or alters cluster configuration. Only its UUID schema
is removed. Full transcripts and JSON/TSV survive failures under tmp/.
"""
import argparse
import concurrent.futures
import csv
import json
import os
from pathlib import Path
import platform
import queue
import re
import subprocess
import threading
import time
import uuid


class Session:
    def __init__(self, database, directory, name, timeout):
        self.timeout = timeout
        self.log = (directory / (name + '.log')).open('w', buffering=1)
        self.lines = queue.Queue()
        self.proc = subprocess.Popen(
            ['psql', '-X', '-qAt', '-d', database, '-v', 'ON_ERROR_STOP=0',
             '-v', 'VERBOSITY=verbose', '-v', 'SHOW_CONTEXT=never'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1)
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        self.run("SET application_name = 'saga_defer_%s'; SET statement_timeout = '%ss'; "
                 "SET lock_timeout = '%ss'; SET client_min_messages = warning;" %
                 (name, timeout, timeout))
        self.pid = int(self.run('SELECT pg_backend_pid();')[-1])

    def _read(self):
        for line in self.proc.stdout:
            self.log.write(line)
            self.lines.put(line.rstrip('\n'))
        self.lines.put(None)

    def start(self, sql):
        marker = 'DONE_' + uuid.uuid4().hex
        self.proc.stdin.write(sql + '\n\\echo ' + marker + '\n')
        self.proc.stdin.flush()
        return marker, time.perf_counter()

    def finish(self, pending, allow_error=False):
        marker, started = pending
        result = []
        deadline = time.monotonic() + self.timeout + 10
        while True:
            line = self.lines.get(timeout=max(.01, deadline - time.monotonic()))
            if line is None:
                raise RuntimeError('psql exited: ' + '\n'.join(result))
            if line == marker:
                break
            result.append(line)
        if not allow_error and any('ERROR:' in line or 'FATAL:' in line for line in result):
            raise RuntimeError('\n'.join(result))
        return result, time.perf_counter() - started

    def run(self, sql, allow_error=False):
        return self.finish(self.start(sql), allow_error)[0]

    def close(self):
        if self.proc.poll() is None:
            try:
                self.proc.stdin.write('ROLLBACK;\n\\q\n')
                self.proc.stdin.flush()
            except BrokenPipeError:
                pass
            try:
                self.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=2)
        self.reader.join(timeout=1)
        if not self.reader.is_alive():
            self.log.close()


class Experiment:
    def __init__(self, args, directory):
        self.args = args
        self.directory = directory
        self.schema = 'defer_' + uuid.uuid4().hex[:16]
        self.sessions = []
        self.admin = self.session('observer')
        self.a = self.session('a')
        self.b = self.session('b')
        self.records = []
        self.evidence = {}

    def session(self, name):
        session = Session(self.args.database, self.directory, name, self.args.timeout)
        self.sessions.append(session)
        return session

    def setup(self):
        s = self.schema
        self.admin.run(f"""
CREATE SCHEMA {s};
CREATE TABLE {s}.parent (id int NOT NULL, payload text, valid_range daterange NOT NULL,
 valid_from date, valid_until date);
SELECT sql_saga.add_era('{s}.parent', 'valid_range', 'valid',
 valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('{s}.parent', ARRAY['id'], key_type => 'primary');
CREATE TABLE {s}.regular_child (id int PRIMARY KEY, parent_id int);
CREATE INDEX ON {s}.regular_child (parent_id);
SELECT sql_saga.add_foreign_key('{s}.regular_child', ARRAY['parent_id'], '{s}.parent', ARRAY['id']);
CREATE TABLE {s}.native_child (id int NOT NULL, parent_id int, payload text,
 valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('{s}.native_child', 'valid_range', 'valid',
 valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('{s}.native_child', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_foreign_key('{s}.native_child', ARRAY['parent_id'], '{s}.parent', ARRAY['id']);
CREATE INDEX ON {s}.native_child (parent_id);
""")
        for session, lo in ((self.a, 1), (self.b, self.args.rows + 1)):
            hi = lo + self.args.rows - 1
            session.run(f"""CREATE TEMP TABLE parent_source AS SELECT i AS row_id, i AS id,
 'replacement'::text AS payload, DATE '2020-01-01' AS valid_from,
 DATE '2030-01-01' AS valid_until FROM generate_series({lo},{hi}) AS i;
CREATE TEMP TABLE child_source AS SELECT row_id, id, id AS parent_id, payload,
 valid_from, valid_until FROM parent_source;
ANALYZE parent_source; ANALYZE child_source;""")
        self.reset()
        self.save_lookup_plans()

    def reset(self):
        s = self.schema
        self.admin.run(f"""TRUNCATE {s}.regular_child, {s}.native_child, {s}.parent;
INSERT INTO {s}.parent (id,payload,valid_from,valid_until)
 SELECT i, 'old_a', DATE '2020-01-01', DATE '2025-01-01' FROM generate_series(1,{2*self.args.rows}) AS i
 UNION ALL SELECT i, 'old_b', DATE '2025-01-01', DATE '2030-01-01' FROM generate_series(1,{2*self.args.rows}) AS i;
INSERT INTO {s}.regular_child SELECT i,i FROM generate_series(1,{2*self.args.rows}) AS i;
INSERT INTO {s}.native_child (id,parent_id,payload,valid_from,valid_until)
 SELECT i,i,'old','2020-01-01','2030-01-01' FROM generate_series(1,{2*self.args.rows}) AS i;
ANALYZE {s}.parent; ANALYZE {s}.regular_child; ANALYZE {s}.native_child;""")

    def save_lookup_plans(self):
        """Representative key/ref probes, not a claim to capture SPI's cached plans."""
        s = self.schema
        queries = {
            'parent_key': f'SELECT 1 FROM {s}.parent AS uk WHERE uk.id = 1 LIMIT 1 FOR KEY SHARE OF uk',
            'regular_reference': f'SELECT 1 FROM {s}.regular_child AS fk WHERE fk.parent_id = 1 LIMIT 1 FOR KEY SHARE OF fk',
            'native_reference': f'SELECT 1 FROM {s}.native_child AS fk WHERE fk.parent_id = 1 LIMIT 1 FOR KEY SHARE OF fk',
        }
        plans = {}
        for name, query in queries.items():
            lines = self.admin.run('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' + query + ';')
            plans[name] = {'query': query, 'plan': json.loads('\n'.join(lines))}
        (self.directory / 'lookup_plans.json').write_text(json.dumps(plans, indent=2) + '\n')
        self.evidence['lookup_plans'] = 'lookup_plans.json'
        self.evidence['statistics'] = 'ANALYZE shared tables after each reset and temp sources once, excluded from trial timings'

    def begin(self, mode):
        s = self.schema
        if mode == 'defer':
            return f"BEGIN; CALL sql_saga.defer_temporal_checks('{s}.parent'::regclass);"
        if mode == 'toggle':
            return f"BEGIN; SET CONSTRAINTS ALL DEFERRED; CALL sql_saga.disable_temporal_triggers('{s}.parent'::regclass);"
        return 'BEGIN;'

    def body(self, mode):
        s = self.schema
        tail = (f"CALL sql_saga.enable_temporal_triggers('{s}.parent'::regclass);"
                if mode == 'toggle' else '')
        return self.begin(mode) + f"""
CALL sql_saga.temporal_merge('{s}.parent', 'pg_temp.parent_source',
 natural_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_REPLACE');
SELECT 'parent_delete_count=' || count(*) FROM pg_temp.temporal_merge_plan WHERE operation = 'DELETE';
CALL sql_saga.temporal_merge('{s}.native_child', 'pg_temp.child_source',
 natural_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_REPLACE');
{tail}
"""

    def deletes(self, lines):
        counts = [int(x.split('=')[1]) for x in lines if x.startswith('parent_delete_count=')]
        assert counts and counts[0] > 0, ('No parent DELETE checks queued', lines)
        return counts[0]

    def integrity(self):
        s = self.schema
        counts = self.admin.run(f"""SELECT
 (SELECT count(*) FROM {s}.regular_child AS c WHERE NOT EXISTS
  (SELECT 1 FROM {s}.parent AS p WHERE p.id=c.parent_id)),
 (SELECT count(*) FROM {s}.native_child AS c WHERE NOT EXISTS
  (SELECT 1 FROM {s}.parent AS p WHERE p.id=c.parent_id AND p.valid_range @> c.valid_range)),
 (SELECT count(*) FROM {s}.parent), (SELECT count(*) FROM {s}.native_child),
 (SELECT count(*) FROM {s}.regular_child),
 (SELECT count(*) FROM {s}.parent AS p WHERE p.payload IS DISTINCT FROM 'replacement'
  OR p.valid_from IS DISTINCT FROM DATE '2020-01-01' OR p.valid_until IS DISTINCT FROM DATE '2030-01-01'
  OR p.valid_range IS DISTINCT FROM daterange(p.valid_from,p.valid_until,'[)')),
 (SELECT count(*) FROM {s}.native_child AS c WHERE c.payload IS DISTINCT FROM 'replacement'
  OR c.parent_id IS DISTINCT FROM c.id OR c.valid_from IS DISTINCT FROM DATE '2020-01-01'
  OR c.valid_until IS DISTINCT FROM DATE '2030-01-01'
  OR c.valid_range IS DISTINCT FROM daterange(c.valid_from,c.valid_until,'[)'));""")[-1]
        expected = ['0', '0'] + [str(2*self.args.rows)] * 3 + ['0', '0']
        assert counts.split('|') == expected, (counts, expected)
        return counts

    def wait_for_lock(self, session):
        deadline = time.monotonic() + min(10, self.args.timeout / 2)
        while True:
            rows = self.admin.run(f"SELECT coalesce(wait_event_type,'') FROM pg_stat_activity WHERE pid={session.pid};")
            if rows and rows[-1] == 'Lock':
                return 'Lock'
            if time.monotonic() > deadline:
                raise AssertionError('Expected caller was not observed waiting for Lock')
            time.sleep(.02)

    def commit_races(self):
        """Two staged-DML COMMIT request orders, plus reverse completion order.

        A child COMMIT requested while a parent DELETE holds its tuple must wait
        for that parent transaction. Request order is not completion order.
        The third schedule commits the child while the parent transaction has
        already read its snapshot, then deletes the parent. These schedules are
        correctness probes only and never included in performance timings.
        """
        s = self.schema
        evidence = []
        self.evidence['same_key_commit_races'] = evidence
        for isolation in ('READ COMMITTED', 'REPEATABLE READ'):
            for schedule in ('parent_commit_requested_first', 'child_commit_requested_first',
                             'child_commit_completes_first'):
                key = -100 - len(evidence)
                self.admin.run(f"INSERT INTO {s}.parent (id,payload,valid_from,valid_until) "
                               f"VALUES ({key},'race','2020-01-01','2030-01-01');")
                begin = self.begin('defer').replace('BEGIN;', f'BEGIN ISOLATION LEVEL {isolation};')
                self.a.run(begin + f'INSERT INTO {s}.regular_child VALUES ({key},{key});')
                self.b.run(begin + f'SELECT id FROM {s}.parent WHERE id={key};')
                delete = f'DELETE FROM {s}.parent WHERE id={key} RETURNING id;'
                observed_wait = None
                if schedule == 'child_commit_completes_first':
                    child = self.a.run('COMMIT;', allow_error=True)
                    assert str(key) in self.b.run(delete)
                    parent = self.b.run('COMMIT;', allow_error=True)
                else:
                    assert str(key) in self.b.run(delete)
                    if schedule == 'child_commit_requested_first':
                        pending = self.a.start('COMMIT;')
                        observed_wait = self.wait_for_lock(self.a)
                        parent = self.b.run('COMMIT;', allow_error=True)
                        child = self.a.finish(pending, allow_error=True)[0]
                    else:
                        parent = self.b.run('COMMIT;', allow_error=True)
                        child = self.a.run('COMMIT;', allow_error=True)
                def state(lines):
                    errors = [re.search(r'ERROR:\s+([0-9A-Z]{5}):', line) for line in lines if 'ERROR:' in line]
                    if not errors:
                        return 'committed'
                    assert len(errors) == 1 and errors[0], lines
                    return errors[0].group(1)
                states = {'child': state(child), 'parent': state(parent)}
                assert sorted(states.values()).count('committed') == 1, (schedule, states)
                assert set(states.values()) <= {'committed', '23503', '40001'}, states
                if schedule == 'child_commit_completes_first':
                    assert states['child'] == 'committed', states
                else:
                    assert states['parent'] == 'committed', states
                orphans = self.admin.run(f"SELECT count(*) FROM {s}.regular_child AS c "
                    f"WHERE c.id={key} AND NOT EXISTS (SELECT 1 FROM {s}.parent AS p WHERE p.id=c.parent_id);")[-1]
                assert orphans == '0', (schedule, states, orphans)
                evidence.append({'isolation': isolation, 'schedule': schedule, 'commit_results': states,
                                 'child_commit_wait_event_type': observed_wait, 'orphan_count': int(orphans)})
                assert self.a.run('SELECT 42;')[-1] == '42'
                assert self.b.run('SELECT 42;')[-1] == '42'
                self.admin.run(f'DELETE FROM {s}.regular_child WHERE id={key}; DELETE FROM {s}.parent WHERE id={key};')
        self.evidence['post_race_integrity'] = self.integrity()

    def correctness(self):
        s = self.schema
        pa, pb = self.a.start(self.body('defer')), self.b.start(self.body('defer'))
        self.evidence['disjoint_parent_deletes'] = [self.deletes(self.a.finish(pa)[0]), self.deletes(self.b.finish(pb)[0])]
        locks = self.admin.run(f"""SELECT count(*) FROM pg_locks AS l
 JOIN pg_class AS c ON c.oid=l.relation JOIN pg_namespace AS n ON n.oid=c.relnamespace
 WHERE n.nspname='{s}' AND l.pid IN ({self.a.pid},{self.b.pid})
 AND l.mode IN ('ShareRowExclusiveLock','AccessExclusiveLock');""")[-1]
        assert locks == '0', locks
        self.evidence['strong_table_locks_before_either_commit'] = int(locks)
        self.a.run('COMMIT;')
        self.b.run('COMMIT;')
        self.evidence['post_concurrent_integrity'] = self.integrity()
        self.reset()
        self.a.run(self.body('toggle'))
        pending = self.b.start(self.body('toggle'))
        wait = self.wait_for_lock(self.b)
        self.evidence['toggle_second_wait_event_type'] = wait
        self.a.run('COMMIT;')
        self.deletes(self.b.finish(pending)[0])
        self.b.run('COMMIT;')
        # Compare primary diagnostics from real immediate statement and real COMMIT.
        invalid = f"INSERT INTO {s}.regular_child VALUES (-1,-1);"
        self.a.run(self.begin('defer'))
        immediate = self.b.run('BEGIN; ' + invalid, allow_error=True)
        self.b.run('ROLLBACK;')
        self.a.run(invalid)
        deferred = self.a.run('COMMIT;', allow_error=True)
        def diagnostic(lines):
            return next((x[x.index('ERROR:'):] for x in lines if 'ERROR:' in x), '')
        assert '23503' in diagnostic(immediate), immediate
        assert diagnostic(immediate) == diagnostic(deferred), (immediate, deferred)
        assert self.a.run('SELECT 42;')[-1] == '42'
        self.evidence['immediate_error'] = diagnostic(immediate)
        self.evidence['commit_error'] = diagnostic(deferred)
        self.evidence['session_reused_after_commit_failure'] = True
        self.evidence['unopted_session_immediate_while_other_defers'] = True
        self.evidence['final_integrity'] = self.integrity()
        # Include the native child so only the sql_saga regular-parent guard fires.
        self.a.run(self.begin('defer'))
        truncate = self.a.run(f'TRUNCATE {s}.parent, {s}.native_child;', allow_error=True)
        assert '0A000' in diagnostic(truncate), truncate
        assert any('uk_truncate_check_c' in line for line in truncate), truncate
        self.a.run('ROLLBACK;')
        self.evidence['truncate_immediate_error'] = diagnostic(truncate)
        self.evidence['post_truncate_integrity'] = self.integrity()
        self.commit_races()

    def trial(self, mode, concurrent, repetition):
        self.reset()
        load = os.getloadavg()
        started = time.perf_counter()
        def work(session):
            body, body_seconds = session.finish(session.start(self.body(mode)))
            _, commit_seconds = session.finish(session.start('COMMIT;'))
            return {'body_seconds': body_seconds, 'commit_seconds': commit_seconds,
                    'parent_delete_count': self.deletes(body)}
        if concurrent:
            with concurrent_futures_pool() as pool:
                futures = [pool.submit(work, session) for session in (self.a, self.b)]
                measurements = [f.result() for f in futures]
        else:
            measurements = [work(session) for session in (self.a, self.b)]
        elapsed = time.perf_counter() - started
        self.records.append({'mode': mode, 'concurrent': concurrent, 'repetition': repetition,
            'source_rows_per_session_per_target': self.args.rows, 'source_rows_pair': 4*self.args.rows,
            'pair_wall_seconds': elapsed, 'sessions': measurements, 'load_average': load,
            'integrity': self.integrity()})
        print(json.dumps(self.records[-1]), flush=True)

    def cleanup(self):
        # Cancel only our own recorded backends, then close workers. Killing a
        # psql client alone need not promptly interrupt a sleeping backend.
        self.admin.timeout = 5
        self.admin.run("SET statement_timeout = '5s'; SET lock_timeout = '2s';")
        self.admin.run(f'SELECT pg_cancel_backend(pid) FROM pg_stat_activity '
                       f'WHERE pid IN ({self.a.pid},{self.b.pid});')
        for session in (self.a, self.b):
            session.close()
        self.admin.run(f'DROP SCHEMA IF EXISTS {self.schema} CASCADE;')


# Separate name avoids shadowing the concurrent module with the trial flag.
def concurrent_futures_pool():
    return concurrent.futures.ThreadPoolExecutor(max_workers=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True)
    parser.add_argument('--rows', type=int, default=20)
    parser.add_argument('--rounds', type=int, default=1)
    parser.add_argument('--performance', action='store_true')
    parser.add_argument('--skip-checks', action='store_true', help='skip prototype-only correctness checks for a baseline DB')
    parser.add_argument('--modes', nargs='+', choices=['toggle', 'defer', 'enabled'],
                        default=['toggle', 'defer', 'enabled'])
    parser.add_argument('--timeout', type=int, default=180)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r'(?:sql_saga_defer|saga_defer)[A-Za-z0-9_]*', args.database):
        parser.error('database must be an explicitly named disposable sql_saga_defer* or saga_defer* database')
    if min(args.rows, args.rounds, args.timeout) < 1:
        parser.error('rows, rounds and timeout must be positive')
    directory = args.output or Path(__file__).resolve().parents[1] / 'tmp' / ('defer_checks_' + time.strftime('%Y%m%d_%H%M%S') + '_' + uuid.uuid4().hex[:6])
    directory.mkdir(parents=True, exist_ok=False)
    print('Logs and reports:', directory, flush=True)
    experiment = None
    report = {'arguments': vars(args) | {'output': str(directory)},
              'machine': platform.platform(), 'cpu_count': os.cpu_count(),
              'timing': 'setup excluded, client-observed body/commit and whole pair wall time, no performance barriers'}
    try:
        experiment = Experiment(args, directory)
        experiment.setup()
        if not args.skip_checks:
            experiment.correctness()
        else:
            experiment.evidence['correctness_checks'] = 'explicitly skipped'
        if args.performance:
            for repetition in range(args.rounds):
                modes = args.modes
                modes = modes[repetition % len(modes):] + modes[:repetition % len(modes)]
                for parallel in (False, True):
                    for mode in modes:
                        experiment.trial(mode, parallel, repetition + 1)
        report['status'] = 'passed'
    except BaseException as error:
        report['status'] = 'failed'
        report['error'] = repr(error)
        raise
    finally:
        if experiment:
            report['schema'] = experiment.schema
            report['evidence'] = experiment.evidence
            report['trials'] = experiment.records
            try:
                experiment.cleanup()
            except Exception as error:
                report['cleanup_error'] = repr(error)
                report['status'] = 'failed'
            for session in experiment.sessions:
                try:
                    session.close()
                except Exception as error:
                    report.setdefault('close_errors', []).append(repr(error))
                    report['status'] = 'failed'
        (directory / 'report.json').write_text(json.dumps(report, indent=2, default=str) + '\n')
        with (directory / 'trials.tsv').open('w') as file:
            columns = ['mode','concurrent','repetition','source_rows_per_session_per_target',
                       'pair_wall_seconds','session','body_seconds','commit_seconds','parent_delete_count','load_average']
            writer = csv.DictWriter(file, fieldnames=columns, delimiter='\t')
            writer.writeheader()
            for trial in report.get('trials', []):
                for number, measurement in enumerate(trial['sessions'], 1):
                    writer.writerow({key: value for key, value in (trial | measurement | {'session': number}).items() if key in columns})
    if report['status'] != 'passed':
        raise RuntimeError('Experiment cleanup failed, see report.json')
    print('PASS:', directory / 'report.json')


if __name__ == '__main__':
    main()
