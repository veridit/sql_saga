#!/usr/bin/env python3
"""Generate the isolationtester specs of the FOR PORTION OF revert study (doc/upstream/for-portion-of-revert.md).

Each scenario runs temporal_merge (or the for_portion_of view, which calls it) against one other session.
Every scenario is written twice:
  <name>.spec         sql_saga 1.14 as released
  <name>_locked.spec  the same, with the prototype protocol before each temporal write: an advisory lock on the
                      entity and SELECT ... FOR UPDATE of its rows (bug 7rzgv5)
Run with run.sh. Specs avoid braces inside SQL because the spec parser ends a step at the first '}'.
"""
import pathlib

HERE = pathlib.Path(__file__).resolve().parent

SETUP = """setup
{
  DROP TABLE IF EXISTS t CASCADE;
  CREATE TABLE t (id int NOT NULL, price int NOT NULL, note text NOT NULL,
                  valid_range daterange NOT NULL, valid_from date, valid_until date);
  SELECT sql_saga.add_era('t', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
  SELECT sql_saga.add_unique_key('t', ARRAY['id'], key_type => 'primary');
  SELECT sql_saga.add_for_portion_of_view('t');
  INSERT INTO t (id, price, note, valid_from, valid_until) VALUES (1, 0, 'orig', '2020-01-01', '2030-01-01');
}
teardown
{
  SELECT sql_saga.drop_for_portion_of_view('t');
  SELECT sql_saga.drop_unique_key('t', ARRAY['id']);
  SELECT sql_saga.drop_era('t');
  DROP TABLE t;
}
"""

RESULT = "step result { SELECT id, price, note, valid_from, valid_until FROM t ORDER BY id, valid_from; }"


def lock(entity, locked):
    if not locked:
        return ""
    return (f"SELECT pg_advisory_xact_lock(hashtext('t'), {entity});\n"
            f"  SELECT count(*) FROM (SELECT FROM t WHERE id = {entity} FOR UPDATE) AS l;\n  ")


def merge(name, rows, mode, locked, entity=1, cols="row_id int, id int, price int, valid_from date, valid_until date"):
    return f"""step {name}
{{
  CREATE TEMP TABLE src_{name} ({cols}) ON COMMIT DROP;
  INSERT INTO src_{name} VALUES {", ".join(rows)};
  {lock(entity, locked)}CALL sql_saga.temporal_merge(target_table => 't', source_table => 'src_{name}', primary_identity_columns => ARRAY['id'], mode => '{mode}');
  SELECT status, error_message FROM pg_temp.temporal_merge_feedback;
}}"""


def session(name, isolation, steps):
    return f"session {name}\nsetup {{ BEGIN ISOLATION LEVEL {isolation}; }}\n" + "\n".join(steps) + f"\nstep {name}c {{ COMMIT; }}\n"


def scenarios(locked):
    s = {}
    for iso, tag in (("READ COMMITTED", "rc"), ("REPEATABLE READ", "rr")):
        if locked and tag == "rr":
            continue
        s[f"a_disjoint_updates_{tag}"] = (
            "# Two UPDATE_FOR_PORTION_OF on disjoint slices of one row (PostgreSQL's own documented case).\n"
            "# Serial result: 2022 price 1, 2025 price 2, the rest price 0.\n" + SETUP
            + session("s1", iso, [merge("s1m", ["(1, 1, 1, '2022-01-01', '2023-01-01')"], "UPDATE_FOR_PORTION_OF", locked)])
            + session("s2", iso, [merge("s2m", ["(1, 1, 2, '2025-01-01', '2026-01-01')"], "UPDATE_FOR_PORTION_OF", locked), RESULT])
            + "permutation s1m s2m s1c s2c result\n")
        s[f"b_note_vs_portion_{tag}"] = (
            "# A plain UPDATE of another column races a portion update of price.\n"
            "# Serial result: note 's1' on every piece, 2025 price 2.\n" + SETUP
            + session("s1", iso, ["step s1u { UPDATE t SET note = 's1' WHERE id = 1; }"])
            + session("s2", iso, [merge("s2m", ["(1, 1, 2, '2025-01-01', '2026-01-01')"], "UPDATE_FOR_PORTION_OF", locked), RESULT])
            + "permutation s1u s2m s1c s2c result\n"
            + "permutation s2m s1u s2c s1c result\n")
        s[f"c_delete_all_vs_portion_{tag}"] = (
            "# One session deletes the entity, the other updates a slice of it.\n"
            "# Serial result: no rows, in either order.\n" + SETUP
            + session("s1", iso, ["step s1d { DELETE FROM t WHERE id = 1; }"])
            + session("s2", iso, [merge("s2m", ["(1, 1, 2, '2025-01-01', '2026-01-01')"], "UPDATE_FOR_PORTION_OF", locked), RESULT])
            + "permutation s1d s2m s1c s2c result\n")
    # With the locks, s3 waits for s2, so s2 must commit first (the unlocked order would wait forever).
    order = "s1d s2m s1c s3m s2c s3c result" if locked else "s1d s2m s1c s3m s3c s2c result"
    s["d_andres_delete2_rc"] = (
        "# The shape of Andres Freund's for-portion-of-lost-delete2.spec, with temporal_merge:\n"
        "# s1 deletes [2020,2021), s2 and s3 update disjoint slices. Serial result: 2020 gone, 2022 price 3, 2024 price 2.\n"
        + SETUP
        + session("s1", "READ COMMITTED", [merge("s1d", ["(1, 1, '2020-01-01', '2021-01-01')"], "DELETE_FOR_PORTION_OF", locked,
                                                 cols="row_id int, id int, valid_from date, valid_until date")])
        + session("s2", "READ COMMITTED", [merge("s2m", ["(1, 1, 2, '2024-01-01', '2025-01-01')"], "UPDATE_FOR_PORTION_OF", locked)])
        + session("s3", "READ COMMITTED", [merge("s3m", ["(1, 1, 3, '2022-01-01', '2023-01-01')"], "UPDATE_FOR_PORTION_OF", locked), RESULT])
        + f"permutation {order}\n")
    view_lock = lock(1, locked)
    s["e_view_note_vs_portion_rc"] = (
        "# As b, but the portion update goes through the for_portion_of view.\n"
        "# Serial result: note 's1' on every piece, 2025 price 2.\n" + SETUP
        + session("s1", "READ COMMITTED", ["step s1u { UPDATE t SET note = 's1' WHERE id = 1; }"])
        + session("s2", "READ COMMITTED", [
            f"step s2v\n{{\n  {view_lock}UPDATE t__for_portion_of_valid SET price = 2, valid_from = '2025-01-01', valid_until = '2026-01-01' WHERE id = 1;\n}}",
            RESULT])
        + "permutation s1u s2v s1c s2c result\n")
    note_cols = "row_id int, id int, price int, note text, valid_from date, valid_until date"
    s["f_new_entity_race_rc"] = (
        "# Both sessions create entity 2 for overlapping time with MERGE_ENTITY_PATCH.\n"
        "# Serial result: the second patches the first, no overlap, no error.\n" + SETUP
        + session("s1", "READ COMMITTED", [merge("s1m", ["(1, 2, 1, 'n1', '2020-01-01', '2025-01-01')"], "MERGE_ENTITY_PATCH", locked, 2, note_cols)])
        + session("s2", "READ COMMITTED", [merge("s2m", ["(1, 2, 2, 'n2', '2023-01-01', '2028-01-01')"], "MERGE_ENTITY_PATCH", locked, 2, note_cols), RESULT])
        + "permutation s1m s2m s1c s2c result\n")
    return s


if __name__ == "__main__":
    for locked in (False, True):
        for name, text in scenarios(locked).items():
            (HERE / f"{name}{'_locked' if locked else ''}.spec").write_text(text)
    print("\n".join(sorted(p.name for p in HERE.glob("*.spec"))))
