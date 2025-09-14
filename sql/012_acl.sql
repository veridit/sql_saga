\i sql/include/test_setup.sql

/* Tests for access control on the history tables */

CREATE ROLE periods_acl_1;
CREATE ROLE periods_acl_2;

GRANT periods_acl_1 TO sql_saga_unprivileged_user;
GRANT periods_acl_2 TO sql_saga_unprivileged_user;

SET ROLE TO sql_saga_unprivileged_user;

/* OWNER */

-- We call this query several times, so make it a view for eaiser maintenance
CREATE VIEW show_owners AS
    SELECT c.relnamespace::regnamespace AS schema_name,
           c.relname AS object_name,
           CASE c.relkind
               WHEN 'r' THEN 'table'
               WHEN 'v' THEN 'view'
           END AS object_type,
           c.relowner::regrole AS owner
    FROM pg_class AS c
    WHERE c.relnamespace = 'public'::regnamespace
      AND c.relname = ANY (ARRAY['owner_test', 'owner_test_history', 'owner_test_with_history', 'owner_test__for_portion_of_p'])
    UNION ALL
    SELECT p.pronamespace, p.proname, 'function', p.proowner
    FROM pg_proc AS p
    WHERE p.pronamespace = 'public'::regnamespace
      AND p.proname = ANY (ARRAY['owner_test__as_of', 'owner_test__between', 'owner_test__between_symmetric', 'owner_test__from_to']);

CREATE TABLE owner_test (col text PRIMARY KEY, f integer, u integer);
ALTER TABLE owner_test OWNER TO periods_acl_1;
SELECT sql_saga.add_era('owner_test', 'f', 'u', 'p');
SELECT sql_saga.add_for_portion_of_view('owner_test', 'p');
TABLE show_owners ORDER BY object_name;

-- This should change everything
ALTER TABLE owner_test OWNER TO periods_acl_2;
TABLE show_owners ORDER BY object_name;

SELECT sql_saga.drop_for_portion_of_view('owner_test', 'p');
SELECT sql_saga.drop_era('owner_test', 'p');
DROP TABLE owner_test CASCADE;
DROP VIEW show_owners;

/* FOR PORTION OF ACL */

-- We call this query several times, so make it a view for eaiser maintenance
CREATE VIEW show_acls AS
    SELECT row_number() OVER (ORDER BY array_position(ARRAY['table', 'view', 'function'], object_type),
                                       schema_name, object_name, grantee, privilege_type) AS sort_order,
           *
    FROM (
        SELECT c.relnamespace::regnamespace AS schema_name,
               c.relname AS object_name,
               CASE c.relkind
                   WHEN 'r' THEN 'table'
                   WHEN 'v' THEN 'view'
               END AS object_type,
               acl.grantee::regrole::text AS grantee,
               acl.privilege_type
        FROM pg_class AS c
        CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
        WHERE c.relname IN ('fpacl', 'fpacl__for_portion_of_p')
    ) AS _;

CREATE TABLE fpacl (col text PRIMARY KEY, f integer, u integer);
ALTER TABLE fpacl OWNER TO periods_acl_1;
SELECT sql_saga.add_era('fpacl', 'f', 'u', 'p');
SELECT sql_saga.add_for_portion_of_view('fpacl', 'p');
TABLE show_acls ORDER BY sort_order;

GRANT SELECT, UPDATE ON TABLE fpacl__for_portion_of_p TO periods_acl_2; -- fail
GRANT SELECT, UPDATE ON TABLE fpacl TO periods_acl_2;
TABLE show_acls ORDER BY sort_order;

REVOKE UPDATE ON TABLE fpacl__for_portion_of_p FROM periods_acl_2; -- fail
REVOKE UPDATE ON TABLE fpacl FROM periods_acl_2;
TABLE show_acls ORDER BY sort_order;

SELECT sql_saga.drop_for_portion_of_view('fpacl', 'p');
SELECT sql_saga.drop_era('fpacl', 'p');
DROP TABLE fpacl CASCADE;
DROP VIEW show_acls;

/* Clean up */

RESET ROLE;
DROP ROLE periods_acl_1;
DROP ROLE periods_acl_2;

\i sql/include/test_teardown.sql
