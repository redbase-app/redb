-- =====================================================================
-- 99_smoke_auto.sql
-- ---------------------------------------------------------------------
-- Automated PVT smoke runner. Iterates a static table of (scheme_alias,
-- label, filter_json) cases, builds the SQL via pvt_build_query_sql
-- and EXECUTEs it inside a count(*) wrapper. Reports OK/FAIL per case
-- through RAISE NOTICE, raises at the end if any case failed.
--
-- Designed against the redb.Examples model corpus (Employee, Person,
-- City, ProjectMetrics, Department). Schemes are looked up by
-- _schemes._alias first, then by suffix match on _schemes._name.
-- Missing schemes are reported as SKIP and do not fail the run.
--
-- Usage (from psql):
--   \i redb.Postgres/sql/v2-pvt/99_smoke_auto.sql
-- =====================================================================

DO $SMOKE$
DECLARE
    v_scheme_name text;
    v_scheme_id   bigint;
    v_label       text;
    v_filter      jsonb;
    v_order       jsonb;
    v_sql         text;
    v_count       bigint;
    v_pass        int := 0;
    v_fail        int := 0;
    v_skip        int := 0;
    v_msg         text;
    v_cases       text[][] := ARRAY[
        -- Employee --------------------------------------------------------
        ['redb.Tests.Integration.Models.EmployeeProps', 'eq string',                  '{"FirstName":{"$eq":"Ivan"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'shorthand eq',               '{"LastName":"Doe"}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'ne string',                  '{"FirstName":{"$ne":"Ivan"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'in string',                  '{"Department":{"$in":["IT","HR"]}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'nin string',                 '{"Department":{"$nin":["Sales"]}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'range int',                  '{"Age":{"$gte":18,"$lte":65}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'numeric gt decimal',         '{"Salary":{"$gt":1000}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'numeric lt decimal',         '{"Salary":{"$lt":1000000}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'numeric range',              '{"Salary":{"$gte":0,"$lte":1000000}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'null check',                 '{"EmployeeCode":{"$null":true}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'notNull check',              '{"EmployeeCode":{"$notNull":true}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', '$exists true',               '{"EmployeeCode":{"$exists":true}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', '$exists false',              '{"EmployeeCode":{"$exists":false}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', '$isNull true',               '{"EmployeeCode":{"$isNull":true}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'startsWith',                 '{"FirstName":{"$startsWith":"A"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'endsWith',                   '{"LastName":{"$endsWith":"ov"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'contains',                   '{"Position":{"$contains":"eng"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'containsIgnoreCase',         '{"Position":{"$containsIgnoreCase":"ENG"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'startsWithIgnoreCase',       '{"FirstName":{"$startsWithIgnoreCase":"a"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'endsWithIgnoreCase',         '{"LastName":{"$endsWithIgnoreCase":"OV"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'like',                       '{"FirstName":{"$like":"A%"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'ilike',                      '{"FirstName":{"$ilike":"a%"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', '$and+$or',                   '{"$and":[{"Age":{"$gt":18}},{"$or":[{"Department":{"$eq":"IT"}},{"Department":{"$eq":"HR"}}]}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', '$or top-level',              '{"$or":[{"Department":"IT"},{"Department":"HR"}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', '$not',                       '{"$not":{"Age":{"$lt":18}}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', '$not + $and',                '{"$not":{"$and":[{"Age":{"$lt":18}},{"Department":"IT"}]}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'multi-field AND merge',      '{"Department":"IT","Age":{"$gt":18}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'date gt',                    '{"HireDate":{"$gt":"2020-01-01T00:00:00Z"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'date lte',                   '{"HireDate":{"$lte":"2030-01-01T00:00:00Z"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'date range',                 '{"HireDate":{"$gte":"2000-01-01T00:00:00Z","$lt":"2030-01-01T00:00:00Z"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'base 0$:Id eq',              '{"0$:Id":{"$gt":0}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'base 0$:Name eq',            '{"0$:Name":{"$startsWith":"E"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'base _id range',             '{"_id":{"$gt":0}}'],
        -- B1 base-pushdown semantics
        ['redb.Tests.Integration.Models.EmployeeProps', 'pushdown _date_create gte',  '{"_date_create":{"$gte":"2000-01-01T00:00:00Z"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'pushdown base+props AND',    '{"_id":{"$gt":0},"FirstName":{"$startsWith":"A"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'pushdown $and explicit',     '{"$and":[{"_id":{"$gt":0}},{"Department":"IT"}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'pushdown $or all-base',      '{"$or":[{"_id":{"$lt":10}},{"_id":{"$gt":100}}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'pushdown $or mixed (stays)', '{"$or":[{"_id":{"$lt":10}},{"FirstName":"Ivan"}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'pushdown $not base',         '{"$not":{"_id":{"$lt":0}}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'pushdown $not mixed (stays)','{"$not":{"$and":[{"_id":{"$gt":0}},{"FirstName":"Ivan"}]}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'pushdown nested $and+$or',   '{"$and":[{"_id":{"$gt":0}},{"$or":[{"Department":"IT"},{"Department":"HR"}]}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'pushdown 0$:Name+props',     '{"0$:Name":{"$startsWith":"E"},"Age":{"$gt":0}}'],
        -- B2 expression engine: arithmetic, functions, expression-form predicates
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr arithmetic gt',         '{"$gt":[{"$add":[{"$field":"Age"},{"$const":1}]},{"$const":18}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr mul base pushdown',     '{"$lt":[{"$mul":[{"$field":"_id"},{"$const":2}]},{"$const":1000000}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr upper eq',              '{"$eq":[{"$upper":[{"$field":"FirstName"}]},{"$const":"IVAN"}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr concat ilike',          '{"$ilike":[{"$concat":[{"$field":"FirstName"},{"$const":" "},{"$field":"LastName"}]},{"$const":"%van%"}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr coalesce eq',           '{"$eq":[{"$coalesce":[{"$field":"Department"},{"$const":"none"}]},{"$const":"IT"}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr between base push',     '{"$between":[{"$field":"_id"},{"$const":0},{"$const":1000000}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr in props',              '{"$in":[{"$field":"Department"},["IT","HR","R&D"]]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr nin base push',         '{"$nin":[{"$field":"_id"},[-1,-2,-3]]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr length gt',             '{"$gt":[{"$length":[{"$field":"FirstName"}]},{"$const":0}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr abs sub',               '{"$lt":[{"$abs":[{"$sub":[{"$field":"Age"},{"$const":30}]}]},{"$const":1000}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr min max',               '{"$lt":[{"$min":[{"$field":"Age"},{"$const":100}]},{"$max":[{"$field":"Age"},{"$const":0}]}]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr null base push',        '{"$null":{"$field":"_id_parent"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr notNull props',         '{"$notNull":{"$field":"FirstName"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr $contains sugar',       '{"$contains":[{"$field":"FirstName"},"a"]}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'expr $startsWith sugar',     '{"$startsWith":[{"$field":"LastName"},"S"]}'],
        -- Arrays + .$length / .$count + array ops
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayContains',       '{"Skills":{"$arrayContains":"C#"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayCount eq',       '{"Skills":{"$arrayCount":3}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayCount 0',        '{"Skills":{"$arrayCount":0}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayCountGt',        '{"Skills":{"$arrayCountGt":2}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayCountGte',       '{"Skills":{"$arrayCountGte":1}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayCountLt',        '{"Skills":{"$arrayCountLt":10}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayCountLte',       '{"Skills":{"$arrayCountLte":5}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayAny true',       '{"Skills":{"$arrayAny":true}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayAny false',      '{"Skills":{"$arrayAny":false}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayEmpty false',    '{"Skills":{"$arrayEmpty":false}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayEmpty true',     '{"Skills":{"$arrayEmpty":true}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayFirst',          '{"Skills":{"$arrayFirst":"C#"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayLast',           '{"Skills":{"$arrayLast":"SQL"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayAt index=1',     '{"Skills":{"$arrayAt":{"index":1,"value":"Python"}}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayAt index=0',     '{"Skills":{"$arrayAt":{"index":0,"value":"C#"}}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayStartsWith',     '{"Skills":{"$arrayStartsWith":"C"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayEndsWith',       '{"Skills":{"$arrayEndsWith":"L"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array $arrayMatches',        '{"Skills":{"$arrayMatches":"%QL%"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array int $arraySum',        '{"SkillLevels":{"$arraySum":10}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array int $arrayMin',        '{"SkillLevels":{"$arrayMin":1}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array int $arrayMax',        '{"SkillLevels":{"$arrayMax":5}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array int $arrayAvg',        '{"SkillLevels":{"$arrayAvg":3}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'array int $arrayCount',      '{"SkillLevels":{"$arrayCount":3}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', '.$length gt',                '{"Skills.$length":{"$gt":0}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', '.$length eq 0',              '{"Skills.$length":{"$eq":0}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', '.$count gte',                '{"Skills.$count":{"$gte":1}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', '.$count int gt',             '{"SkillLevels.$count":{"$gt":0}}'],
        -- Dictionaries
        ['redb.Tests.Integration.Models.EmployeeProps', 'dict ContainsKey simple',    '{"PhoneDirectory.ContainsKey":"work"}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'dict ContainsKey home',      '{"PhoneDirectory.ContainsKey":"home"}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'dict indexer simple',        '{"PhoneDirectory[work]":{"$eq":"+7-000-0000000"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'dict indexer notNull',       '{"PhoneDirectory[work]":{"$notNull":true}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'dict nested .City',          '{"OfficeLocations[Moscow].City":{"$eq":"Moscow"}}'],
        ['redb.Tests.Integration.Models.EmployeeProps', 'dict nested .Street',        '{"OfficeLocations[Moscow].Street":{"$notNull":true}}'],
        -- Person (ListItem accessors)
        ['redb.Tests.Integration.Models.PersonProps',   'eq Name (props)',            '{"Name":{"$eq":"Alice"}}'],
        ['redb.Tests.Integration.Models.PersonProps',   'Age range',                  '{"Age":{"$gte":18,"$lte":99}}'],
        ['redb.Tests.Integration.Models.PersonProps',   'Email contains',             '{"Email":{"$contains":"@"}}'],
        -- NB: shorthand '{"Status":"Active"}' is intentionally NOT covered:
        -- ListItem field stores _listitem (bigint ID). Scalar .Value/.Alias
        -- in WHERE compare as text against the dereferenced
        -- _list_items._value/_alias column (Free PG LINQ passes string
        -- literals; Pro pre-resolves to id on the client side).
        ['redb.Tests.Integration.Models.PersonProps',   'ListItem Status.Value eq',   '{"Status.Value":{"$eq":"Active"}}'],
        ['redb.Tests.Integration.Models.PersonProps',   'ListItem Status.Alias eq',   '{"Status.Alias":{"$eq":"active"}}'],
        ['redb.Tests.Integration.Models.PersonProps',   'ListItem Status.Id',         '{"Status.Id":{"$gt":0}}'],
        ['redb.Tests.Integration.Models.PersonProps',   'ListItem Roles[].Value in',  '{"Roles[].Value":{"$in":["admin","user"]}}'],
        ['redb.Tests.Integration.Models.PersonProps',   'ListItem Roles[].Value nin', '{"Roles[].Value":{"$nin":["guest"]}}'],
        -- ProjectMetrics
        ['redb.Tests.Integration.Models.ProjectMetricsProps', 'numeric range Budget', '{"Budget":{"$gte":0}}'],
        ['redb.Tests.Integration.Models.ProjectMetricsProps', 'TasksCompleted gte',   '{"TasksCompleted":{"$gte":0}}'],
        ['redb.Tests.Integration.Models.ProjectMetricsProps', 'BugsFixed gte',        '{"BugsFixed":{"$gte":0}}'],
        ['redb.Tests.Integration.Models.ProjectMetricsProps', 'TeamSize range',       '{"TeamSize":{"$gte":1,"$lte":1000}}'],
        ['redb.Tests.Integration.Models.ProjectMetricsProps', 'tech $arrayContains',  '{"Technologies":{"$arrayContains":"PostgreSQL"}}'],
        ['redb.Tests.Integration.Models.ProjectMetricsProps', 'tech $arrayAny',       '{"Technologies":{"$arrayAny":true}}'],
        ['redb.Tests.Integration.Models.ProjectMetricsProps', 'tech .$length gt',     '{"Technologies.$length":{"$gt":0}}'],
        ['redb.Tests.Integration.Models.ProjectMetricsProps', 'tech $arrayStartsWith','{"Technologies":{"$arrayStartsWith":"P"}}'],
        ['redb.Tests.Integration.Models.ProjectMetricsProps', 'ProjectId gt',         '{"ProjectId":{"$gt":0}}'],
        -- City
        ['redb.Tests.Integration.Models.CityProps', 'bool true',                      '{"IsCapital":{"$eq":true}}'],
        ['redb.Tests.Integration.Models.CityProps', 'bool false',                     '{"IsCapital":{"$eq":false}}'],
        ['redb.Tests.Integration.Models.CityProps', 'coords $arrayCount',             '{"Coordinates":{"$arrayCount":2}}'],
        ['redb.Tests.Integration.Models.CityProps', 'coords $arrayAt 0',              '{"Coordinates":{"$arrayAt":{"index":0,"value":55.7558}}}'],
        ['redb.Tests.Integration.Models.CityProps', 'coords .$length eq',             '{"Coordinates.$length":{"$eq":2}}'],
        ['redb.Tests.Integration.Models.CityProps', 'population gt',                  '{"Population":{"$gt":1000}}'],
        ['redb.Tests.Integration.Models.CityProps', 'population range',               '{"Population":{"$gte":0,"$lt":50000000}}'],
        ['redb.Tests.Integration.Models.CityProps', 'region startsWith',              '{"Region":{"$startsWith":"M"}}'],
        ['redb.Tests.Integration.Models.CityProps', 'name in',                        '{"Name":{"$in":["Moscow","Paris"]}}'],
        -- Department (tree-style scheme)
        ['redb.Tests.Integration.Models.DepartmentProps', 'eq bool',                  '{"IsActive":{"$eq":true}}'],
        ['redb.Tests.Integration.Models.DepartmentProps', 'name startsWith',          '{"Name":{"$startsWith":"IT"}}'],
        ['redb.Tests.Integration.Models.DepartmentProps', 'name like',                '{"Name":{"$like":"%"}}'],
        ['redb.Tests.Integration.Models.DepartmentProps', 'code contains',            '{"Code":{"$contains":"-"}}'],
        ['redb.Tests.Integration.Models.DepartmentProps', 'budget range',             '{"Budget":{"$gte":0}}'],
        ['redb.Tests.Integration.Models.DepartmentProps', 'description notNull',      '{"Description":{"$notNull":true}}']
    ];
    i int;
    v_total int;
    v_dump  boolean := COALESCE(current_setting('pvt.dump_sql', true), '0') IN ('1','on','true','TRUE');
BEGIN
    RAISE NOTICE '----------------------------------------------------------';
    RAISE NOTICE 'pvt smoke run starting, module version: %', pvt_module_version();
    IF v_dump THEN
        RAISE NOTICE 'dump_sql mode is ON (pvt.dump_sql) -- SQL for every case will be printed';
    END IF;
    RAISE NOTICE '----------------------------------------------------------';

    v_total := COALESCE(array_length(v_cases, 1), 0);
    FOR i IN 1..v_total LOOP
        v_scheme_name := v_cases[i][1];
        v_label       := v_cases[i][2];

        -- Each filter is a JSON literal; parse defensively.
        BEGIN
            v_filter := v_cases[i][3]::jsonb;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [% / %] bad filter JSON: %', v_scheme_name, v_label, v_msg;
            CONTINUE;
        END;

        -- Scheme lookup: exact match on _name (full CLR type name, as stored by C# InitializeAsync).
        SELECT _id INTO v_scheme_id
          FROM _schemes
         WHERE _name = v_scheme_name
         LIMIT 1;

        IF v_scheme_id IS NULL THEN
            v_skip := v_skip + 1;
            RAISE NOTICE 'SKIP [% / %] scheme not found', v_scheme_name, v_label;
            CONTINUE;
        END IF;

        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => v_filter);
            IF v_dump THEN
                RAISE NOTICE E'----- DUMP [% / %] -----\nFILTER: %\nSQL:\n%',
                    v_scheme_name, v_label, v_filter::text, v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [% / %] rows=%', v_scheme_name, v_label, v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [% / %] %', v_scheme_name, v_label, v_msg;
        END;
    END LOOP;

    -- Order / paging / tree shape smoke -----------------------------------
    -- (Run only if Employee scheme exists so we know we are connected to
    -- a database with the example data layout.)
    SELECT _id INTO v_scheme_id
      FROM _schemes
     WHERE _name = 'redb.Tests.Integration.Models.EmployeeProps'
     LIMIT 1;

    IF v_scheme_id IS NOT NULL THEN
        v_order := '[{"field":"Age","dir":"desc","nulls":"last"}]'::jsonb;
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => NULL,
                p_order     => v_order,
                p_limit     => 10,
                p_offset    => 0);
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / order+limit] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / order+limit] %', v_msg;
        END;

        v_order := '[{"field":"LastName","dir":"asc","nulls":"first"},{"field":"FirstName","dir":"asc"}]'::jsonb;
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => NULL,
                p_order     => v_order,
                p_limit     => 25,
                p_offset    => 5);
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / order multi+offset] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / order multi+offset] %', v_msg;
        END;

        v_order := '[{"field":"0$:Id","dir":"desc"}]'::jsonb;
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => '{"Age":{"$gte":18}}'::jsonb,
                p_order     => v_order,
                p_limit     => 5);
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / filter+order by base Id] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / filter+order by base Id] %', v_msg;
        END;

        -- Pro-parity ORDER BY $expr: arithmetic on a pivot prop.
        v_order := '[{"$expr":{"$mul":[{"$field":"Age"},{"$const":2}]},"dir":"desc"}]'::jsonb;
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => NULL,
                p_order     => v_order,
                p_limit     => 5);
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            IF v_sql !~* 'ORDER BY \(.*Age.* \* 2\)' THEN
                RAISE EXCEPTION 'expected ORDER BY (Age * 2) in SQL, got: %', v_sql;
            END IF;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / order $expr arith Age*2 desc] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / order $expr arith Age*2 desc] %', v_msg;
        END;

        -- Pro-parity ORDER BY $expr: function call on a pivot prop.
        v_order := '[{"$expr":{"$upper":{"$field":"FirstName"}},"dir":"asc","nulls":"last"}]'::jsonb;
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => NULL,
                p_order     => v_order,
                p_limit     => 5);
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            IF v_sql !~* 'ORDER BY UPPER\(.*FirstName.*\) ASC NULLS LAST' THEN
                RAISE EXCEPTION 'expected ORDER BY UPPER(FirstName) ASC NULLS LAST in SQL, got: %', v_sql;
            END IF;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / order $expr UPPER(FirstName)] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / order $expr UPPER(FirstName)] %', v_msg;
        END;

        -- Pro-parity ORDER BY $expr: mixed function + arithmetic on base column.
        v_order := '[{"$expr":{"$add":[{"$length":{"$field":"_name"}},{"$const":1}]},"dir":"desc"}]'::jsonb;
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => NULL,
                p_order     => v_order,
                p_limit     => 5);
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            IF v_sql !~* 'ORDER BY \(LENGTH\(.*_name.*\) \+ 1\) DESC' THEN
                RAISE EXCEPTION 'expected ORDER BY (LENGTH(_name) + 1) DESC in SQL, got: %', v_sql;
            END IF;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / order $expr LENGTH(_name)+1 desc] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / order $expr LENGTH(_name)+1 desc] %', v_msg;
        END;

        -- Pro-parity DISTINCT: plain dedup over an Employee subset.
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => '{"Age":{"$gte":18}}'::jsonb,
                p_distinct  => true,
                p_limit     => 5);
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            IF v_sql !~* 'SELECT DISTINCT ' THEN
                RAISE EXCEPTION 'expected SELECT DISTINCT in SQL, got: %', v_sql;
            END IF;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / DISTINCT] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / DISTINCT] %', v_msg;
        END;

        -- Pro-parity DISTINCT ON: dedup by a pivot field, with ORDER BY user keys.
        v_order := '[{"field":"_id","dir":"desc"}]'::jsonb;
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id   => v_scheme_id,
                p_filter      => NULL,
                p_order       => v_order,
                p_limit       => 10,
                p_distinct_on => '[{"field":"Age"}]'::jsonb);
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            IF v_sql !~* 'DISTINCT ON \(.*"Age".*\)' THEN
                RAISE EXCEPTION 'expected DISTINCT ON ("Age") in SQL, got: %', v_sql;
            END IF;
            IF v_sql !~* 'ORDER BY .*"Age" ASC, .*o\._id DESC' THEN
                RAISE EXCEPTION 'expected DISTINCT-ON key auto-prepended ASC in ORDER BY, got: %', v_sql;
            END IF;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / DISTINCT ON (Age) + order _id desc] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / DISTINCT ON (Age) + order _id desc] %', v_msg;
        END;

        -- Pro-parity DISTINCT ON $expr: dedup by UPPER(FirstName).
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id   => v_scheme_id,
                p_filter      => NULL,
                p_order       => NULL,
                p_limit       => 10,
                p_distinct_on => '[{"$expr":{"$upper":{"$field":"FirstName"}}}]'::jsonb);
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            IF v_sql !~* 'DISTINCT ON \(UPPER\(.*"FirstName".*\)\)' THEN
                RAISE EXCEPTION 'expected DISTINCT ON (UPPER("FirstName")) in SQL, got: %', v_sql;
            END IF;
            IF v_sql !~* 'ORDER BY UPPER\(.*"FirstName".*\) ASC' THEN
                RAISE EXCEPTION 'expected DISTINCT-ON $expr auto-prepended ASC in ORDER BY, got: %', v_sql;
            END IF;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / DISTINCT ON $expr UPPER(FirstName)] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / DISTINCT ON $expr UPPER(FirstName)] %', v_msg;
        END;

        -- Pro-parity: DISTINCT + DISTINCT ON together must raise.
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id   => v_scheme_id,
                p_distinct    => true,
                p_distinct_on => '[{"field":"Age"}]'::jsonb);
            v_fail := v_fail + 1;
            RAISE NOTICE 'FAIL [Employee / DISTINCT + DISTINCT ON guard] expected exception, got SQL: %', v_sql;
        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            IF v_msg ~* 'mutually exclusive' THEN
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Employee / DISTINCT + DISTINCT ON guard] raised: %', v_msg;
            ELSE
                v_fail := v_fail + 1;
                RAISE NOTICE 'FAIL [Employee / DISTINCT + DISTINCT ON guard] wrong exception: %', v_msg;
            END IF;
        END;
    END IF;

    -- Tree-mode smoke on Department --------------------------------------
    SELECT _id INTO v_scheme_id
      FROM _schemes
     WHERE _name = 'redb.Tests.Integration.Models.DepartmentProps'
     LIMIT 1;

    IF v_scheme_id IS NOT NULL THEN
        DECLARE
            v_root bigint;
        BEGIN
            SELECT _id INTO v_root
              FROM _objects
             WHERE _id_scheme = v_scheme_id AND _id_parent IS NULL
             LIMIT 1;
            -- Fallback: when no Department objects exist, seed=-1 exercises all
            -- tree-mode SQL generators; all queries return 0 rows, no errors.
            v_root := COALESCE(v_root, -1);
                v_sql := pvt_build_query_sql(
                    p_scheme_id   => v_scheme_id,
                    p_filter      => NULL,
                    p_source_mode => 'tree',
                    p_tree_ids    => ARRAY[v_root]::bigint[],
                    p_max_depth   => 5);
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Department / tree depth=5] rows=%', v_count;

                v_sql := pvt_build_query_sql(
                    p_scheme_id   => v_scheme_id,
                    p_filter      => NULL,
                    p_source_mode => 'tree',
                    p_tree_ids    => ARRAY[v_root]::bigint[],
                    p_max_depth   => 1);
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Department / tree depth=1] rows=%', v_count;

                v_sql := pvt_build_query_sql(
                    p_scheme_id   => v_scheme_id,
                    p_filter      => '{"IsActive":{"$eq":true}}'::jsonb,
                    p_source_mode => 'tree',
                    p_tree_ids    => ARRAY[v_root]::bigint[],
                    p_max_depth   => 10);
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Department / tree + filter] rows=%', v_count;

                -- New tree-modes coverage (Pro parity bite) ----------
                v_sql := pvt_build_query_sql(
                    p_scheme_id   => v_scheme_id,
                    p_filter      => NULL,
                    p_source_mode => 'tree_descendants',
                    p_tree_ids    => ARRAY[v_root]::bigint[],
                    p_include_seed=> false);
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Department / tree_descendants exclude seed] rows=%', v_count;

                v_sql := pvt_build_query_sql(
                    p_scheme_id   => v_scheme_id,
                    p_filter      => NULL,
                    p_source_mode => 'tree_children',
                    p_tree_ids    => ARRAY[v_root]::bigint[]);
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Department / tree_children] rows=%', v_count;

                v_sql := pvt_build_query_sql(
                    p_scheme_id   => v_scheme_id,
                    p_filter      => NULL,
                    p_source_mode => 'tree_roots');
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Department / tree_roots] rows=%', v_count;

                v_sql := pvt_build_query_sql(
                    p_scheme_id   => v_scheme_id,
                    p_filter      => NULL,
                    p_source_mode => 'tree_leaves');
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Department / tree_leaves] rows=%', v_count;

                -- tree_ancestors only makes sense from a non-root seed.
                DECLARE
                    v_leaf_id bigint;
                BEGIN
                    SELECT _id INTO v_leaf_id
                      FROM _objects
                     WHERE _id_scheme = v_scheme_id AND _id_parent IS NOT NULL
                     LIMIT 1;
                    IF v_leaf_id IS NOT NULL THEN
                        v_sql := pvt_build_query_sql(
                            p_scheme_id   => v_scheme_id,
                            p_filter      => NULL,
                            p_source_mode => 'tree_ancestors',
                            p_tree_ids    => ARRAY[v_leaf_id]::bigint[]);
                        EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                        v_pass := v_pass + 1;
                        RAISE NOTICE 'OK   [Department / tree_ancestors from non-root] rows=%', v_count;
                    ELSE
                        v_skip := v_skip + 1;
                        RAISE NOTICE 'SKIP [Department / tree_ancestors] no non-root object found';
                    END IF;
                END;

                -- Hierarchical DSL operators (legacy free parity) ----
                v_sql := pvt_build_query_sql(
                    p_scheme_id => v_scheme_id,
                    p_filter    => '{"$isRoot":true}'::jsonb);
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Department / $isRoot] rows=%', v_count;

                v_sql := pvt_build_query_sql(
                    p_scheme_id => v_scheme_id,
                    p_filter    => '{"$isLeaf":true}'::jsonb);
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Department / $isLeaf] rows=%', v_count;

                v_sql := pvt_build_query_sql(
                    p_scheme_id => v_scheme_id,
                    p_filter    => ('{"$childrenOf":' || v_root::text || '}')::jsonb);
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Department / $childrenOf root] rows=%', v_count;

                v_sql := pvt_build_query_sql(
                    p_scheme_id => v_scheme_id,
                    p_filter    => '{"$level":{"$gte":0}}'::jsonb);
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Department / $level>=0] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Department / tree depth=5] %', v_msg;
        END;
    END IF;

    -- ===============================================================
    -- Extended scalar ops on Employee (Pro-parity + free extras).
    -- ===============================================================
    SELECT _id INTO v_scheme_id
      FROM _schemes
     WHERE _name = 'redb.Tests.Integration.Models.EmployeeProps'
     LIMIT 1;

    IF v_scheme_id IS NOT NULL THEN
        -- $year over a base date column.
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => '{"$gte":[{"$year":{"$field":"HireDate"}},{"$const":2000}]}'::jsonb,
                p_limit     => 5);
            IF v_sql !~* 'EXTRACT\(YEAR FROM' THEN
                RAISE EXCEPTION 'expected EXTRACT(YEAR FROM ...) in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / $year EXTRACT] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / $year EXTRACT] %', v_msg;
        END;

        -- $trimStart over a props string.
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => '{"$eq":[{"$trimStart":{"$field":"FirstName"}},{"$const":"Ivan"}]}'::jsonb,
                p_limit     => 5);
            IF v_sql !~* 'LTRIM\(' THEN
                RAISE EXCEPTION 'expected LTRIM(...) in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / $trimStart LTRIM] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / $trimStart LTRIM] %', v_msg;
        END;

        -- $substring (3-arg).
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => '{"$eq":[{"$substring":[{"$field":"FirstName"},{"$const":1},{"$const":3}]},{"$const":"Iva"}]}'::jsonb,
                p_limit     => 5);
            IF v_sql !~* 'SUBSTRING\(.+ FROM .+ FOR ' THEN
                RAISE EXCEPTION 'expected SUBSTRING(... FROM ... FOR ...) in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / $substring] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / $substring] %', v_msg;
        END;

        -- $replace.
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => '{"$eq":[{"$replace":[{"$field":"FirstName"},{"$const":"a"},{"$const":"A"}]},{"$const":"IvAn"}]}'::jsonb,
                p_limit     => 5);
            IF v_sql !~* 'REPLACE\(' THEN
                RAISE EXCEPTION 'expected REPLACE(...) in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / $replace] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / $replace] %', v_msg;
        END;

        -- $power.
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => '{"$lt":[{"$power":[{"$field":"Age"},{"$const":2}]},{"$const":1000000}]}'::jsonb,
                p_limit     => 5);
            IF v_sql !~* 'POWER\(' THEN
                RAISE EXCEPTION 'expected POWER(...) in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / $power] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / $power] %', v_msg;
        END;

        -- $dateAdd: HireDate + 30 days.
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => '{"$gt":[{"$dateAdd":["day",{"$field":"HireDate"},{"$const":30}]},{"$const":"2000-01-01T00:00:00Z"}]}'::jsonb,
                p_limit     => 5);
            IF v_sql !~* 'INTERVAL ''1 day' THEN
                RAISE EXCEPTION 'expected INTERVAL ''1 day'' in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / $dateAdd day] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / $dateAdd day] %', v_msg;
        END;

        -- $if conditional: salary >= 100000 then "High" else "Normal".
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => '{"$eq":[{"$if":[{"$gte":[{"$field":"Salary"},{"$const":100000}]},{"$const":"High"},{"$const":"Normal"}]},{"$const":"Normal"}]}'::jsonb,
                p_limit     => 5);
            IF v_sql !~* '\(CASE WHEN ' THEN
                RAISE EXCEPTION 'expected (CASE WHEN ... in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / $if] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / $if] %', v_msg;
        END;

        -- $case: bucket Age into groups.
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => '{"$eq":[{"$case":[{"when":{"$lt":[{"$field":"Age"},{"$const":18}]},"then":{"$const":"minor"}},{"when":{"$lt":[{"$field":"Age"},{"$const":65}]},"then":{"$const":"adult"}},{"else":{"$const":"senior"}}]},{"$const":"adult"}]}'::jsonb,
                p_limit     => 5);
            IF v_sql !~* '\(CASE WHEN .* WHEN .* ELSE' THEN
                RAISE EXCEPTION 'expected multi-arm CASE in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / $case multi-arm] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / $case multi-arm] %', v_msg;
        END;

        -- ===============================================================
        -- Terminal aggregations: pvt_build_aggregate_sql.
        -- ===============================================================

        -- COUNT(*) over filtered set.
        BEGIN
            v_sql := pvt_build_aggregate_sql(
                p_scheme_id    => v_scheme_id,
                p_filter       => '{"Age":{"$gte":18}}'::jsonb,
                p_aggregations => '[{"alias":"total","$count":"*"}]'::jsonb);
            IF v_sql !~* 'COUNT\(\*\) AS "?total"?' THEN
                RAISE EXCEPTION 'expected COUNT(*) AS total in SQL, got: %', v_sql;
            END IF;
            EXECUTE v_sql INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / agg COUNT(*)] total=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / agg COUNT(*)] %', v_msg;
        END;

        -- SUM + AVG + MIN + MAX over Age.
        BEGIN
            v_sql := pvt_build_aggregate_sql(
                p_scheme_id    => v_scheme_id,
                p_filter       => NULL,
                p_aggregations => '[
                    {"alias":"sum_age","$sum":{"$field":"Age"}},
                    {"alias":"avg_age","$avg":{"$field":"Age"}},
                    {"alias":"min_age","$min":{"$field":"Age"}},
                    {"alias":"max_age","$max":{"$field":"Age"}}
                ]'::jsonb);
            IF v_sql !~* 'SUM\(' OR v_sql !~* 'AVG\(' OR v_sql !~* 'MIN\(' OR v_sql !~* 'MAX\(' THEN
                RAISE EXCEPTION 'expected SUM/AVG/MIN/MAX in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT 1 FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / agg SUM+AVG+MIN+MAX over Age]';
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / agg SUM+AVG+MIN+MAX over Age] %', v_msg;
        END;

        -- C# emit shape (TASK-16): batch with Count(*)+Sum+Avg+Min+Max +
        -- a filter, mirroring exactly what PostgresQueryProvider.Aggregation
        -- serializes from AggregateRequest[]. Locks the C#/SQL contract.
        BEGIN
            v_sql := pvt_build_aggregate_sql(
                p_scheme_id    => v_scheme_id,
                p_filter       => '{"Position":{"$eq":"Developer"}}'::jsonb,
                p_aggregations => '[
                    {"alias":"_agg_1","$count":"*"},
                    {"alias":"sum_salary","$sum":{"$field":"Salary"}},
                    {"alias":"avg_salary","$avg":{"$field":"Salary"}},
                    {"alias":"min_salary","$min":{"$field":"Salary"}},
                    {"alias":"max_salary","$max":{"$field":"Salary"}}
                ]'::jsonb);
            IF v_sql !~* 'COUNT\(\*\) AS "?_agg_1"?' OR v_sql !~* 'SUM\(' OR v_sql !~* 'AVG\('
               OR v_sql !~* 'MIN\(' OR v_sql !~* 'MAX\(' THEN
                RAISE EXCEPTION 'expected Count+Sum+Avg+Min+Max projection, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT 1 FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / agg C# emit shape batch+filter]';
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / agg C# emit shape batch+filter] %', v_msg;
        END;

        -- COUNT DISTINCT (free extra over Pro).
        BEGIN
            v_sql := pvt_build_aggregate_sql(
                p_scheme_id    => v_scheme_id,
                p_filter       => NULL,
                p_aggregations => '[{"alias":"unique_depts","$count":{"$field":"Department"},"distinct":true}]'::jsonb);
            IF v_sql !~* 'COUNT\(DISTINCT' THEN
                RAISE EXCEPTION 'expected COUNT(DISTINCT ...) in SQL, got: %', v_sql;
            END IF;
            EXECUTE v_sql INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / agg COUNT DISTINCT] unique=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / agg COUNT DISTINCT] %', v_msg;
        END;

        -- COUNT with FILTER clause (free extra: per-aggregate WHERE).
        BEGIN
            v_sql := pvt_build_aggregate_sql(
                p_scheme_id    => v_scheme_id,
                p_filter       => NULL,
                p_aggregations => '[{"alias":"adults","$count":"*","filter":{"$gte":[{"$field":"Age"},{"$const":18}]}}]'::jsonb);
            IF v_sql !~* 'COUNT\(\*\) FILTER \(WHERE' THEN
                RAISE EXCEPTION 'expected COUNT(*) FILTER (WHERE ...) in SQL, got: %', v_sql;
            END IF;
            EXECUTE v_sql INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / agg COUNT FILTER] adults=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / agg COUNT FILTER] %', v_msg;
        END;

        -- Pure-base aggregation: Shape A shortcut (no CTE).
        BEGIN
            v_sql := pvt_build_aggregate_sql(
                p_scheme_id    => v_scheme_id,
                p_filter       => '{"_id":{"$gt":0}}'::jsonb,
                p_aggregations => '[{"alias":"total","$count":"*"}]'::jsonb);
            IF v_sql ~* 'WITH _pvt_cte' THEN
                RAISE EXCEPTION 'expected Shape A (no CTE) for pure-base aggregation, got: %', v_sql;
            END IF;
            EXECUTE v_sql INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / agg Shape A pure-base] total=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / agg Shape A pure-base] %', v_msg;
        END;

        -- ===============================================================
        -- GROUP BY: pvt_build_groupby_sql + HAVING (free over Pro).
        -- ===============================================================

        -- Single key + COUNT.
        BEGIN
            v_sql := pvt_build_groupby_sql(
                p_scheme_id    => v_scheme_id,
                p_filter       => NULL,
                p_group_by     => '[{"field":"Department"}]'::jsonb,
                p_aggregations => '[{"alias":"cnt","$count":"*"}]'::jsonb);
            IF v_sql !~* 'GROUP BY ' THEN
                RAISE EXCEPTION 'expected GROUP BY in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / GROUP BY Department + COUNT] groups=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / GROUP BY Department + COUNT] %', v_msg;
        END;

        -- Multi-key GROUP BY + multiple aggregates + ORDER BY + LIMIT.
        BEGIN
            v_sql := pvt_build_groupby_sql(
                p_scheme_id    => v_scheme_id,
                p_filter       => NULL,
                p_group_by     => '[{"field":"Department"},{"field":"Position"}]'::jsonb,
                p_aggregations => '[
                    {"alias":"cnt","$count":"*"},
                    {"alias":"avg_salary","$avg":{"$field":"Salary"}}
                ]'::jsonb,
                p_order        => '[{"$expr":{"$count":"*"},"dir":"desc"}]'::jsonb,
                p_limit        => 10);
            IF v_sql !~* 'GROUP BY .+, ' THEN
                RAISE EXCEPTION 'expected multi-key GROUP BY in SQL, got: %', v_sql;
            END IF;
            IF v_sql !~* 'ORDER BY COUNT\(\*\) DESC' THEN
                RAISE EXCEPTION 'expected ORDER BY COUNT(*) DESC in SQL, got: %', v_sql;
            END IF;
            IF v_sql !~* 'LIMIT 10' THEN
                RAISE EXCEPTION 'expected LIMIT 10, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / GROUP BY multi + ORDER agg + LIMIT] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / GROUP BY multi + ORDER agg + LIMIT] %', v_msg;
        END;

        -- GROUP BY + HAVING (free over Pro).
        BEGIN
            v_sql := pvt_build_groupby_sql(
                p_scheme_id    => v_scheme_id,
                p_filter       => NULL,
                p_group_by     => '[{"field":"Department"}]'::jsonb,
                p_aggregations => '[{"alias":"cnt","$count":"*"}]'::jsonb,
                p_having       => '{"$gt":[{"$count":"*"},{"$const":1}]}'::jsonb);
            IF v_sql !~* 'HAVING ' THEN
                RAISE EXCEPTION 'expected HAVING in SQL, got: %', v_sql;
            END IF;
            IF v_sql !~* 'HAVING \(COUNT\(\*\) > 1\)' THEN
                RAISE EXCEPTION 'expected HAVING (COUNT(*) > 1) in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / GROUP BY + HAVING COUNT>1] groups=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / GROUP BY + HAVING COUNT>1] %', v_msg;
        END;

        -- GROUP BY $expr key (free: arbitrary expression as group key).
        BEGIN
            v_sql := pvt_build_groupby_sql(
                p_scheme_id    => v_scheme_id,
                p_filter       => NULL,
                p_group_by     => '[{"$expr":{"$year":{"$field":"HireDate"}},"alias":"yr"}]'::jsonb,
                p_aggregations => '[{"alias":"cnt","$count":"*"}]'::jsonb);
            IF v_sql !~* 'EXTRACT\(YEAR FROM .+\)(::integer)? AS "?yr"?' THEN
                RAISE EXCEPTION 'expected EXTRACT(YEAR FROM ...) AS yr in SQL, got: %', v_sql;
            END IF;
            IF v_sql !~* 'GROUP BY EXTRACT\(YEAR FROM' THEN
                RAISE EXCEPTION 'expected GROUP BY EXTRACT(YEAR FROM ...) in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / GROUP BY $expr $year(HireDate)] groups=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / GROUP BY $expr $year(HireDate)] %', v_msg;
        END;

        -- C# emit shape (TASK-17): batch GROUP BY with multi-key and Count+Avg+Sum
        -- mirroring exactly what PostgresQueryProvider.Grouping serializes from
        -- GroupFieldRequest[] + AggregateRequest[]. Locks the C#/SQL contract.
        BEGIN
            v_sql := pvt_build_groupby_sql(
                p_scheme_id    => v_scheme_id,
                p_filter       => '{"Position":{"$eq":"Developer"}}'::jsonb,
                p_group_by     => '[{"field":"Department","alias":"dept"},{"field":"Position","alias":"pos"}]'::jsonb,
                p_aggregations => '[
                    {"alias":"_agg_1","$count":"*"},
                    {"alias":"avg_salary","$avg":{"$field":"Salary"}},
                    {"alias":"sum_salary","$sum":{"$field":"Salary"}}
                ]'::jsonb);
            IF v_sql !~* 'GROUP BY ' THEN
                RAISE EXCEPTION 'expected GROUP BY in SQL, got: %', v_sql;
            END IF;
            IF v_sql !~* 'COUNT\(\*\) AS "?_agg_1"?' THEN
                RAISE EXCEPTION 'expected COUNT(*) AS _agg_1, got: %', v_sql;
            END IF;
            IF v_sql !~* 'AVG\(' OR v_sql !~* 'SUM\(' THEN
                RAISE EXCEPTION 'expected AVG and SUM in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / groupby C# emit shape multi-key+filter] groups=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / groupby C# emit shape multi-key+filter] %', v_msg;
        END;

        -- ===============================================================
        -- Window functions: pvt_build_window_sql.
        -- ===============================================================

        -- ROW_NUMBER OVER (ORDER BY Age DESC).
        BEGIN
            v_sql := pvt_build_window_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => NULL,
                p_select    => '[
                    {"field":"_id"},
                    {"field":"Age"},
                    {"alias":"rn","$expr":{"$over":{"func":"row_number","order_by":[{"field":"Age","dir":"desc"}]}}}
                ]'::jsonb,
                p_limit     => 10);
            IF v_sql !~* 'ROW_NUMBER\(\) OVER \(ORDER BY ' THEN
                RAISE EXCEPTION 'expected ROW_NUMBER() OVER (ORDER BY ...) in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / window ROW_NUMBER ORDER BY Age desc] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / window ROW_NUMBER] %', v_msg;
        END;

        -- SUM(Salary) OVER (PARTITION BY Department).
        BEGIN
            v_sql := pvt_build_window_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => NULL,
                p_select    => '[
                    {"field":"_id"},
                    {"field":"Department"},
                    {"alias":"dept_total","$expr":{"$over":{"func":"sum","args":[{"$field":"Salary"}],"partition_by":[{"field":"Department"}]}}}
                ]'::jsonb,
                p_limit     => 20);
            IF v_sql !~* 'SUM\(.+\) OVER \(PARTITION BY ' THEN
                RAISE EXCEPTION 'expected SUM(...) OVER (PARTITION BY ...) in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / window SUM OVER PARTITION BY Dept] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / window SUM PARTITION BY] %', v_msg;
        END;

        -- LAG(Age, 1, 0) OVER (PARTITION BY Department ORDER BY Age)
        -- + explicit frame ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
        BEGIN
            v_sql := pvt_build_window_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => NULL,
                p_select    => '[
                    {"field":"_id"},
                    {"alias":"prev_age","$expr":{"$over":{
                        "func":"lag",
                        "args":[{"$field":"Age"},{"$const":1},{"$const":0}],
                        "partition_by":[{"field":"Department"}],
                        "order_by":[{"field":"Age","dir":"asc"}]
                    }}},
                    {"alias":"running_sum","$expr":{"$over":{
                        "func":"sum",
                        "args":[{"$field":"Salary"}],
                        "partition_by":[{"field":"Department"}],
                        "order_by":[{"field":"_id","dir":"asc"}],
                        "frame":{"type":"rows","start":"unbounded_preceding","end":"current_row"}
                    }}}
                ]'::jsonb,
                p_limit     => 10);
            IF v_sql !~* 'LAG\(' THEN
                RAISE EXCEPTION 'expected LAG(...) in SQL, got: %', v_sql;
            END IF;
            IF v_sql !~* 'ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW' THEN
                RAISE EXCEPTION 'expected ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / window LAG + SUM with ROWS frame] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / window LAG + SUM frame] %', v_msg;
        END;

        -- NTILE(4) over Age (free: ranking quartiles).
        BEGIN
            v_sql := pvt_build_window_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => NULL,
                p_select    => '[
                    {"field":"_id"},
                    {"alias":"quartile","$expr":{"$over":{"func":"ntile","args":[{"$const":4}],"order_by":[{"field":"Age","dir":"asc"}]}}}
                ]'::jsonb,
                p_limit     => 10);
            IF v_sql !~* 'NTILE\(4\)' THEN
                RAISE EXCEPTION 'expected NTILE(4) in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / window NTILE(4)] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / window NTILE(4)] %', v_msg;
        END;

        -- C# emit shape (TASK-18): combined plain-field selects + SUM OVER
        -- (PARTITION BY ... ORDER BY ... ROWS frame) mirroring exactly what
        -- PostgresQueryProvider.Window serializes from WindowFieldRequest[] +
        -- WindowFuncRequest[] with global partition_by / order_by / frame /
        -- take. Locks the C#/SQL contract.
        BEGIN
            v_sql := pvt_build_window_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => '{"Position":{"$eq":"Developer"}}'::jsonb,
                p_select    => '[
                    {"field":"_id"},
                    {"field":"Department"},
                    {"alias":"running_sum","$expr":{"$over":{
                        "func":"sum",
                        "args":[{"$field":"Salary"}],
                        "partition_by":[{"field":"Department"}],
                        "order_by":[{"field":"Age","dir":"asc"}],
                        "frame":{"type":"rows","start":"unbounded_preceding","end":"current_row"}
                    }}}
                ]'::jsonb,
                p_limit     => 25,
                p_offset    => 0);
            IF v_sql !~* 'SUM\(' THEN
                RAISE EXCEPTION 'expected SUM in SQL, got: %', v_sql;
            END IF;
            IF v_sql !~* 'PARTITION BY ' THEN
                RAISE EXCEPTION 'expected PARTITION BY in SQL, got: %', v_sql;
            END IF;
            IF v_sql !~* 'ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW' THEN
                RAISE EXCEPTION 'expected ROWS frame in SQL, got: %', v_sql;
            END IF;
            IF v_sql !~* 'LIMIT 25' THEN
                RAISE EXCEPTION 'expected LIMIT 25 in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / window C# emit shape SUM OVER + frame + filter] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / window C# emit shape SUM OVER + frame + filter] %', v_msg;
        END;
    END IF;

    -- ===============================================================
    -- Tree-variant of GROUP BY (Department): COUNT per parent.
    -- ===============================================================
    SELECT _id INTO v_scheme_id
      FROM _schemes
     WHERE _name = 'redb.Tests.Integration.Models.DepartmentProps'
     LIMIT 1;
    IF v_scheme_id IS NOT NULL THEN
        DECLARE
            v_root bigint;
        BEGIN
            SELECT _id INTO v_root
              FROM _objects
             WHERE _id_scheme = v_scheme_id AND _id_parent IS NULL
             LIMIT 1;
            -- Fallback: when no Department objects exist, seed=-1 exercises the
            -- groupby SQL generator; query returns 0 rows, no errors.
            v_root := COALESCE(v_root, -1);
                BEGIN
                    v_sql := pvt_build_groupby_sql(
                        p_scheme_id    => v_scheme_id,
                        p_filter       => NULL,
                        p_group_by     => '[{"field":"_id_parent"}]'::jsonb,
                        p_aggregations => '[{"alias":"cnt","$count":"*"}]'::jsonb,
                        p_source_mode  => 'tree_descendants',
                        p_tree_ids     => ARRAY[v_root]::bigint[],
                        p_max_depth    => 5);
                    IF v_sql !~* 'WITH _pvt_tree' AND v_sql !~* 'RECURSIVE' THEN
                        RAISE EXCEPTION 'expected tree-mode CTE in SQL, got: %', v_sql;
                    END IF;
                    EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                    v_pass := v_pass + 1;
                    RAISE NOTICE 'OK   [Department / tree_descendants GROUP BY _id_parent + COUNT] groups=%', v_count;
                EXCEPTION WHEN OTHERS THEN
                    v_fail := v_fail + 1;
                    GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
                    RAISE NOTICE 'FAIL [Department / tree GROUP BY] %', v_msg;
                END;
        END;
    END IF;

    -- ===================================================================
    -- Projections: pvt_build_projection_sql.
    -- ===================================================================
    DECLARE
        v_emp_id bigint;
    BEGIN
        SELECT _id INTO v_emp_id
          FROM _schemes
         WHERE _name = 'redb.Tests.Integration.Models.EmployeeProps'
         LIMIT 1;
        v_scheme_id := v_emp_id;
    END;
    IF v_scheme_id IS NOT NULL THEN

        -- Bare fields.
        BEGIN
            v_sql := pvt_build_projection_sql(
                p_scheme_id  => v_scheme_id,
                p_projection => '[{"field":"FirstName"},{"field":"Age"}]'::jsonb,
                p_limit      => 5);
            IF v_sql !~* 'AS "?FirstName"?' OR v_sql !~* 'AS "?Age"?' THEN
                RAISE EXCEPTION 'expected FirstName/Age aliases, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / projection bare fields] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / projection bare fields] %', v_msg;
        END;

        -- Base + props mixed with explicit aliases.
        BEGIN
            v_sql := pvt_build_projection_sql(
                p_scheme_id  => v_scheme_id,
                p_projection => '[
                    {"field":"_id","alias":"id"},
                    {"field":"FirstName","alias":"name"},
                    {"field":"Salary","alias":"sal"}
                ]'::jsonb,
                p_order      => '[{"field":"Salary","dir":"desc"}]'::jsonb,
                p_limit      => 3);
            IF v_sql !~* 'AS "?id"?' OR v_sql !~* 'AS "?name"?' OR v_sql !~* 'AS "?sal"?' THEN
                RAISE EXCEPTION 'expected id/name/sal aliases, got: %', v_sql;
            END IF;
            IF v_sql !~* 'ORDER BY' THEN
                RAISE EXCEPTION 'expected ORDER BY in SQL, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / projection mixed base+props + order + limit] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / projection mixed base+props + order + limit] %', v_msg;
        END;

        -- $expr projection: computed column $year(HireDate).
        BEGIN
            v_sql := pvt_build_projection_sql(
                p_scheme_id  => v_scheme_id,
                p_projection => '[
                    {"field":"FirstName"},
                    {"alias":"hire_year","$expr":{"$year":{"$field":"HireDate"}}}
                ]'::jsonb,
                p_limit      => 5);
            IF v_sql !~* 'EXTRACT\(YEAR FROM .+\) AS "?hire_year"?' THEN
                RAISE EXCEPTION 'expected EXTRACT(YEAR FROM ...) AS hire_year, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / projection $expr $year] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / projection $expr $year] %', v_msg;
        END;

        -- $expr projection: $case ternary (free over Pro: Pro NotSupported).
        BEGIN
            v_sql := pvt_build_projection_sql(
                p_scheme_id  => v_scheme_id,
                p_projection => '[
                    {"field":"FirstName"},
                    {"alias":"kind","$expr":{"$case":[
                        {"when":{"$gt":[{"$field":"Age"},{"$const":18}]},
                         "then":{"$const":"adult"}},
                        {"else":{"$const":"minor"}}
                    ]}}
                ]'::jsonb,
                p_limit      => 10);
            IF v_sql !~* 'CASE WHEN .+ THEN .+ ELSE .+ END' THEN
                RAISE EXCEPTION 'expected CASE WHEN ... THEN ... ELSE ... END, got: %', v_sql;
            END IF;
            IF v_sql !~* 'AS "?kind"?' THEN
                RAISE EXCEPTION 'expected AS kind, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / projection $expr $case] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / projection $expr $case] %', v_msg;
        END;

        -- $expr projection: $coalesce (free over Pro: Pro NotSupported on ??).
        BEGIN
            v_sql := pvt_build_projection_sql(
                p_scheme_id  => v_scheme_id,
                p_projection => '[
                    {"alias":"display","$expr":{"$coalesce":[{"$field":"FirstName"},{"$const":"n/a"}]}}
                ]'::jsonb,
                p_limit      => 5);
            IF v_sql !~* 'COALESCE\(' THEN
                RAISE EXCEPTION 'expected COALESCE(...), got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / projection $coalesce] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / projection $coalesce] %', v_msg;
        END;

        -- $expr projection: $concat (n-ary).
        BEGIN
            v_sql := pvt_build_projection_sql(
                p_scheme_id  => v_scheme_id,
                p_projection => '[
                    {"alias":"label","$expr":{"$concat":[{"$field":"FirstName"},{"$const":" / "},{"$field":"Department"}]}}
                ]'::jsonb,
                p_limit      => 5);
            IF v_sql !~* ' \|\| ' THEN
                RAISE EXCEPTION 'expected || concat operator, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / projection $concat] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / projection $concat] %', v_msg;
        END;

        -- Computed expression with filter + paging + DISTINCT.
        BEGIN
            v_sql := pvt_build_projection_sql(
                p_scheme_id  => v_scheme_id,
                p_projection => '[{"field":"Department"}]'::jsonb,
                p_filter     => '{"Salary":{"$gt":0}}'::jsonb,
                p_distinct   => true,
                p_limit      => 50);
            IF v_sql !~* 'SELECT DISTINCT' THEN
                RAISE EXCEPTION 'expected SELECT DISTINCT, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / projection DISTINCT + filter] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / projection DISTINCT + filter] %', v_msg;
        END;

        -- DISTINCT ON (Department) ordered by Salary desc.
        BEGIN
            v_sql := pvt_build_projection_sql(
                p_scheme_id   => v_scheme_id,
                p_projection  => '[{"field":"Department"},{"field":"FirstName"},{"field":"Salary"}]'::jsonb,
                p_distinct_on => '[{"field":"Department"}]'::jsonb,
                p_order       => '[{"field":"Salary","dir":"desc"}]'::jsonb);
            IF v_sql !~* 'SELECT DISTINCT ON' THEN
                RAISE EXCEPTION 'expected SELECT DISTINCT ON, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / projection DISTINCT ON Department] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / projection DISTINCT ON Department] %', v_msg;
        END;

        -- Pure-base shortcut (Shape A): only _id requested.
        BEGIN
            v_sql := pvt_build_projection_sql(
                p_scheme_id  => v_scheme_id,
                p_projection => '[{"field":"_id"}]'::jsonb,
                p_limit      => 3);
            IF v_sql ~* '_pvt_cte' THEN
                RAISE EXCEPTION 'expected Shape A (no CTE) for pure-base projection, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / projection Shape A pure-base] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / projection Shape A pure-base] %', v_msg;
        END;

        -- $cast: explicit cast (free-only; Pro has no Convert.To* mapping).
        BEGIN
            v_sql := pvt_build_projection_sql(
                p_scheme_id  => v_scheme_id,
                p_projection => '[
                    {"alias":"age_str","$expr":{"$cast":["text",{"$field":"Age"}]}}
                ]'::jsonb,
                p_limit      => 3);
            IF v_sql !~* '::text' THEN
                RAISE EXCEPTION 'expected ::text cast, got: %', v_sql;
            END IF;
            EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / projection $cast] rows=%', v_count;
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / projection $cast] %', v_msg;
        END;

        -- Duplicate alias must raise.
        BEGIN
            BEGIN
                v_sql := pvt_build_projection_sql(
                    p_scheme_id  => v_scheme_id,
                    p_projection => '[{"field":"FirstName","alias":"x"},{"field":"Age","alias":"x"}]'::jsonb);
                RAISE EXCEPTION 'expected duplicate-alias error, but no exception raised';
            EXCEPTION WHEN OTHERS THEN
                GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
                IF v_msg NOT ILIKE '%duplicate alias%' THEN
                    RAISE EXCEPTION 'unexpected error message: %', v_msg;
                END IF;
            END;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / projection duplicate alias rejected]';
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / projection duplicate alias rejected] %', v_msg;
        END;

        -- Empty projection must raise.
        BEGIN
            BEGIN
                v_sql := pvt_build_projection_sql(
                    p_scheme_id  => v_scheme_id,
                    p_projection => '[]'::jsonb);
                RAISE EXCEPTION 'expected empty-projection error, but no exception raised';
            EXCEPTION WHEN OTHERS THEN
                GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
                IF v_msg NOT ILIKE '%non-empty%' THEN
                    RAISE EXCEPTION 'unexpected error message: %', v_msg;
                END IF;
            END;
            v_pass := v_pass + 1;
            RAISE NOTICE 'OK   [Employee / projection empty array rejected]';
        EXCEPTION WHEN OTHERS THEN
            v_fail := v_fail + 1;
            GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
            RAISE NOTICE 'FAIL [Employee / projection empty array rejected] %', v_msg;
        END;

    END IF;

    -- Tree-mode projection (descendants).
    DECLARE
        v_dep_scheme bigint;
        v_root2      bigint;
    BEGIN
        SELECT _id INTO v_dep_scheme
          FROM _schemes
         WHERE _name = 'redb.Tests.Integration.Models.DepartmentProps'
         LIMIT 1;
        SELECT _id INTO v_root2
          FROM _objects
         WHERE _id_scheme = v_dep_scheme AND _id_parent IS NULL
         LIMIT 1;
        -- Fallback: when no Department objects exist, seed=-1 exercises the
        -- projection SQL generator; query returns 0 rows, no errors.
        v_root2 := COALESCE(v_root2, -1);
        IF v_dep_scheme IS NOT NULL THEN
            BEGIN
                v_sql := pvt_build_projection_sql(
                    p_scheme_id   => v_dep_scheme,
                    p_projection  => '[{"field":"_id"},{"field":"_id_parent"},{"field":"Name"}]'::jsonb,
                    p_source_mode => 'tree_descendants',
                    p_tree_ids    => ARRAY[v_root2]::bigint[],
                    p_max_depth   => 5);
                IF v_sql !~* 'WITH _pvt_tree' AND v_sql !~* 'RECURSIVE' THEN
                    RAISE EXCEPTION 'expected tree-mode CTE in SQL, got: %', v_sql;
                END IF;
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Department / projection tree_descendants] rows=%', v_count;
            EXCEPTION WHEN OTHERS THEN
                v_fail := v_fail + 1;
                GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
                RAISE NOTICE 'FAIL [Department / projection tree_descendants] %', v_msg;
            END;
        END IF;
    END;

    -- ===================================================================
    -- New $expr operators: trig, $log10, $now/$today/$utcNow,
    -- $dateTrunc, $regexReplace and predicate-side $regex/$iregex/$fts.
    -- All free-only (Pro has none of these mapped).
    -- ===================================================================
    DECLARE
        v_emp_id bigint;
    BEGIN
        SELECT _id INTO v_emp_id
          FROM _schemes
         WHERE _name = 'redb.Tests.Integration.Models.EmployeeProps'
         LIMIT 1;
        IF v_emp_id IS NULL THEN
            v_skip := v_skip + 1;
            RAISE NOTICE 'SKIP [Employee / new $expr ops] no Employee scheme';
        ELSE
            -- $sin / $cos / $tan / $log10
            BEGIN
                v_sql := pvt_build_projection_sql(
                    p_scheme_id  => v_emp_id,
                    p_projection => '[
                        {"alias":"s","$expr":{"$sin":{"$const":0}}},
                        {"alias":"c","$expr":{"$cos":{"$const":0}}},
                        {"alias":"t","$expr":{"$tan":{"$const":0}}},
                        {"alias":"l","$expr":{"$log10":{"$const":1000}}}
                    ]'::jsonb,
                    p_limit      => 1);
                IF v_sql !~* 'SIN\(' OR v_sql !~* 'COS\(' OR v_sql !~* 'TAN\(' OR v_sql !~* 'LOG\(' THEN
                    RAISE EXCEPTION 'expected SIN/COS/TAN/LOG, got: %', v_sql;
                END IF;
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Employee / projection $sin $cos $tan $log10] rows=%', v_count;
            EXCEPTION WHEN OTHERS THEN
                v_fail := v_fail + 1;
                GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
                RAISE NOTICE 'FAIL [Employee / projection $sin $cos $tan $log10] %', v_msg;
            END;

            -- $now / $today / $utcNow
            BEGIN
                v_sql := pvt_build_projection_sql(
                    p_scheme_id  => v_emp_id,
                    p_projection => '[
                        {"alias":"now_ts","$expr":{"$now":[]}},
                        {"alias":"today",  "$expr":{"$today":[]}},
                        {"alias":"utc_ts", "$expr":{"$utcNow":[]}}
                    ]'::jsonb,
                    p_limit      => 1);
                IF v_sql !~* 'NOW\(\)' OR v_sql !~* 'CURRENT_DATE' THEN
                    RAISE EXCEPTION 'expected NOW() and CURRENT_DATE, got: %', v_sql;
                END IF;
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Employee / projection $now $today $utcNow] rows=%', v_count;
            EXCEPTION WHEN OTHERS THEN
                v_fail := v_fail + 1;
                GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
                RAISE NOTICE 'FAIL [Employee / projection $now $today $utcNow] %', v_msg;
            END;

            -- $dateTrunc("month", HireDate)
            BEGIN
                v_sql := pvt_build_projection_sql(
                    p_scheme_id  => v_emp_id,
                    p_projection => '[
                        {"alias":"hire_month","$expr":{"$dateTrunc":["month",{"$field":"HireDate"}]}}
                    ]'::jsonb,
                    p_limit      => 3);
                IF v_sql !~* 'DATE_TRUNC\(''month''' THEN
                    RAISE EXCEPTION 'expected DATE_TRUNC(''month'',...), got: %', v_sql;
                END IF;
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Employee / projection $dateTrunc month] rows=%', v_count;
            EXCEPTION WHEN OTHERS THEN
                v_fail := v_fail + 1;
                GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
                RAISE NOTICE 'FAIL [Employee / projection $dateTrunc month] %', v_msg;
            END;

            -- $regexReplace
            BEGIN
                v_sql := pvt_build_projection_sql(
                    p_scheme_id  => v_emp_id,
                    p_projection => '[
                        {"alias":"clean","$expr":{"$regexReplace":[{"$field":"FirstName"},{"$const":"[aeiou]"},{"$const":"_"}]}}
                    ]'::jsonb,
                    p_limit      => 3);
                IF v_sql !~* 'REGEXP_REPLACE\(' THEN
                    RAISE EXCEPTION 'expected REGEXP_REPLACE(...), got: %', v_sql;
                END IF;
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Employee / projection $regexReplace] rows=%', v_count;
            EXCEPTION WHEN OTHERS THEN
                v_fail := v_fail + 1;
                GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
                RAISE NOTICE 'FAIL [Employee / projection $regexReplace] %', v_msg;
            END;

            -- $regex predicate via $expr filter (pvt_build_expr_predicate path).
            BEGIN
                v_sql := pvt_build_query_sql(
                    p_scheme_id => v_emp_id,
                    p_filter    => '{"$expr":{"$iregex":[{"$field":"FirstName"},{"$const":"^[a-z]+$"}]}}'::jsonb);
                IF v_sql !~* ' ~\* ' THEN
                    RAISE EXCEPTION 'expected ~* operator, got: %', v_sql;
                END IF;
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Employee / filter $iregex via $expr] rows=%', v_count;
            EXCEPTION WHEN OTHERS THEN
                v_fail := v_fail + 1;
                GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
                RAISE NOTICE 'FAIL [Employee / filter $iregex via $expr] %', v_msg;
            END;

            -- $regex shorthand on field.
            BEGIN
                v_sql := pvt_build_query_sql(
                    p_scheme_id => v_emp_id,
                    p_filter    => '{"FirstName":{"$regex":"^[A-Z]"}}'::jsonb);
                IF v_sql !~* ' ~ ''' THEN
                    RAISE EXCEPTION 'expected ~ operator with literal, got: %', v_sql;
                END IF;
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Employee / filter $regex shorthand] rows=%', v_count;
            EXCEPTION WHEN OTHERS THEN
                v_fail := v_fail + 1;
                GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
                RAISE NOTICE 'FAIL [Employee / filter $regex shorthand] %', v_msg;
            END;

            -- $fts predicate (multi-field, object form).
            BEGIN
                v_sql := pvt_build_query_sql(
                    p_scheme_id => v_emp_id,
                    p_filter    => '{"$expr":{"$fts":{"query":{"$const":"ivan or john"},"fields":[{"$field":"FirstName"},{"$field":"LastName"}],"language":"simple"}}}'::jsonb);
                IF v_sql !~* 'to_tsvector\(' OR v_sql !~* 'websearch_to_tsquery\(' OR v_sql !~* '@@' THEN
                    RAISE EXCEPTION 'expected to_tsvector + websearch_to_tsquery + @@, got: %', v_sql;
                END IF;
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Employee / filter $fts multi-field] rows=%', v_count;
            EXCEPTION WHEN OTHERS THEN
                v_fail := v_fail + 1;
                GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
                RAISE NOTICE 'FAIL [Employee / filter $fts multi-field] %', v_msg;
            END;

            -- $fts shorthand on a single field.
            BEGIN
                v_sql := pvt_build_query_sql(
                    p_scheme_id => v_emp_id,
                    p_filter    => '{"FirstName":{"$fts":"ivan"}}'::jsonb);
                IF v_sql !~* 'to_tsvector\(' OR v_sql !~* 'websearch_to_tsquery\(' THEN
                    RAISE EXCEPTION 'expected FTS via to_tsvector / websearch_to_tsquery, got: %', v_sql;
                END IF;
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Employee / filter $fts shorthand] rows=%', v_count;
            EXCEPTION WHEN OTHERS THEN
                v_fail := v_fail + 1;
                GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
                RAISE NOTICE 'FAIL [Employee / filter $fts shorthand] %', v_msg;
            END;

            -- $padLeft / $indexOf already wired earlier — quick smoke.
            BEGIN
                v_sql := pvt_build_projection_sql(
                    p_scheme_id  => v_emp_id,
                    p_projection => '[
                        {"alias":"padded","$expr":{"$padLeft":[{"$field":"FirstName"},{"$const":10},{"$const":"*"}]}},
                        {"alias":"pos",   "$expr":{"$indexOf":[{"$field":"FirstName"},{"$const":"a"}]}}
                    ]'::jsonb,
                    p_limit      => 2);
                IF v_sql !~* 'LPAD\(' OR v_sql !~* 'POSITION\(' THEN
                    RAISE EXCEPTION 'expected LPAD/POSITION, got: %', v_sql;
                END IF;
                EXECUTE 'SELECT count(*) FROM (' || v_sql || ') _t' INTO v_count;
                v_pass := v_pass + 1;
                RAISE NOTICE 'OK   [Employee / projection $padLeft + $indexOf] rows=%', v_count;
            EXCEPTION WHEN OTHERS THEN
                v_fail := v_fail + 1;
                GET STACKED DIAGNOSTICS v_msg = MESSAGE_TEXT;
                RAISE NOTICE 'FAIL [Employee / projection $padLeft + $indexOf] %', v_msg;
            END;

        END IF;
    END;

    RAISE NOTICE '----------------------------------------------------------';
    RAISE NOTICE 'pvt smoke summary: % passed, % failed, % skipped',
                 v_pass, v_fail, v_skip;
    RAISE NOTICE '----------------------------------------------------------';

    IF v_fail > 0 THEN
        RAISE EXCEPTION 'pvt smoke: % case(s) failed (see NOTICE lines above)', v_fail;
    END IF;
END
$SMOKE$;


-- =====================================================================
-- SQL inspection: dump a few generated queries verbatim so you can see
-- both the base-pushdown predicate inside the CTE (`WHERE o._id_scheme
-- = X AND <push>`) and the residual outer `WHERE` over the CTE.
-- Runs even if the smoke above raised --- wrap separately.
-- =====================================================================
DO $INSPECT$
DECLARE
    v_scheme_id bigint;
    v_sql       text;
    v_dump      text;
    v_inspect   jsonb[];
    v_labels    text[];
    v_tag       text;
    v_has_cte         boolean;
    v_has_narrow_grp  boolean;
    v_has_narrow_join boolean;
    v_has_wide_join   boolean;
    v_shape_fail_cnt  int := 0;
    i           int;
BEGIN
    SELECT _id INTO v_scheme_id
      FROM _schemes
     WHERE _name = 'redb.Tests.Integration.Models.EmployeeProps'
     LIMIT 1;
    IF v_scheme_id IS NULL THEN
        RAISE NOTICE 'inspect: Employee scheme not found, skipping SQL dump';
        RETURN;
    END IF;

    -- Each label is tagged with the expected outer shape so we can
    -- assert it programmatically. Recognized prefixes:
    --   SHAPE_A: no CTE, plain SELECT over _objects.
    --   NARROW:  Pro-shape pivot CTE (`GROUP BY v._id_object`) + outer
    --            `JOIN _objects o ON o._id = _pvt_cte._id_object`.
    --   WIDE:    legacy wide CTE (`INNER|LEFT JOIN _values v ON
    --            v._id_object = o._id` + 21-col GROUP BY).
    v_labels  := ARRAY[
        'SHAPE_A: pure base _id>0 (expect no CTE, no _values)',
        'SHAPE_A: pure base $or all-base (expect no CTE)',
        'SHAPE_A: pure base $not (expect no CTE)',
        'SHAPE_A: empty filter (expect just SELECT _id FROM _objects o WHERE _id_scheme=X)',
        'NARROW: base+props AND (pushdown _id, residual FirstName)',
        'SHAPE_A: $or all-base (full pushdown, residual NULL)',
        'NARROW: $or mixed base/props (no pushdown, stays in outer WHERE)',
        'NARROW: expr arithmetic on props (residual UPPER+Age etc.)',
        'SHAPE_A: expr in base (full pushdown -- expect SHAPE A)',
        'WIDE: NULL props (LEFT JOIN to catch missing _values rows)',
        'NARROW: NOT NULL props ($notNull is satisfied by INNER JOIN)',
        'NARROW: simple dict ContainsKey (PhoneDirectory[work])',
        'NARROW: simple dict indexer eq (PhoneDirectory[work] = ...)',
        'NARROW: scalar prop eq (FirstName=Ivan)',
        'NARROW: scalar prop + base AND (FirstName + _id range)',
        'NARROW: ORDER BY base only on prop query (Age desc)'
    ];
    v_inspect := ARRAY[
        '{"_id":{"$gt":0}}'::jsonb,
        '{"$or":[{"_id":{"$lt":10}},{"_id":{"$gt":100}}]}'::jsonb,
        '{"$not":{"_id":{"$gt":1000000}}}'::jsonb,
        '{}'::jsonb,
        '{"_id":{"$gt":0},"FirstName":{"$startsWith":"A"}}'::jsonb,
        '{"$or":[{"_id":{"$lt":10}},{"_id":{"$gt":100}}]}'::jsonb,
        '{"$or":[{"_id":{"$lt":10}},{"FirstName":"Ivan"}]}'::jsonb,
        '{"$gt":[{"$add":[{"$field":"Age"},{"$const":1}]},{"$const":18}]}'::jsonb,
        '{"$between":[{"$field":"_id"},{"$const":0},{"$const":1000000}]}'::jsonb,
        '{"EmployeeCode":{"$null":true}}'::jsonb,
        '{"EmployeeCode":{"$notNull":true}}'::jsonb,
        '{"PhoneDirectory.ContainsKey":"work"}'::jsonb,
        '{"PhoneDirectory[work]":{"$eq":"+7-000-0000000"}}'::jsonb,
        '{"FirstName":"Ivan"}'::jsonb,
        '{"FirstName":{"$startsWith":"A"},"_id":{"$gt":0}}'::jsonb,
        '{"Age":{"$gt":18}}'::jsonb
    ];

    FOR i IN 1..array_length(v_inspect, 1) LOOP
        BEGIN
            v_sql := pvt_build_query_sql(
                p_scheme_id => v_scheme_id,
                p_filter    => v_inspect[i],
                p_limit     => 5);
            RAISE NOTICE E'----- INSPECT [%] -----\nFILTER: %\nSQL:\n%',
                v_labels[i], v_inspect[i]::text, v_sql;

            -- ---- Shape assertion driven by the label prefix --------
            v_tag := split_part(v_labels[i], ':', 1);
            v_has_cte         := position('WITH ' IN v_sql) > 0
                                 OR position('WITH _pvt_cte' IN v_sql) > 0
                                 OR position('WITH RECURSIVE' IN v_sql) > 0;
            v_has_narrow_grp  := position('GROUP BY v._id_object' IN v_sql) > 0;
            v_has_narrow_join := position('JOIN _objects o ON o._id = _pvt_cte._id_object' IN v_sql) > 0;
            v_has_wide_join   := position('JOIN _values v ON v._id_object = o._id' IN v_sql) > 0;

            IF v_tag = 'SHAPE_A' THEN
                IF v_has_cte THEN
                    v_shape_fail_cnt := v_shape_fail_cnt + 1;
                    RAISE NOTICE 'SHAPE-FAIL [%]: expected SHAPE_A (no CTE) but got CTE', v_labels[i];
                END IF;
            ELSIF v_tag = 'NARROW' THEN
                IF NOT (v_has_narrow_grp AND v_has_narrow_join) OR v_has_wide_join THEN
                    v_shape_fail_cnt := v_shape_fail_cnt + 1;
                    RAISE NOTICE 'SHAPE-FAIL [%]: expected NARROW shape (GROUP BY v._id_object + JOIN _objects o ON _pvt_cte._id_object, no wide _values JOIN)', v_labels[i];
                END IF;
            ELSIF v_tag = 'WIDE' THEN
                IF NOT v_has_wide_join OR v_has_narrow_grp THEN
                    v_shape_fail_cnt := v_shape_fail_cnt + 1;
                    RAISE NOTICE 'SHAPE-FAIL [%]: expected WIDE shape (JOIN _values v ON v._id_object = o._id)', v_labels[i];
                END IF;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'INSPECT [%] FAILED: %', v_labels[i], SQLERRM;
            v_shape_fail_cnt := v_shape_fail_cnt + 1;
        END;
    END LOOP;

    -- ---- Tree-mode narrow assertion ------------------------------------
    -- Tree pushdown (Pro CHANGELOG 2.0.1 parity): when source_mode=tree
    -- the recursive `_pvt_tree` CTE is still emitted, but its
    -- restriction (`o._id IN (SELECT _id FROM _pvt_tree)`) must be
    -- folded into the narrow IN-subquery rather than appended to a
    -- wide JOIN WHERE. We assert both signatures appear together and
    -- that the wide _values JOIN does NOT.
    BEGIN
        v_sql := pvt_build_query_sql(
            p_scheme_id   => v_scheme_id,
            p_filter      => '{"FirstName":{"$startsWith":"A"}}'::jsonb,
            p_limit       => 5,
            p_source_mode => 'tree',
            p_tree_ids    => ARRAY[v_scheme_id]::bigint[]);
        RAISE NOTICE E'----- INSPECT [NARROW: tree-mode pushdown (_pvt_tree folded into IN-subquery)] -----\nFILTER: %\nSQL:\n%',
            '{"FirstName":{"$startsWith":"A"}} +tree', v_sql;
        IF position('WITH RECURSIVE' IN v_sql) = 0
           OR position('GROUP BY v._id_object' IN v_sql) = 0
           OR position('o._id IN (SELECT _id FROM _pvt_tree)' IN v_sql) = 0
           OR position('JOIN _values v ON v._id_object = o._id' IN v_sql) > 0 THEN
            v_shape_fail_cnt := v_shape_fail_cnt + 1;
            RAISE NOTICE 'SHAPE-FAIL [NARROW: tree-mode pushdown]: expected WITH RECURSIVE _pvt_tree + narrow GROUP BY + tree filter folded into IN-subquery, no wide _values JOIN';
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'INSPECT [NARROW: tree-mode pushdown] FAILED: %', SQLERRM;
        v_shape_fail_cnt := v_shape_fail_cnt + 1;
    END;

    -- ---- Tree-modes family assertion (5 CTE shapes) --------------------
    -- For each explicit tree mode we assert the right CTE shape:
    --   tree_descendants / tree_ancestors -> WITH RECURSIVE
    --   tree_children / tree_roots / tree_leaves -> non-recursive WITH
    -- In every case the narrow pivot must be selected (no wide
    -- _values JOIN) and the tree restriction folded into the
    -- IN-subquery (`o._id IN (SELECT _id FROM _pvt_tree...`).
    DECLARE
        v_tree_labels text[];
        v_tree_modes  text[];
        v_recursive   boolean[];
        v_root        bigint;
        v_dep_scheme  bigint;
        j             int;
    BEGIN
        SELECT _id INTO v_dep_scheme
          FROM _schemes
         WHERE _name = 'redb.Tests.Integration.Models.DepartmentProps'
         LIMIT 1;
        SELECT _id INTO v_root
          FROM _objects
         WHERE _id_scheme = v_dep_scheme AND _id_parent IS NULL
         LIMIT 1;

        IF v_dep_scheme IS NULL OR v_root IS NULL THEN
            RAISE NOTICE 'inspect: Department scheme/root not found, skipping tree-modes assertion';
        ELSE
            v_tree_labels := ARRAY[
                'NARROW: tree_descendants (recursive walk DOWN)',
                'NARROW: tree_children (single-level, non-recursive)',
                'NARROW: tree_roots (_id_parent IS NULL, non-recursive)',
                'NARROW: tree_leaves (no children, non-recursive)',
                'NARROW: tree_ancestors (recursive walk UP)'
            ];
            v_tree_modes := ARRAY[
                'tree_descendants',
                'tree_children',
                'tree_roots',
                'tree_leaves',
                'tree_ancestors'
            ];
            v_recursive := ARRAY[true, false, false, false, true];

            FOR j IN 1..array_length(v_tree_modes, 1) LOOP
                BEGIN
                    v_sql := pvt_build_query_sql(
                        p_scheme_id   => v_dep_scheme,
                        p_filter      => '{"IsActive":{"$eq":true}}'::jsonb,
                        p_limit       => 5,
                        p_source_mode => v_tree_modes[j],
                        p_tree_ids    => ARRAY[v_root]::bigint[]);
                    RAISE NOTICE E'----- INSPECT [%] -----\nMODE: %\nSQL:\n%',
                        v_tree_labels[j], v_tree_modes[j], v_sql;
                    v_has_cte         := position('WITH ' IN v_sql) > 0;
                    v_has_narrow_grp  := position('GROUP BY v._id_object' IN v_sql) > 0;
                    v_has_wide_join   := position('JOIN _values v ON v._id_object = o._id' IN v_sql) > 0;
                    IF NOT v_has_cte
                       OR NOT v_has_narrow_grp
                       OR v_has_wide_join
                       OR position('o._id IN (SELECT _id FROM _pvt_tree' IN v_sql) = 0
                       OR (v_recursive[j] AND position('WITH RECURSIVE' IN v_sql) = 0)
                       OR (NOT v_recursive[j] AND position('WITH RECURSIVE' IN v_sql) > 0) THEN
                        v_shape_fail_cnt := v_shape_fail_cnt + 1;
                        RAISE NOTICE 'SHAPE-FAIL [%]: expected % CTE + narrow GROUP BY + IN(_pvt_tree) fold, no wide _values JOIN',
                            v_tree_labels[j],
                            CASE WHEN v_recursive[j] THEN 'WITH RECURSIVE' ELSE 'non-recursive WITH' END;
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    RAISE NOTICE 'INSPECT [%] FAILED: %', v_tree_labels[j], SQLERRM;
                    v_shape_fail_cnt := v_shape_fail_cnt + 1;
                END;
            END LOOP;
        END IF;
    END;

    IF v_shape_fail_cnt > 0 THEN
        RAISE EXCEPTION 'pvt inspect: % shape assertion(s) failed (see SHAPE-FAIL NOTICE lines above)', v_shape_fail_cnt;
    END IF;
END
$INSPECT$;

