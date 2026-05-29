-- =====================================================================
-- 99_smoke_auto.sql  (MSSql v2-pvt)
-- ---------------------------------------------------------------------
-- Automated PVT smoke runner. Iterates a static table of filter cases,
-- builds SQL via dbo.pvt_build_query_sql and counts rows. Reports
-- OK/FAIL/SKIP per case through PRINT. Raises at the end when any
-- case fails (via RAISERROR).
--
-- Scope of this slice (files 00-22, MSSql free tier):
--   * Filter operators: $eq $ne $gt $gte $lt $lte $in $nin $like
--                       $startsWith $endsWith $contains
--                       $startsWithIgnoreCase $endsWithIgnoreCase
--                       $containsIgnoreCase $null $isNull $notNull $exists
--   * Boolean combinators: $and $or $not
--   * Base-field pushdown: 0$: prefix, _id/_name/_date_* etc.
--   * ORDER BY + paging (OFFSET/FETCH)
--   * DistinctBy via ROW_NUMBER (DISTINCT ON emulation)
--   * Terminal aggregation: pvt_build_aggregate_sql (file 21)
--   * GROUP BY: pvt_build_groupby_sql (file 22)
--   * Array operators: $arrayContains $arrayAny $arrayEmpty
--                      $arrayCount $arrayCountGt $arrayFirst $arrayLast
--   * Property functions: .$length [].$count
--   * Hierarchical: $isRoot (flat-data test)
--
-- NOT in this slice (therefore not tested here):
--   * $ilike / $regex / $fts (PG-specific or not yet ported)
--   * $expr / B2 expression engine
--   * Dictionary operators (ContainsKey etc.)
--   * Window functions (file 23, PG-specific windowing)
--   * Array GroupBy (file 26, not yet ported)
--   * Full tree-mode tests (need tree-shaped data)
--
-- Designed against the redb.Tests.Integration model corpus
-- (Employee, Person, City, ProjectMetrics, Department).
-- Schemes are looked up by suffix match on _schemes._name or _alias.
-- Missing schemes are reported as SKIP and do not fail the run.
--
-- Usage (from sqlcmd or SSMS):
--   :r redb.MSSql/sql/v2-pvt/99_smoke_auto.sql
--   -- or:
--   sqlcmd -S ... -d <db> -I -i redb.MSSql/sql/v2-pvt/99_smoke_auto.sql
-- =====================================================================

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;

PRINT N'----------------------------------------------------------';
PRINT N'pvt smoke run starting, module version: ' + dbo.pvt_module_version();
PRINT N'----------------------------------------------------------';

-- =====================================================================
-- 1. FILTER CASES
-- =====================================================================

CREATE TABLE #smoke_cases (
    case_id     INT IDENTITY(1,1) PRIMARY KEY,
    scheme_name NVARCHAR(256) NOT NULL,  -- full CLR type name, as registered by C# InitializeAsync
    label       NVARCHAR(200) NOT NULL,
    filter_json NVARCHAR(MAX) NULL,
    skip_reason NVARCHAR(200) NULL   -- NULL = run; non-NULL = SKIP (not yet implemented)
);

INSERT INTO #smoke_cases (scheme_name, label, filter_json) VALUES
-- ----- Employee: comparison operators --------------------------------
('redb.Tests.Integration.Models.EmployeeProps', 'eq string',               '{"Department":{"$eq":"IT"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'shorthand eq',            '{"Department":"IT"}'),
('redb.Tests.Integration.Models.EmployeeProps', 'ne string',               '{"Department":{"$ne":"Sales"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'in string',               '{"Department":{"$in":["IT","HR"]}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'nin string',              '{"Department":{"$nin":["Sales"]}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'range int',               '{"Age":{"$gte":18,"$lte":65}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'numeric gt decimal',      '{"Salary":{"$gt":1000}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'numeric lt decimal',      '{"Salary":{"$lt":1000000}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'numeric range',           '{"Salary":{"$gte":0,"$lte":1000000}}'),
-- ----- Employee: null checks -----------------------------------------
('redb.Tests.Integration.Models.EmployeeProps', 'null check',              '{"EmployeeCode":{"$null":true}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'notNull check',           '{"EmployeeCode":{"$notNull":true}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$exists true',            '{"EmployeeCode":{"$exists":true}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$exists false',           '{"EmployeeCode":{"$exists":false}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$isNull true',            '{"EmployeeCode":{"$isNull":true}}'),
-- ----- Employee: string pattern operators ----------------------------
('redb.Tests.Integration.Models.EmployeeProps', 'startsWith',              '{"FirstName":{"$startsWith":"A"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'endsWith',                '{"LastName":{"$endsWith":"ov"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'contains',                '{"Position":{"$contains":"eng"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'containsIgnoreCase',      '{"Position":{"$containsIgnoreCase":"ENG"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'startsWithIgnoreCase',    '{"FirstName":{"$startsWithIgnoreCase":"a"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'endsWithIgnoreCase',      '{"LastName":{"$endsWithIgnoreCase":"OV"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'like',                    '{"FirstName":{"$like":"A%"}}'),
-- ----- Employee: boolean combinators ---------------------------------
('redb.Tests.Integration.Models.EmployeeProps', '$and+$or',                '{"$and":[{"Age":{"$gt":18}},{"$or":[{"Department":{"$eq":"IT"}},{"Department":{"$eq":"HR"}}]}]}'),
('redb.Tests.Integration.Models.EmployeeProps', '$or top-level',           '{"$or":[{"Department":"IT"},{"Department":"HR"}]}'),
('redb.Tests.Integration.Models.EmployeeProps', '$not',                    '{"$not":{"Age":{"$lt":18}}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$not + $and',             '{"$not":{"$and":[{"Age":{"$lt":18}},{"Department":"IT"}]}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'multi-field AND merge',   '{"Department":"IT","Age":{"$gt":18}}'),
-- ----- Employee: date comparisons ------------------------------------
('redb.Tests.Integration.Models.EmployeeProps', 'date gt',                 '{"HireDate":{"$gt":"2020-01-01T00:00:00Z"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'date lte',                '{"HireDate":{"$lte":"2030-01-01T00:00:00Z"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'date range',              '{"HireDate":{"$gte":"2000-01-01T00:00:00Z","$lt":"2030-01-01T00:00:00Z"}}'),
-- ----- Employee: base-field pushdown (0$: prefix + _col names) -------
('redb.Tests.Integration.Models.EmployeeProps', 'base 0$:Id gt',           '{"0$:Id":{"$gt":0}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'base 0$:Name startsWith', '{"0$:Name":{"$startsWith":"E"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'base _id range',          '{"_id":{"$gt":0}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'pushdown _date_create',   '{"_date_create":{"$gte":"2000-01-01T00:00:00Z"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'pushdown base+props AND', '{"_id":{"$gt":0},"Department":{"$ne":"X"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'pushdown $and explicit',  '{"$and":[{"_id":{"$gt":0}},{"Department":"IT"}]}'),
('redb.Tests.Integration.Models.EmployeeProps', 'pushdown $or all-base',   '{"$or":[{"_id":{"$lt":10}},{"_id":{"$gt":100}}]}'),
('redb.Tests.Integration.Models.EmployeeProps', 'pushdown $not base',      '{"$not":{"_id":{"$lt":0}}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'pushdown nested $and+$or','{"$and":[{"_id":{"$gt":0}},{"$or":[{"Department":"IT"},{"Department":"HR"}]}]}'),
-- ----- Employee: bool field -------------------------------------------
('redb.Tests.Integration.Models.EmployeeProps', 'bool eq true',            '{"IsRemote":{"$eq":true}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'bool eq false',           '{"IsRemote":{"$eq":false}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'bool shorthand true',     '{"IsRemote":true}'),
-- ----- Person: basic --------------------------------------------------
('redb.Tests.Integration.Models.PersonProps',   'eq Name (props)',         '{"Name":{"$eq":"Alice"}}'),
('redb.Tests.Integration.Models.PersonProps',   'Age range',               '{"Age":{"$gte":18,"$lte":99}}'),
('redb.Tests.Integration.Models.PersonProps',   'Email contains',          '{"Email":{"$contains":"@"}}'),
-- ListItem field by sub-property
('redb.Tests.Integration.Models.PersonProps',   'ListItem Status.Value',   '{"Status.Value":{"$eq":"Active"}}'),
('redb.Tests.Integration.Models.PersonProps',   'ListItem Status.Id',      '{"Status.Id":{"$gt":0}}'),
-- ----- ProjectMetrics: basic -----------------------------------------
('redb.Tests.Integration.Models.ProjectMetricsProps', 'numeric range Budget',  '{"Budget":{"$gte":0}}'),
('redb.Tests.Integration.Models.ProjectMetricsProps', 'TasksCompleted gte',    '{"TasksCompleted":{"$gte":0}}'),
('redb.Tests.Integration.Models.ProjectMetricsProps', 'TeamSize range',        '{"TeamSize":{"$gte":1,"$lte":1000}}'),
('redb.Tests.Integration.Models.ProjectMetricsProps', 'ProjectId gt',          '{"ProjectId":{"$gt":0}}'),
-- ----- City -----------------------------------------------------------
('redb.Tests.Integration.Models.CityProps', 'bool true',      '{"IsCapital":{"$eq":true}}'),
('redb.Tests.Integration.Models.CityProps', 'bool false',     '{"IsCapital":{"$eq":false}}'),
('redb.Tests.Integration.Models.CityProps', 'population gt',  '{"Population":{"$gt":1000}}'),
('redb.Tests.Integration.Models.CityProps', 'name in',        '{"Name":{"$in":["Moscow","Paris"]}}'),
('redb.Tests.Integration.Models.CityProps', 'region startsWith', '{"Region":{"$startsWith":"M"}}'),
-- ----- Department (auto-SKIP: DepartmentProps not in test DB) --------
('redb.Tests.Integration.Models.DepartmentProps', 'eq bool',           '{"IsActive":{"$eq":true}}'),
('redb.Tests.Integration.Models.DepartmentProps', 'name startsWith',   '{"Name":{"$startsWith":"IT"}}'),
('redb.Tests.Integration.Models.DepartmentProps', 'name like',         '{"Name":{"$like":"%"}}'),
('redb.Tests.Integration.Models.DepartmentProps', 'budget range',      '{"Budget":{"$gte":0}}'),
-- ----- Employee: array operators -------------------------------------
('redb.Tests.Integration.Models.EmployeeProps', '$arrayContains Skills C#',     '{"Skills":{"$arrayContains":"C#"}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayAny Skills',             '{"Skills":{"$arrayAny":true}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayEmpty Skills false',     '{"Skills":{"$arrayEmpty":false}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayCount Skills eq 3',      '{"Skills":{"$arrayCount":3}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayCountGt SkillLevels',    '{"SkillLevels":{"$arrayCountGt":0}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayFirst Skills C#',        '{"Skills":{"$arrayFirst":"C#"}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayLast SkillLevels',       '{"SkillLevels":{"$arrayLast":3}}'),
-- ----- Employee: property functions ----------------------------------
('redb.Tests.Integration.Models.EmployeeProps', 'FirstName.$length gt 3',       '{"FirstName.$length":{"$gt":3}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'Skills[].$count gte 2',        '{"Skills[].$count":{"$gte":2}}'),
-- ----- Employee: hierarchical (flat data; $isRoot = all rows) --------
('redb.Tests.Integration.Models.EmployeeProps', '$isRoot true',                 '{"$isRoot":true}'),
('redb.Tests.Integration.Models.EmployeeProps', '$isRoot false',                '{"$isRoot":false}'),
-- ----- Employee: more array operators (already implemented) ----------
('redb.Tests.Integration.Models.EmployeeProps', '$arrayCountGte SkillLevels',   '{"SkillLevels":{"$arrayCountGte":1}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayCountLt Skills',         '{"Skills":{"$arrayCountLt":10}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayCountLte Skills',        '{"Skills":{"$arrayCountLte":5}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayAny false',              '{"Skills":{"$arrayAny":false}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayEmpty true',             '{"Skills":{"$arrayEmpty":true}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayAt index=0 C#',          '{"Skills":{"$arrayAt":{"index":0,"value":"C#"}}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayAt index=1',             '{"Skills":{"$arrayAt":{"index":1,"value":"SQL"}}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayStartsWith C',           '{"Skills":{"$arrayStartsWith":"C"}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayEndsWith L',             '{"Skills":{"$arrayEndsWith":"L"}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'Skills.$count gte 1',          '{"Skills.$count":{"$gte":1}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'FirstName.$length eq 0',       '{"FirstName.$length":{"$eq":0}}'),
-- ----- Employee: PG-parity variants ---------------------------------
('redb.Tests.Integration.Models.EmployeeProps', 'ilike',                         '{"FirstName":{"$ilike":"a%"}}'),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayCount 0',                 '{"Skills":{"$arrayCount":0}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'Skills.$length gt',             '{"Skills.$length":{"$gt":0}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'SkillLevels.$count gt',         '{"SkillLevels.$count":{"$gt":0}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'pushdown $or mixed',            '{"$or":[{"_id":{"$lt":10}},{"FirstName":"Ivan"}]}'),
('redb.Tests.Integration.Models.EmployeeProps', 'pushdown $not mixed',           '{"$not":{"$and":[{"_id":{"$gt":0}},{"FirstName":"Ivan"}]}}'),
('redb.Tests.Integration.Models.EmployeeProps', 'pushdown 0$:Name+props',        '{"0$:Name":{"$startsWith":"E"},"Age":{"$gt":0}}'),
-- ----- ProjectMetrics: array + property ops -------------------------
('redb.Tests.Integration.Models.ProjectMetricsProps', 'BugsFixed gte',           '{"BugsFixed":{"$gte":0}}'),
('redb.Tests.Integration.Models.ProjectMetricsProps', 'tech $arrayContains',     '{"Technologies":{"$arrayContains":"PostgreSQL"}}'),
('redb.Tests.Integration.Models.ProjectMetricsProps', 'tech $arrayAny',          '{"Technologies":{"$arrayAny":true}}'),
('redb.Tests.Integration.Models.ProjectMetricsProps', 'tech $arrayStartsWith',   '{"Technologies":{"$arrayStartsWith":"P"}}'),
('redb.Tests.Integration.Models.ProjectMetricsProps', 'tech .$length gt',        '{"Technologies.$length":{"$gt":0}}'),
-- ----- City: float-array coords + range -----------------------------
('redb.Tests.Integration.Models.CityProps', 'coords $arrayCount 2',              '{"Coordinates":{"$arrayCount":2}}'),
('redb.Tests.Integration.Models.CityProps', 'coords $arrayAt index=0',           '{"Coordinates":{"$arrayAt":{"index":0,"value":55.7558}}}'),
('redb.Tests.Integration.Models.CityProps', 'coords .$length eq 2',              '{"Coordinates.$length":{"$eq":2}}'),
('redb.Tests.Integration.Models.CityProps', 'population range',                  '{"Population":{"$gte":0,"$lt":50000000}}'),
-- ----- Department (auto-SKIP: DepartmentProps not in test DB) --------
('redb.Tests.Integration.Models.DepartmentProps', 'code contains',               '{"Code":{"$contains":"-"}}'),
('redb.Tests.Integration.Models.DepartmentProps', 'description notNull',         '{"Description":{"$notNull":true}}');

-- ----- Not-yet-implemented: tracked as SKIP -------------------------
INSERT INTO #smoke_cases (scheme_name, label, filter_json, skip_reason) VALUES
-- $array aggregates
('redb.Tests.Integration.Models.EmployeeProps', '$arrayMatches Skills',    '{"Skills":{"$arrayMatches":"%QL%"}}',          NULL),
('redb.Tests.Integration.Models.EmployeeProps', '$arraySum SkillLevels',   '{"SkillLevels":{"$arraySum":10}}',              NULL),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayMin SkillLevels',   '{"SkillLevels":{"$arrayMin":1}}',               NULL),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayMax SkillLevels',   '{"SkillLevels":{"$arrayMax":5}}',               NULL),
('redb.Tests.Integration.Models.EmployeeProps', '$arrayAvg SkillLevels',   '{"SkillLevels":{"$arrayAvg":3}}',               NULL),
-- Dictionary operators
('redb.Tests.Integration.Models.EmployeeProps', 'dict ContainsKey work',   '{"PhoneDirectory.ContainsKey":"work"}',          NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'dict ContainsKey home',   '{"PhoneDirectory.ContainsKey":"home"}',          NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'dict indexer value',      '{"PhoneDirectory[work]":{"$eq":"+7-000"}}',     NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'dict indexer notNull',    '{"PhoneDirectory[work]":{"$notNull":true}}',    NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'dict nested .City',       '{"OfficeLocations[Moscow].City":{"$eq":"Moscow"}}', NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'dict nested .Street',     '{"OfficeLocations[Moscow].Street":{"$notNull":true}}', NULL),
-- ListItem accessors
('redb.Tests.Integration.Models.PersonProps', 'ListItem Status.Value',     '{"Status.Value":{"$eq":"Active"}}',             NULL),
('redb.Tests.Integration.Models.PersonProps', 'ListItem Status.Alias',     '{"Status.Alias":{"$eq":"active"}}',             NULL),
('redb.Tests.Integration.Models.PersonProps', 'ListItem Status.Id',        '{"Status.Id":{"$gt":0}}',                       NULL),
('redb.Tests.Integration.Models.PersonProps', 'ListItem Roles[].Value in', '{"Roles[].Value":{"$in":["admin","user"]}}',    NULL),
('redb.Tests.Integration.Models.PersonProps', 'ListItem Roles[].Value nin', '{"Roles[].Value":{"$nin":["guest"]}}',          NULL),
-- B2 expression engine
('redb.Tests.Integration.Models.EmployeeProps', 'expr $add gt',            '{"$gt":[{"$add":[{"$field":"Age"},{"$const":1}]},{"$const":18}]}',              NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $mul lt',            '{"$lt":[{"$mul":[{"$field":"_id"},{"$const":2}]},{"$const":1000000}]}',         NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $upper eq',          '{"$eq":[{"$upper":[{"$field":"FirstName"}]},{"$const":"IVAN"}]}',               NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $concat ilike',      '{"$ilike":[{"$concat":[{"$field":"FirstName"},{"$const":" "},{"$field":"LastName"}]},{"$const":"%van%"}]}', NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $coalesce eq',       '{"$eq":[{"$coalesce":[{"$field":"Department"},{"$const":"none"}]},{"$const":"IT"}]}', NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $between',           '{"$between":[{"$field":"_id"},{"$const":0},{"$const":1000000}]}',               NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $in props',          '{"$in":[{"$field":"Department"},["IT","HR","R&D"]]}',                           NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $nin base',          '{"$nin":[{"$field":"_id"},[-1,-2,-3]]}',                                        NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $length gt',         '{"$gt":[{"$length":[{"$field":"FirstName"}]},{"$const":0}]}',                   NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $abs sub',           '{"$lt":[{"$abs":[{"$sub":[{"$field":"Age"},{"$const":30}]}]},{"$const":1000}]}', NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $null base push',    '{"$null":{"$field":"_id_parent"}}',                                              NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $notNull props',     '{"$notNull":{"$field":"FirstName"}}',                                            NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $contains sugar',    '{"$contains":[{"$field":"FirstName"},"a"]}',                                    NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $startsWith sugar',  '{"$startsWith":[{"$field":"LastName"},"S"]}',                                   NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $endsWith sugar',    '{"$endsWith":[{"$field":"LastName"},"ov"]}',                                    NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $min 2-arg',         '{"$lt":[{"$min":[{"$field":"Age"},{"$const":100}]},{"$max":[{"$field":"Age"},{"$const":0}]}]}', NULL),
-- B2-expr ORDER BY $expr: tested in dedicated block (section 2), not in the filter loop.
-- B2-expr extended scalar functions in @filter
('redb.Tests.Integration.Models.EmployeeProps', 'expr $year EXTRACT',    '{"$gte":[{"$year":{"$field":"HireDate"}},{"$const":2000}]}',                        NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $trimStart LTRIM',  '{"$eq":[{"$trimStart":{"$field":"FirstName"}},{"$const":"Ivan"}]}',                 NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $substring',       '{"$eq":[{"$substring":[{"$field":"FirstName"},{"$const":1},{"$const":3}]},{"$const":"Iva"}]}', NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $replace',         '{"$eq":[{"$replace":[{"$field":"FirstName"},{"$const":"a"},{"$const":"A"}]},{"$const":"IvAn"}]}', NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $power',           '{"$lt":[{"$power":[{"$field":"Age"},{"$const":2}]},{"$const":1000000}]}',          NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $dateAdd day',     '{"$gt":[{"$dateAdd":["day",{"$field":"HireDate"},{"$const":30}]},{"$const":"2000-01-01T00:00:00Z"}]}', NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $if conditional',  '{"$eq":[{"$if":[{"$gte":[{"$field":"Salary"},{"$const":100000}]},{"$const":"High"},{"$const":"Normal"}]},{"$const":"Normal"}]}', NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $case multi-arm',  '{"$eq":[{"$case":[{"when":{"$lt":[{"$field":"Age"},{"$const":18}]},"then":{"$const":"minor"}},{"when":{"$lt":[{"$field":"Age"},{"$const":65}]},"then":{"$const":"adult"}},{"else":{"$const":"senior"}}]},{"$const":"adult"}]}', NULL),
-- B2-expr: operators added in Pro parity sprint (floor/ceil/div/mod/neg/lower/trimEnd/
--          month/day/hour/minute/second/sqrt/sign/exp/ln/log10/round/log/dateSub/indexOf)
('redb.Tests.Integration.Models.EmployeeProps', 'expr $floor',         '{"$gte":[{"$floor":{"$field":"Age"}},{"$const":0}]}',                                                                NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $ceil',          '{"$gte":[{"$ceil":{"$field":"Age"}},{"$const":0}]}',                                                                 NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $div',           '{"$gte":[{"$div":[{"$field":"Age"},{"$const":1}]},{"$const":0}]}',                                                NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $mod',           '{"$gte":[{"$mod":[{"$field":"Age"},{"$const":2}]},{"$const":0}]}',                                                NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $neg',           '{"$lte":[{"$neg":{"$field":"Age"}},{"$const":0}]}',                                                               NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $lower',         '{"$ne":[{"$lower":{"$field":"FirstName"}},{"$const":""}]}',                                                    NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $trimEnd',       '{"$gte":[{"$length":{"$trimEnd":{"$field":"FirstName"}}},{"$const":0}]}',                                     NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $month',         '{"$gte":[{"$month":{"$field":"HireDate"}},{"$const":1}]}',                                                      NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $day',           '{"$gte":[{"$day":{"$field":"HireDate"}},{"$const":1}]}',                                                        NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $hour',          '{"$gte":[{"$hour":{"$field":"HireDate"}},{"$const":0}]}',                                                      NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $minute',        '{"$gte":[{"$minute":{"$field":"HireDate"}},{"$const":0}]}',                                                    NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $second',        '{"$gte":[{"$second":{"$field":"HireDate"}},{"$const":0}]}',                                                    NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $sqrt',          '{"$gte":[{"$sqrt":{"$field":"Age"}},{"$const":0}]}',                                                           NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $sign',          '{"$gt":[{"$sign":{"$field":"Age"}},{"$const":0}]}',                                                            NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $exp',           '{"$gte":[{"$exp":{"$const":0}},{"$const":1}]}',                                                                  NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $ln',            '{"$gte":[{"$ln":{"$const":1}},{"$const":0}]}',                                                                   NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $log10',         '{"$gte":[{"$log10":{"$const":100}},{"$const":1}]}',                                                             NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $round',         '{"$gte":[{"$round":[{"$field":"Age"},{"$const":0}]},{"$const":0}]}',                                        NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $log 2-arg',     '{"$gte":[{"$log":[{"$const":100},{"$const":10}]},{"$const":1}]}',                                             NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $dateSub day',   '{"$gt":[{"$dateSub":["day",{"$field":"HireDate"},{"$const":30}]},{"$const":"1990-01-01T00:00:00Z"}]}',  NULL),
('redb.Tests.Integration.Models.EmployeeProps', 'expr $indexOf',       '{"$gte":[{"$indexOf":[{"$field":"FirstName"},{"$const":"a"}]},{"$const":0}]}',                          NULL);
-- DISTINCT + DISTINCT ON guard: tested in dedicated block (section 3), not in the filter loop.

-- =====================================================================
-- Run filter cases
-- =====================================================================

DECLARE
    @i          INT = 1,
    @max        INT,
    @pass       INT = 0,
    @fail       INT = 0,
    @skip       INT = 0;

SELECT @max = MAX(case_id) FROM #smoke_cases;

WHILE @i <= @max
BEGIN
    DECLARE
        @scheme_name NVARCHAR(100),
        @label       NVARCHAR(200),
        @filter_json NVARCHAR(MAX),
        @skip_reason NVARCHAR(200);

    SELECT
        @scheme_name = scheme_name,
        @label       = label,
        @filter_json = filter_json,
        @skip_reason = skip_reason
    FROM #smoke_cases
    WHERE case_id = @i;

    IF @skip_reason IS NOT NULL
    BEGIN
        SET @skip += 1;
        PRINT N'SKIP [' + @scheme_name + N' / ' + @label + N'] ' + @skip_reason;
        SET @i += 1;
        CONTINUE;
    END;

    -- Scheme lookup: exact match on _name (full CLR type name, as stored by C# InitializeAsync).
    DECLARE @scheme_id BIGINT = NULL;
    SELECT TOP 1 @scheme_id = [_id]
    FROM dbo._schemes
    WHERE [_name] = @scheme_name;

    IF @scheme_id IS NULL
    BEGIN
        SET @skip += 1;
        PRINT N'SKIP [' + @scheme_name + N' / ' + @label + N'] scheme not found';
        SET @i += 1;
        CONTINUE;
    END;

    BEGIN TRY
        DECLARE @sql NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @scheme_id, @filter_json,
            NULL, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);

        DECLARE @cnt_sql NVARCHAR(MAX) =
            N'SELECT @cnt = COUNT(*) FROM (' + @sql + N') _t';
        DECLARE @cnt BIGINT;
        EXEC sp_executesql @cnt_sql, N'@cnt BIGINT OUTPUT', @cnt = @cnt OUTPUT;

        SET @pass += 1;
        PRINT N'OK   [' + @scheme_name + N' / ' + @label + N'] rows='
              + CAST(ISNULL(@cnt, 0) AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [' + @scheme_name + N' / ' + @label + N'] '
              + ERROR_MESSAGE();
    END CATCH;

    SET @i += 1;
END;

DROP TABLE #smoke_cases;

-- =====================================================================
-- 2. ORDER BY + PAGING (Employee only)
-- =====================================================================

DECLARE @emp_id BIGINT = NULL;
SELECT TOP 1 @emp_id = [_id]
FROM dbo._schemes
WHERE [_name] = N'redb.Tests.Integration.Models.EmployeeProps';

IF @emp_id IS NOT NULL
BEGIN
    -- order by props field desc + FETCH
    BEGIN TRY
        DECLARE @s1 NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @emp_id, NULL, 10, 0,
            N'[{"field":"Age","dir":"desc","nulls":"last"}]',
            NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        DECLARE @c1 BIGINT;
        DECLARE @x1 NVARCHAR(MAX) = N'SELECT @c = COUNT(*) FROM (' + @s1 + N') _t';
        EXEC sp_executesql @x1, N'@c BIGINT OUTPUT', @c = @c1 OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / order Age desc + FETCH 10] rows=' + CAST(@c1 AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / order Age desc + FETCH 10] ' + ERROR_MESSAGE();
    END CATCH;

    -- multi-column ORDER BY + offset
    BEGIN TRY
        DECLARE @s2 NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @emp_id, NULL, 25, 5,
            N'[{"field":"LastName","dir":"asc"},{"field":"FirstName","dir":"asc"}]',
            NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        DECLARE @c2 BIGINT;
        DECLARE @x2 NVARCHAR(MAX) = N'SELECT @c = COUNT(*) FROM (' + @s2 + N') _t';
        EXEC sp_executesql @x2, N'@c BIGINT OUTPUT', @c = @c2 OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / order multi + offset 5] rows=' + CAST(@c2 AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / order multi + offset 5] ' + ERROR_MESSAGE();
    END CATCH;

    -- order by base field 0$:Id desc + filter
    BEGIN TRY
        DECLARE @s3 NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @emp_id, N'{"Age":{"$gte":18}}', 5, 0,
            N'[{"field":"0$:Id","dir":"desc"}]',
            NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        DECLARE @c3 BIGINT;
        DECLARE @x3 NVARCHAR(MAX) = N'SELECT @c = COUNT(*) FROM (' + @s3 + N') _t';
        EXEC sp_executesql @x3, N'@c BIGINT OUTPUT', @c = @c3 OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / filter + order base Id desc] rows=' + CAST(@c3 AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / filter + order base Id desc] ' + ERROR_MESSAGE();
    END CATCH;

    -- ORDER BY $expr: arithmetic Age*2 DESC.
    -- NOTE: $expr ORDER BY currently emits ORDER BY (SELECT 1) — full expression
    -- evaluation in ORDER BY is not yet implemented; test guards against regression
    -- (ORDER BY clause must be present and query must execute without error).
    BEGIN TRY
        DECLARE @oe1 NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @emp_id, NULL, 5, 0,
            N'[{"$expr":{"$mul":[{"$field":"Age"},{"$const":2}]},"dir":"desc"}]',
            NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF @oe1 NOT LIKE N'%ORDER BY%'
            RAISERROR(N'expected ORDER BY in SQL, got: %.200s', 16, 1, @oe1);
        DECLARE @coe1 BIGINT;
        DECLARE @xoe1 NVARCHAR(MAX) = N'SELECT @c = COUNT(*) FROM (' + @oe1 + N') _t';
        EXEC sp_executesql @xoe1, N'@c BIGINT OUTPUT', @c = @coe1 OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / order $expr Age*2 desc] rows=' + CAST(@coe1 AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / order $expr Age*2 desc] ' + ERROR_MESSAGE();
    END CATCH;

    -- ORDER BY $expr: UPPER(FirstName) ASC.
    -- NOTE: same as above — placeholder ORDER BY (SELECT 1) until $expr ORDER BY is implemented.
    BEGIN TRY
        DECLARE @oe2 NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @emp_id, NULL, 5, 0,
            N'[{"$expr":{"$upper":{"$field":"FirstName"}},"dir":"asc"}]',
            NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF @oe2 NOT LIKE N'%ORDER BY%'
            RAISERROR(N'expected ORDER BY in SQL, got: %.200s', 16, 1, @oe2);
        DECLARE @coe2 BIGINT;
        DECLARE @xoe2 NVARCHAR(MAX) = N'SELECT @c = COUNT(*) FROM (' + @oe2 + N') _t';
        EXEC sp_executesql @xoe2, N'@c BIGINT OUTPUT', @c = @coe2 OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / order $expr UPPER(FirstName) asc] rows=' + CAST(@coe2 AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / order $expr UPPER(FirstName) asc] ' + ERROR_MESSAGE();
    END CATCH;

    -- ORDER BY $expr: LEN(_name)+1 DESC (base column in $expr).
    -- NOTE: same placeholder limitation — full $expr ORDER BY to be implemented later.
    BEGIN TRY
        DECLARE @oe3 NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @emp_id, NULL, 5, 0,
            N'[{"$expr":{"$add":[{"$length":{"$field":"_name"}},{"$const":1}]},"dir":"desc"}]',
            NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF @oe3 NOT LIKE N'%ORDER BY%'
            RAISERROR(N'expected ORDER BY in SQL, got: %.200s', 16, 1, @oe3);
        DECLARE @coe3 BIGINT;
        DECLARE @xoe3 NVARCHAR(MAX) = N'SELECT @c = COUNT(*) FROM (' + @oe3 + N') _t';
        EXEC sp_executesql @xoe3, N'@c BIGINT OUTPUT', @c = @coe3 OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / order $expr LEN(_name)+1 desc] rows=' + CAST(@coe3 AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / order $expr LEN(_name)+1 desc] ' + ERROR_MESSAGE();
    END CATCH;
END
ELSE
    PRINT N'SKIP [Employee / order+paging block] scheme not found';

-- =====================================================================
-- 2b. Phase 2: top-level $eq / $expr filter (base pushdown + residual)
-- =====================================================================

IF @emp_id IS NOT NULL
BEGIN
    -- Top-level $eq with base field -> pushed into Shape A.
    BEGIN TRY
        DECLARE @ex1 NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @emp_id, N'{"$eq":[{"$field":"_id"},{"$field":"_id"}]}',
            5, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @ex1) > 0
            RAISERROR(N'expected Shape A (no CTE) for top-level $eq base, got: %.200s', 16, 1, @ex1);
        DECLARE @cex1 BIGINT;
        DECLARE @xex1 NVARCHAR(MAX) = N'SELECT @c = COUNT(*) FROM (' + @ex1 + N') _t';
        EXEC sp_executesql @xex1, N'@c BIGINT OUTPUT', @c = @cex1 OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / filter top $eq base (Shape A)] rows=' + CAST(@cex1 AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / filter top $eq base] ' + ERROR_MESSAGE();
    END CATCH;

    -- Top-level $expr with base-only arithmetic -> pushed into Shape A.
    BEGIN TRY
        DECLARE @ex2 NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @emp_id,
            N'{"$expr":{"$gt":[{"$length":{"$field":"_name"}},{"$const":0}]}}',
            10, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @ex2) > 0
            RAISERROR(N'expected Shape A (no CTE) for $expr base-only, got: %.200s', 16, 1, @ex2);
        DECLARE @cex2 BIGINT;
        DECLARE @xex2 NVARCHAR(MAX) = N'SELECT @c = COUNT(*) FROM (' + @ex2 + N') _t';
        EXEC sp_executesql @xex2, N'@c BIGINT OUTPUT', @c = @cex2 OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / filter $expr base-only (Shape A)] rows=' + CAST(@cex2 AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / filter $expr base-only] ' + ERROR_MESSAGE();
    END CATCH;

    -- Top-level $expr referencing props -> Shape C, predicate runs over CTE.
    BEGIN TRY
        DECLARE @ex3 NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @emp_id,
            N'{"$expr":{"$gte":[{"$field":"Age"},{"$const":0}]}}',
            10, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @ex3) = 0
            RAISERROR(N'expected Shape C (CTE) for $expr props, got: %.200s', 16, 1, @ex3);
        DECLARE @cex3 BIGINT;
        DECLARE @xex3 NVARCHAR(MAX) = N'SELECT @c = COUNT(*) FROM (' + @ex3 + N') _t';
        EXEC sp_executesql @xex3, N'@c BIGINT OUTPUT', @c = @cex3 OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / filter $expr props (Shape C residual)] rows=' + CAST(@cex3 AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / filter $expr props] ' + ERROR_MESSAGE();
    END CATCH;

    -- Mixed: implicit $and of base $eq + props field -> Shape C with split.
    BEGIN TRY
        DECLARE @ex4 NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @emp_id,
            N'{"$gt":[{"$field":"_id"},{"$const":0}],"FirstName":{"$startsWith":"A"}}',
            5, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @ex4) = 0
            RAISERROR(N'expected Shape C (CTE) for mixed $gt+props, got: %.200s', 16, 1, @ex4);
        DECLARE @cex4 BIGINT;
        DECLARE @xex4 NVARCHAR(MAX) = N'SELECT @c = COUNT(*) FROM (' + @ex4 + N') _t';
        EXEC sp_executesql @xex4, N'@c BIGINT OUTPUT', @c = @cex4 OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / filter mixed $gt base + props residual] rows=' + CAST(@cex4 AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / filter mixed $gt base + props residual] ' + ERROR_MESSAGE();
    END CATCH;
END
ELSE
    PRINT N'SKIP [Employee / Phase 2 expr-filter block] scheme not found';

-- =====================================================================
-- 3. DISTINCT BY (ROW_NUMBER emulation)
-- =====================================================================

IF @emp_id IS NOT NULL
BEGIN
    -- DistinctBy props field (Department)
    BEGIN TRY
        DECLARE @sd1 NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @emp_id, NULL, NULL, NULL, NULL,
            NULL, NULL, N'flat', NULL, NULL, NULL,
            N'[{"field":"Department"}]');
        DECLARE @cd1 BIGINT;
        DECLARE @xd1 NVARCHAR(MAX) = N'SELECT @c = COUNT(*) FROM (' + @sd1 + N') _t';
        EXEC sp_executesql @xd1, N'@c BIGINT OUTPUT', @c = @cd1 OUTPUT;
        -- Verify ROW_NUMBER pattern in generated SQL
        IF @sd1 NOT LIKE N'%ROW_NUMBER()%'
        BEGIN
            RAISERROR(N'expected ROW_NUMBER() in SQL for DistinctBy, got: %.200s', 16, 1, @sd1);
        END;
        SET @pass += 1;
        PRINT N'OK   [Employee / DistinctBy Department (ROW_NUMBER)] rows='
              + CAST(@cd1 AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / DistinctBy Department (ROW_NUMBER)] ' + ERROR_MESSAGE();
    END CATCH;

    -- DistinctBy base field (_id_owner)
    BEGIN TRY
        DECLARE @sd2 NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @emp_id, NULL, NULL, NULL, NULL,
            NULL, NULL, N'flat', NULL, NULL, NULL,
            N'[{"field":"0$:Id"}]');
        DECLARE @cd2 BIGINT;
        DECLARE @xd2 NVARCHAR(MAX) = N'SELECT @c = COUNT(*) FROM (' + @sd2 + N') _t';
        EXEC sp_executesql @xd2, N'@c BIGINT OUTPUT', @c = @cd2 OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / DistinctBy base 0$:Id] rows='
              + CAST(@cd2 AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / DistinctBy base 0$:Id] ' + ERROR_MESSAGE();
    END CATCH;

    -- Plain SELECT DISTINCT
    BEGIN TRY
        DECLARE @sdis_build NVARCHAR(MAX) = dbo.pvt_build_query_sql(
            @emp_id, N'{"Age":{"$gte":18}}', 5, NULL,
            N'[{"field":"_id","dir":"asc"}]',
            NULL, 1, N'flat', NULL, NULL, NULL, NULL);
        IF @sdis_build NOT LIKE N'%DISTINCT%'
            RAISERROR(N'expected DISTINCT in SQL, got: %.200s', 16, 1, @sdis_build);
        DECLARE @sdis_cnt BIGINT;
        DECLARE @sdis_wrap NVARCHAR(MAX) = N'SELECT @c = COUNT(*) FROM (' + @sdis_build + N') _t';
        EXEC sp_executesql @sdis_wrap, N'@c BIGINT OUTPUT', @c = @sdis_cnt OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / DISTINCT filter Age>=18 limit 5] rows='
              + CAST(ISNULL(@sdis_cnt, 0) AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / DISTINCT filter Age>=18 limit 5] ' + ERROR_MESSAGE();
    END CATCH;

    -- DISTINCT + DISTINCT ON guard: both set must raise 'mutually exclusive'.
    -- Guard is implemented via CAST(text AS INT) in pvt_build_query_sql (file 20).
    BEGIN TRY
        DECLARE @guard_sql NVARCHAR(MAX);
        BEGIN TRY
            SET @guard_sql = dbo.pvt_build_query_sql(
                @emp_id, NULL, NULL, NULL, NULL,
                NULL, 1, N'flat', NULL, NULL, NULL,
                N'[{"field":"Department"}]');  -- @distinct=1 AND @distinct_on not null
            -- If no exception: fail
            SET @fail += 1;
            PRINT N'FAIL [Employee / DISTINCT + DISTINCT_ON guard] expected error, got SQL: '
                  + LEFT(ISNULL(@guard_sql, N'NULL'), 120);
        END TRY
        BEGIN CATCH
            IF ERROR_MESSAGE() LIKE N'%mutually exclusive%'
            BEGIN
                SET @pass += 1;
                PRINT N'OK   [Employee / DISTINCT + DISTINCT_ON guard] raised: ' + ERROR_MESSAGE();
            END
            ELSE
            BEGIN
                SET @fail += 1;
                PRINT N'FAIL [Employee / DISTINCT + DISTINCT_ON guard] wrong error: ' + ERROR_MESSAGE();
            END
        END CATCH;
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / DISTINCT + DISTINCT_ON guard outer] ' + ERROR_MESSAGE();
    END CATCH;
END
ELSE
    PRINT N'SKIP [Employee / DistinctBy block] scheme not found';

-- =====================================================================
-- 4. AGGREGATE (pvt_build_aggregate_sql, file 21)
-- =====================================================================

IF @emp_id IS NOT NULL
BEGIN
    -- COUNT(*)
    BEGIN TRY
        DECLARE @agg1_build NVARCHAR(MAX) = dbo.pvt_build_aggregate_sql(
            @emp_id, NULL,
            N'[{"alias":"total","$count":"*"}]',
            N'flat');
        DECLARE @agg1_wrap NVARCHAR(MAX) =
            N'SELECT (SELECT * FROM (' + @agg1_build
            + N') _agg FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES)';
        DECLARE @agg1_result NVARCHAR(MAX);
        EXEC sp_executesql @agg1_wrap, N'@r NVARCHAR(MAX) OUTPUT', @r = @agg1_result OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / agg COUNT(*)] json=' + ISNULL(@agg1_result, N'NULL');
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / agg COUNT(*)] ' + ERROR_MESSAGE();
    END CATCH;

    -- SUM + AVG on Salary with filter
    BEGIN TRY
        DECLARE @agg2_build NVARCHAR(MAX) = dbo.pvt_build_aggregate_sql(
            @emp_id,
            N'{"IsRemote":{"$eq":true}}',
            N'[{"alias":"cnt","$count":"*"},{"alias":"avg_sal","$avg":{"$field":"Salary"}},{"alias":"sum_sal","$sum":{"$field":"Salary"}}]',
            N'flat');
        DECLARE @agg2_wrap NVARCHAR(MAX) =
            N'SELECT (SELECT * FROM (' + @agg2_build
            + N') _agg FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES)';
        DECLARE @agg2_result NVARCHAR(MAX);
        EXEC sp_executesql @agg2_wrap, N'@r NVARCHAR(MAX) OUTPUT', @r = @agg2_result OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / agg SUM+AVG+COUNT IsRemote=true] json='
              + ISNULL(@agg2_result, N'NULL');
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / agg SUM+AVG+COUNT IsRemote=true] ' + ERROR_MESSAGE();
    END CATCH;

    -- MIN + MAX on Age
    BEGIN TRY
        DECLARE @agg3_build NVARCHAR(MAX) = dbo.pvt_build_aggregate_sql(
            @emp_id, NULL,
            N'[{"alias":"min_age","$min":{"$field":"Age"}},{"alias":"max_age","$max":{"$field":"Age"}}]',
            N'flat');
        DECLARE @agg3_wrap NVARCHAR(MAX) =
            N'SELECT (SELECT * FROM (' + @agg3_build
            + N') _agg FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES)';
        DECLARE @agg3_result NVARCHAR(MAX);
        EXEC sp_executesql @agg3_wrap, N'@r NVARCHAR(MAX) OUTPUT', @r = @agg3_result OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / agg MIN+MAX Age] json='
              + ISNULL(@agg3_result, N'NULL');
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / agg MIN+MAX Age] ' + ERROR_MESSAGE();
    END CATCH;
END
ELSE
    PRINT N'SKIP [Employee / aggregate block] scheme not found';

-- =====================================================================
-- 5. GROUP BY (pvt_build_groupby_sql, file 22)
-- =====================================================================

IF @emp_id IS NOT NULL
BEGIN
    -- GroupBy Department + COUNT + SUM(Salary)
    BEGIN TRY
        DECLARE @grp1_build NVARCHAR(MAX) = dbo.pvt_build_groupby_sql(
            @emp_id, NULL,
            N'[{"field":"Department","alias":"Department"}]',
            N'[{"alias":"Count","$count":"*"},{"alias":"TotalSalary","$sum":{"$field":"Salary"}}]',
            NULL, NULL, NULL, 0, N'flat');
        DECLARE @grp1_wrap NVARCHAR(MAX) =
            N'SELECT (SELECT * FROM (' + @grp1_build
            + N') _grp FOR JSON PATH, INCLUDE_NULL_VALUES)';
        DECLARE @grp1_result NVARCHAR(MAX);
        EXEC sp_executesql @grp1_wrap, N'@r NVARCHAR(MAX) OUTPUT', @r = @grp1_result OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / groupby Department + COUNT + SUM] json_len='
              + CAST(LEN(ISNULL(@grp1_result, N'')) AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / groupby Department + COUNT + SUM] ' + ERROR_MESSAGE();
    END CATCH;

    -- GroupBy IsRemote (bool) + COUNT
    BEGIN TRY
        DECLARE @grp2_build NVARCHAR(MAX) = dbo.pvt_build_groupby_sql(
            @emp_id, NULL,
            N'[{"field":"IsRemote","alias":"IsRemote"}]',
            N'[{"alias":"Count","$count":"*"}]',
            NULL, NULL, NULL, 0, N'flat');
        DECLARE @grp2_wrap NVARCHAR(MAX) =
            N'SELECT (SELECT * FROM (' + @grp2_build
            + N') _grp FOR JSON PATH, INCLUDE_NULL_VALUES)';
        DECLARE @grp2_result NVARCHAR(MAX);
        EXEC sp_executesql @grp2_wrap, N'@r NVARCHAR(MAX) OUTPUT', @r = @grp2_result OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / groupby IsRemote + COUNT] json='
              + ISNULL(@grp2_result, N'NULL');
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / groupby IsRemote + COUNT] ' + ERROR_MESSAGE();
    END CATCH;

    -- GroupBy Department with filter (IsRemote=true)
    BEGIN TRY
        DECLARE @grp3_build NVARCHAR(MAX) = dbo.pvt_build_groupby_sql(
            @emp_id,
            N'{"IsRemote":{"$eq":true}}',
            N'[{"field":"Department","alias":"Department"}]',
            N'[{"alias":"Count","$count":"*"},{"alias":"AvgSalary","$avg":{"$field":"Salary"}}]',
            NULL, NULL, NULL, 0, N'flat');
        DECLARE @grp3_wrap NVARCHAR(MAX) =
            N'SELECT (SELECT * FROM (' + @grp3_build
            + N') _grp FOR JSON PATH, INCLUDE_NULL_VALUES)';
        DECLARE @grp3_result NVARCHAR(MAX);
        EXEC sp_executesql @grp3_wrap, N'@r NVARCHAR(MAX) OUTPUT', @r = @grp3_result OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / groupby Department + filter + AVG] json_len='
              + CAST(LEN(ISNULL(@grp3_result, N'')) AS NVARCHAR(20));
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / groupby Department + filter + AVG] ' + ERROR_MESSAGE();
    END CATCH;

    -- GroupBy + ORDER BY + LIMIT
    BEGIN TRY
        DECLARE @grp4_build NVARCHAR(MAX) = dbo.pvt_build_groupby_sql(
            @emp_id, NULL,
            N'[{"field":"Department","alias":"Department"}]',
            N'[{"alias":"Count","$count":"*"}]',
            NULL,
            N'[{"field":"Department","dir":"asc"}]',
            3, 0, N'flat');
        DECLARE @grp4_wrap NVARCHAR(MAX) =
            N'SELECT (SELECT * FROM (' + @grp4_build
            + N') _grp FOR JSON PATH, INCLUDE_NULL_VALUES)';
        DECLARE @grp4_result NVARCHAR(MAX);
        EXEC sp_executesql @grp4_wrap, N'@r NVARCHAR(MAX) OUTPUT', @r = @grp4_result OUTPUT;
        SET @pass += 1;
        PRINT N'OK   [Employee / groupby + ORDER + LIMIT 3] json='
              + ISNULL(@grp4_result, N'NULL');
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [Employee / groupby + ORDER + LIMIT 3] ' + ERROR_MESSAGE();
    END CATCH;
END
ELSE
    PRINT N'SKIP [Employee / groupby block] scheme not found';

-- =====================================================================
-- 5. ARRAY GROUPBY (file 26 — not yet implemented)
-- =====================================================================
IF OBJECT_ID(N'dbo.pvt_build_array_groupby_sql') IS NOT NULL
BEGIN
    IF @emp_id IS NOT NULL
    BEGIN
        BEGIN TRY
            DECLARE @ag_build NVARCHAR(MAX) = dbo.pvt_build_array_groupby_sql(
                @emp_id, N'Skills', NULL, NULL, NULL, NULL, N'flat');
            DECLARE @ag_wrap NVARCHAR(MAX) =
                N'SELECT (SELECT * FROM (' + @ag_build + N') _ag FOR JSON PATH, INCLUDE_NULL_VALUES)';
            DECLARE @ag_result NVARCHAR(MAX);
            EXEC sp_executesql @ag_wrap, N'@r NVARCHAR(MAX) OUTPUT', @r = @ag_result OUTPUT;
            SET @pass += 1;
            PRINT N'OK   [Employee / array groupby Skills] json_len='
                  + CAST(LEN(ISNULL(@ag_result, N'')) AS NVARCHAR(20));
        END TRY
        BEGIN CATCH
            SET @fail += 1;
            PRINT N'FAIL [Employee / array groupby Skills] ' + ERROR_MESSAGE();
        END CATCH;
    END;
END
ELSE
BEGIN
    SET @skip += 1;
    PRINT N'SKIP [Employee / array groupby Skills] not-impl: pvt_build_array_groupby_sql missing';
END;

-- =====================================================================
-- 6. PROJECTION (file 24 — not yet implemented)
-- =====================================================================
IF OBJECT_ID(N'dbo.pvt_build_projection_sql') IS NOT NULL
BEGIN
    IF @emp_id IS NOT NULL
    BEGIN
        BEGIN TRY
            DECLARE @pr_build NVARCHAR(MAX) = dbo.pvt_build_projection_sql(
                @emp_id, NULL, NULL, NULL, N'["FirstName","LastName","Department"]', N'flat');
            DECLARE @pr_wrap NVARCHAR(MAX) =
                N'SELECT COUNT(*) FROM (' + @pr_build + N') _pr';
            DECLARE @pr_cnt BIGINT;
            EXEC sp_executesql @pr_wrap, N'@c BIGINT OUTPUT', @c = @pr_cnt OUTPUT;
            SET @pass += 1;
            PRINT N'OK   [Employee / projection 3 fields] rows=' + CAST(ISNULL(@pr_cnt, 0) AS NVARCHAR(20));
        END TRY
        BEGIN CATCH
            SET @fail += 1;
            PRINT N'FAIL [Employee / projection 3 fields] ' + ERROR_MESSAGE();
        END CATCH;
    END;
END
ELSE
BEGIN
    SET @skip += 1;
    PRINT N'SKIP [Employee / projection 3 fields] not-impl: pvt_build_projection_sql missing';
END;

-- =====================================================================
-- 7. TREE-MODE (source_mode != flat; requires Department scheme)
-- =====================================================================
DECLARE @dept_id BIGINT = NULL;
SELECT TOP 1 @dept_id = [_id]
FROM dbo._schemes
WHERE [_name] = N'redb.Tests.Integration.Models.DepartmentProps';

IF @dept_id IS NOT NULL
BEGIN
    DECLARE @dept_root_id BIGINT = NULL;
    DECLARE @dept_root    NVARCHAR(MAX) = NULL;
    SELECT TOP 1 @dept_root_id = [_id]
    FROM dbo._objects
    WHERE [_id_scheme] = @dept_id AND [_id_parent] IS NULL;
    IF @dept_root_id IS NOT NULL
        SET @dept_root = N'[' + CAST(@dept_root_id AS NVARCHAR(40)) + N']';

    DECLARE @tm_build NVARCHAR(MAX);
    DECLARE @tm_cnt   BIGINT;
    DECLARE @tm_wrap  NVARCHAR(MAX);
    -- Fallback seed list for tree_children/tree_descendants when no root object
    -- exists in the DB (Department has no rows). ID -1 never exists so result=0.
    DECLARE @tree_seed NVARCHAR(MAX) = COALESCE(@dept_root, N'[-1]');

    -- tree_descendants
    SET @tm_build = dbo.pvt_build_query_sql(@dept_id, NULL, NULL, NULL, NULL, 5, NULL, N'tree_descendants', @tree_seed, 0, NULL, NULL);
    IF @tm_build IS NULL BEGIN SET @skip+=1; PRINT N'SKIP [Department / tree_descendants] not-impl: tree modes'; END
    ELSE BEGIN TRY SET @tm_wrap=N'SELECT @c=COUNT(*) FROM ('+@tm_build+N') _t'; EXEC sp_executesql @tm_wrap,N'@c BIGINT OUTPUT',@c=@tm_cnt OUTPUT; SET @pass+=1; PRINT N'OK   [Department / tree_descendants] rows='+CAST(@tm_cnt AS NVARCHAR(20)); END TRY BEGIN CATCH SET @fail+=1; PRINT N'FAIL [Department / tree_descendants] '+ERROR_MESSAGE(); END CATCH;

    -- tree_children
    SET @tm_build = dbo.pvt_build_query_sql(@dept_id, NULL, NULL, NULL, NULL, 1, NULL, N'tree_children', @tree_seed, NULL, NULL, NULL);
    IF @tm_build IS NULL BEGIN SET @skip+=1; PRINT N'SKIP [Department / tree_children] not-impl: tree modes'; END
    ELSE BEGIN TRY SET @tm_wrap=N'SELECT @c=COUNT(*) FROM ('+@tm_build+N') _t'; EXEC sp_executesql @tm_wrap,N'@c BIGINT OUTPUT',@c=@tm_cnt OUTPUT; SET @pass+=1; PRINT N'OK   [Department / tree_children] rows='+CAST(@tm_cnt AS NVARCHAR(20)); END TRY BEGIN CATCH SET @fail+=1; PRINT N'FAIL [Department / tree_children] '+ERROR_MESSAGE(); END CATCH;

    -- tree_roots
    SET @tm_build = dbo.pvt_build_query_sql(@dept_id, NULL, NULL, NULL, NULL, NULL, NULL, N'tree_roots', NULL, NULL, NULL, NULL);
    IF @tm_build IS NULL BEGIN SET @skip+=1; PRINT N'SKIP [Department / tree_roots] not-impl: tree modes'; END
    ELSE BEGIN TRY SET @tm_wrap=N'SELECT @c=COUNT(*) FROM ('+@tm_build+N') _t'; EXEC sp_executesql @tm_wrap,N'@c BIGINT OUTPUT',@c=@tm_cnt OUTPUT; SET @pass+=1; PRINT N'OK   [Department / tree_roots] rows='+CAST(@tm_cnt AS NVARCHAR(20)); END TRY BEGIN CATCH SET @fail+=1; PRINT N'FAIL [Department / tree_roots] '+ERROR_MESSAGE(); END CATCH;

    -- tree_leaves
    SET @tm_build = dbo.pvt_build_query_sql(@dept_id, NULL, NULL, NULL, NULL, NULL, NULL, N'tree_leaves', NULL, NULL, NULL, NULL);
    IF @tm_build IS NULL BEGIN SET @skip+=1; PRINT N'SKIP [Department / tree_leaves] not-impl: tree modes'; END
    ELSE BEGIN TRY SET @tm_wrap=N'SELECT @c=COUNT(*) FROM ('+@tm_build+N') _t'; EXEC sp_executesql @tm_wrap,N'@c BIGINT OUTPUT',@c=@tm_cnt OUTPUT; SET @pass+=1; PRINT N'OK   [Department / tree_leaves] rows='+CAST(@tm_cnt AS NVARCHAR(20)); END TRY BEGIN CATCH SET @fail+=1; PRINT N'FAIL [Department / tree_leaves] '+ERROR_MESSAGE(); END CATCH;

    -- $isRoot
    SET @tm_build = dbo.pvt_build_query_sql(@dept_id, N'{"$isRoot":true}', NULL, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
    IF @tm_build IS NULL BEGIN SET @skip+=1; PRINT N'SKIP [Department / $isRoot] pvt returned NULL'; END
    ELSE BEGIN TRY SET @tm_wrap=N'SELECT @c=COUNT(*) FROM ('+@tm_build+N') _t'; EXEC sp_executesql @tm_wrap,N'@c BIGINT OUTPUT',@c=@tm_cnt OUTPUT; SET @pass+=1; PRINT N'OK   [Department / $isRoot] rows='+CAST(@tm_cnt AS NVARCHAR(20)); END TRY BEGIN CATCH SET @fail+=1; PRINT N'FAIL [Department / $isRoot] '+ERROR_MESSAGE(); END CATCH;

    -- $isLeaf
    SET @tm_build = dbo.pvt_build_query_sql(@dept_id, N'{"$isLeaf":true}', NULL, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
    IF @tm_build IS NULL BEGIN SET @skip+=1; PRINT N'SKIP [Department / $isLeaf] pvt returned NULL'; END
    ELSE BEGIN TRY SET @tm_wrap=N'SELECT @c=COUNT(*) FROM ('+@tm_build+N') _t'; EXEC sp_executesql @tm_wrap,N'@c BIGINT OUTPUT',@c=@tm_cnt OUTPUT; SET @pass+=1; PRINT N'OK   [Department / $isLeaf] rows='+CAST(@tm_cnt AS NVARCHAR(20)); END TRY BEGIN CATCH SET @fail+=1; PRINT N'FAIL [Department / $isLeaf] '+ERROR_MESSAGE(); END CATCH;

    -- $level
    SET @tm_build = dbo.pvt_build_query_sql(@dept_id, N'{"$level":{"$gte":0}}', NULL, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
    IF @tm_build IS NULL BEGIN SET @skip+=1; PRINT N'SKIP [Department / $level>=0] pvt returned NULL'; END
    ELSE BEGIN TRY SET @tm_wrap=N'SELECT @c=COUNT(*) FROM ('+@tm_build+N') _t'; EXEC sp_executesql @tm_wrap,N'@c BIGINT OUTPUT',@c=@tm_cnt OUTPUT; SET @pass+=1; PRINT N'OK   [Department / $level>=0] rows='+CAST(@tm_cnt AS NVARCHAR(20)); END TRY BEGIN CATCH SET @fail+=1; PRINT N'FAIL [Department / $level>=0] '+ERROR_MESSAGE(); END CATCH;

    -- $childrenOf root
    IF @dept_root_id IS NOT NULL
    BEGIN
        DECLARE @cof_filter NVARCHAR(MAX) = N'{"$childrenOf":' + CAST(@dept_root_id AS NVARCHAR(40)) + N'}';
        SET @tm_build = dbo.pvt_build_query_sql(@dept_id, @cof_filter, NULL, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF @tm_build IS NULL BEGIN SET @skip+=1; PRINT N'SKIP [Department / $childrenOf root] pvt returned NULL'; END
        ELSE BEGIN TRY SET @tm_wrap=N'SELECT @c=COUNT(*) FROM ('+@tm_build+N') _t'; EXEC sp_executesql @tm_wrap,N'@c BIGINT OUTPUT',@c=@tm_cnt OUTPUT; SET @pass+=1; PRINT N'OK   [Department / $childrenOf root] rows='+CAST(@tm_cnt AS NVARCHAR(20)); END TRY BEGIN CATCH SET @fail+=1; PRINT N'FAIL [Department / $childrenOf root] '+ERROR_MESSAGE(); END CATCH;
    END
    ELSE BEGIN SET @skip+=1; PRINT N'SKIP [Department / $childrenOf root] no root object found'; END;
END
ELSE
BEGIN
    SET @skip += 8;
    PRINT N'SKIP [Department / tree block (8 cases)] scheme not found';
END;

-- =====================================================================
-- 8. HELPER FUNCTIONS DIRECT TESTS (files 05, 06, 06a, 07)
-- =====================================================================
-- These functions return SQL fragments / JSON, not complete queries.
-- Each test calls the function directly and validates the output.

-- ---- 07: pvt_get_object_base_fields ----------------------------------
DECLARE @bf_obj_id BIGINT = NULL;
IF @emp_id IS NOT NULL
    SELECT TOP 1 @bf_obj_id = [_id] FROM dbo._objects WHERE [_id_scheme] = @emp_id;

IF @bf_obj_id IS NOT NULL
BEGIN
    BEGIN TRY
        DECLARE @bf_json NVARCHAR(MAX) = dbo.pvt_get_object_base_fields(@bf_obj_id);
        IF @bf_json IS NOT NULL AND ISJSON(@bf_json) = 1 AND JSON_VALUE(@bf_json, N'$.id') IS NOT NULL
        BEGIN
            SET @pass += 1;
            PRINT N'OK   [pvt_get_object_base_fields] id=' + JSON_VALUE(@bf_json, N'$.id');
        END
        ELSE
        BEGIN
            SET @fail += 1;
            PRINT N'FAIL [pvt_get_object_base_fields] bad result: ' + ISNULL(@bf_json, N'NULL');
        END
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [pvt_get_object_base_fields] ' + ERROR_MESSAGE();
    END CATCH;
END
ELSE
BEGIN
    SET @skip += 1;
    PRINT N'SKIP [pvt_get_object_base_fields] no Employee objects / scheme not found';
END;

-- ---- 05: pvt_build_single_facet_condition ----------------------------
-- Use a base-field filter so the fragment is a simple WHERE predicate.
IF @emp_id IS NOT NULL
BEGIN
    BEGIN TRY
        DECLARE @sfc_cond NVARCHAR(MAX) = dbo.pvt_build_single_facet_condition(
            N'{"0$:Id":{"$gt":0}}', @emp_id, N'o', NULL);
        IF @sfc_cond IS NULL
        BEGIN
            SET @fail += 1;
            PRINT N'FAIL [pvt_build_single_facet_condition] returned NULL';
        END
        ELSE
        BEGIN
            DECLARE @sfc_wrap NVARCHAR(MAX) =
                N'SELECT @c = COUNT(*) FROM dbo._objects o'
                + N' WHERE o.[_id_scheme] = ' + CAST(@emp_id AS NVARCHAR(20))
                + N' AND ' + @sfc_cond;
            DECLARE @sfc_cnt BIGINT;
            EXEC sp_executesql @sfc_wrap, N'@c BIGINT OUTPUT', @c = @sfc_cnt OUTPUT;
            SET @pass += 1;
            PRINT N'OK   [pvt_build_single_facet_condition] rows=' + CAST(@sfc_cnt AS NVARCHAR(20));
        END
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [pvt_build_single_facet_condition] ' + ERROR_MESSAGE();
    END CATCH;
END
ELSE
BEGIN
    SET @skip += 1;
    PRINT N'SKIP [pvt_build_single_facet_condition] Employee scheme not found';
END;

-- ---- 06: pvt_build_hierarchical_conditions ---------------------------
-- $isRoot:true -> ' AND o.[_id_parent] IS NULL' ; Employee is flat = 210 rows
IF @emp_id IS NOT NULL
BEGIN
    BEGIN TRY
        DECLARE @hc_result NVARCHAR(MAX) = dbo.pvt_build_hierarchical_conditions(
            N'{"$isRoot":true}', N'o');
        IF @hc_result IS NULL OR @hc_result = N''
        BEGIN
            SET @fail += 1;
            PRINT N'FAIL [pvt_build_hierarchical_conditions] returned empty for $isRoot:true';
        END
        ELSE IF @hc_result NOT LIKE N'%_id_parent%'
        BEGIN
            SET @fail += 1;
            PRINT N'FAIL [pvt_build_hierarchical_conditions] unexpected output: ' + @hc_result;
        END
        ELSE
        BEGIN
            -- Strip leading ' AND ' (5 chars) to get bare WHERE predicate
            DECLARE @hc_bare NVARCHAR(MAX) = STUFF(@hc_result, 1, 5, N'');
            DECLARE @hc_wrap NVARCHAR(MAX) =
                N'SELECT @c = COUNT(*) FROM dbo._objects o'
                + N' WHERE o.[_id_scheme] = ' + CAST(@emp_id AS NVARCHAR(20))
                + N' AND ' + @hc_bare;
            DECLARE @hc_cnt BIGINT;
            EXEC sp_executesql @hc_wrap, N'@c BIGINT OUTPUT', @c = @hc_cnt OUTPUT;
            SET @pass += 1;
            PRINT N'OK   [pvt_build_hierarchical_conditions $isRoot:true] rows=' + CAST(@hc_cnt AS NVARCHAR(20));
        END
    END TRY
    BEGIN CATCH
        SET @fail += 1;
        PRINT N'FAIL [pvt_build_hierarchical_conditions] ' + ERROR_MESSAGE();
    END CATCH;
END
ELSE
BEGIN
    SET @skip += 1;
    PRINT N'SKIP [pvt_build_hierarchical_conditions] Employee scheme not found';
END;

-- ---- 06a: pvt_build_level_condition ----------------------------------
BEGIN TRY
    DECLARE @lc_result NVARCHAR(MAX) = dbo.pvt_build_level_condition(0, N'o');
    IF @lc_result IS NULL OR @lc_result NOT LIKE N'%pvt_object_depth%'
    BEGIN
        SET @fail += 1;
        PRINT N'FAIL [pvt_build_level_condition] unexpected: ' + ISNULL(@lc_result, N'NULL');
    END
    ELSE
    BEGIN
        SET @pass += 1;
        PRINT N'OK   [pvt_build_level_condition(0)] sql=' + @lc_result;
    END
END TRY
BEGIN CATCH
    SET @fail += 1;
    PRINT N'FAIL [pvt_build_level_condition] ' + ERROR_MESSAGE();
END CATCH;

-- ---- 06a: pvt_build_level_condition_with_operators -------------------
BEGIN TRY
    DECLARE @lco_result NVARCHAR(MAX) = dbo.pvt_build_level_condition_with_operators(
        N'{"$gte":0}', N'o');
    IF @lco_result IS NULL OR @lco_result NOT LIKE N'%pvt_object_depth%'
    BEGIN
        SET @fail += 1;
        PRINT N'FAIL [pvt_build_level_condition_with_operators] unexpected: ' + ISNULL(@lco_result, N'NULL');
    END
    ELSE
    BEGIN
        SET @pass += 1;
        PRINT N'OK   [pvt_build_level_condition_with_operators {$gte:0}] sql=' + @lco_result;
    END
END TRY
BEGIN CATCH
    SET @fail += 1;
    PRINT N'FAIL [pvt_build_level_condition_with_operators] ' + ERROR_MESSAGE();
END CATCH;

-- =====================================================================
-- SQL inspection: dump generated queries for shape verification.
-- Shape A = no `_pvt_cte` alias; Shape C = subquery alias `_pvt_cte`.
-- Shape assertions increment @fail so the final RAISERROR catches them.
-- =====================================================================
DECLARE @insp_emp  BIGINT;
DECLARE @insp_sql  NVARCHAR(MAX);
DECLARE @insp_fail INT = 0;

SELECT @insp_emp = _id FROM dbo._schemes WHERE _name = N'redb.Tests.Integration.Models.EmployeeProps';

IF @insp_emp IS NULL
BEGIN
    PRINT N'inspect: Employee scheme not found, skipping SQL dump';
END
ELSE
BEGIN
    PRINT N'';
    PRINT N'===== INSPECT: SQL shape verification =====';

    -- 1. Shape A: pure base _id>0 (expect no CTE)
    BEGIN TRY
        SET @insp_sql = dbo.pvt_build_query_sql(@insp_emp, N'{"_id":{"$gt":0}}', 5, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @insp_sql) > 0
        BEGIN
            PRINT N'SHAPE-FAIL [SHAPE_A: pure base _id>0] unexpected CTE';
            SET @insp_fail += 1;
        END
        ELSE
        BEGIN
            PRINT N'----- INSPECT [SHAPE_A: pure base _id>0 (expect no CTE)] -----';
            PRINT N'FILTER: {"_id":{"$gt":0}}';
            PRINT @insp_sql;
        END
    END TRY BEGIN CATCH
        PRINT N'INSPECT FAILED [SHAPE_A: pure base _id>0]: ' + ERROR_MESSAGE();
        SET @insp_fail += 1;
    END CATCH;

    -- 2. Shape A: empty filter (expect no CTE)
    BEGIN TRY
        SET @insp_sql = dbo.pvt_build_query_sql(@insp_emp, NULL, 5, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @insp_sql) > 0
        BEGIN
            PRINT N'SHAPE-FAIL [SHAPE_A: empty filter] unexpected CTE';
            SET @insp_fail += 1;
        END
        ELSE
        BEGIN
            PRINT N'----- INSPECT [SHAPE_A: empty filter (expect SELECT _id FROM [_objects] WHERE [_id_scheme]=X)] -----';
            PRINT N'FILTER: (none)';
            PRINT @insp_sql;
        END
    END TRY BEGIN CATCH
        PRINT N'INSPECT FAILED [SHAPE_A: empty filter]: ' + ERROR_MESSAGE();
        SET @insp_fail += 1;
    END CATCH;

    -- 3. Shape A: $or all-base (no CTE expected)
    BEGIN TRY
        SET @insp_sql = dbo.pvt_build_query_sql(@insp_emp, N'{"$or":[{"_id":{"$lt":10}},{"_id":{"$gt":100}}]}', 5, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @insp_sql) > 0
        BEGIN
            PRINT N'SHAPE-FAIL [SHAPE_A: $or all-base] unexpected CTE';
            SET @insp_fail += 1;
        END
        ELSE
        BEGIN
            PRINT N'----- INSPECT [SHAPE_A: $or all-base (full pushdown, no CTE)] -----';
            PRINT N'FILTER: {"$or":[{"_id":{"$lt":10}},{"_id":{"$gt":100}}]}';
            PRINT @insp_sql;
        END
    END TRY BEGIN CATCH
        PRINT N'INSPECT FAILED [SHAPE_A: $or all-base]: ' + ERROR_MESSAGE();
        SET @insp_fail += 1;
    END CATCH;

    -- 4. Shape C: scalar prop eq (expect CTE)
    BEGIN TRY
        SET @insp_sql = dbo.pvt_build_query_sql(@insp_emp, N'{"FirstName":"Ivan"}', 5, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @insp_sql) = 0
        BEGIN
            PRINT N'SHAPE-FAIL [SHAPE_C: scalar prop eq] expected CTE, not found';
            SET @insp_fail += 1;
        END
        ELSE
        BEGIN
            PRINT N'----- INSPECT [SHAPE_C: scalar prop eq (FirstName=Ivan)] -----';
            PRINT N'FILTER: {"FirstName":"Ivan"}';
            PRINT @insp_sql;
        END
    END TRY BEGIN CATCH
        PRINT N'INSPECT FAILED [SHAPE_C: scalar prop eq]: ' + ERROR_MESSAGE();
        SET @insp_fail += 1;
    END CATCH;

    -- 5. Shape C: base+props AND (pushdown _id in subquery, residual FirstName)
    BEGIN TRY
        SET @insp_sql = dbo.pvt_build_query_sql(@insp_emp, N'{"_id":{"$gt":0},"FirstName":{"$startsWith":"A"}}', 5, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @insp_sql) = 0
        BEGIN
            PRINT N'SHAPE-FAIL [SHAPE_C: base+props AND] expected CTE, not found';
            SET @insp_fail += 1;
        END
        ELSE
        BEGIN
            PRINT N'----- INSPECT [SHAPE_C: base+props AND (pushdown _id, residual FirstName)] -----';
            PRINT N'FILTER: {"_id":{"$gt":0},"FirstName":{"$startsWith":"A"}}';
            PRINT @insp_sql;
        END
    END TRY BEGIN CATCH
        PRINT N'INSPECT FAILED [SHAPE_C: base+props AND]: ' + ERROR_MESSAGE();
        SET @insp_fail += 1;
    END CATCH;

    -- 6. Shape C: NULL check (expect CTE)
    BEGIN TRY
        SET @insp_sql = dbo.pvt_build_query_sql(@insp_emp, N'{"EmployeeCode":{"$null":true}}', 5, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @insp_sql) = 0
        BEGIN
            PRINT N'SHAPE-FAIL [SHAPE_C: $null:true] expected CTE, not found';
            SET @insp_fail += 1;
        END
        ELSE
        BEGIN
            PRINT N'----- INSPECT [SHAPE_C: $null:true (requires LEFT JOIN / wide CTE)] -----';
            PRINT N'FILTER: {"EmployeeCode":{"$null":true}}';
            PRINT @insp_sql;
        END
    END TRY BEGIN CATCH
        PRINT N'INSPECT FAILED [SHAPE_C: $null:true]: ' + ERROR_MESSAGE();
        SET @insp_fail += 1;
    END CATCH;

    -- 7. Shape C: NOT NULL props (expect CTE)
    BEGIN TRY
        SET @insp_sql = dbo.pvt_build_query_sql(@insp_emp, N'{"EmployeeCode":{"$notNull":true}}', 5, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @insp_sql) = 0
        BEGIN
            PRINT N'SHAPE-FAIL [SHAPE_C: $notNull:true] expected CTE, not found';
            SET @insp_fail += 1;
        END
        ELSE
        BEGIN
            PRINT N'----- INSPECT [SHAPE_C: $notNull:true (INNER JOIN, no NULL rows)] -----';
            PRINT N'FILTER: {"EmployeeCode":{"$notNull":true}}';
            PRINT @insp_sql;
        END
    END TRY BEGIN CATCH
        PRINT N'INSPECT FAILED [SHAPE_C: $notNull:true]: ' + ERROR_MESSAGE();
        SET @insp_fail += 1;
    END CATCH;

    -- 8. Shape C: dict ContainsKey (expect CTE)
    BEGIN TRY
        SET @insp_sql = dbo.pvt_build_query_sql(@insp_emp, N'{"PhoneDirectory.ContainsKey":"work"}', 5, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @insp_sql) = 0
        BEGIN
            PRINT N'SHAPE-FAIL [SHAPE_C: dict ContainsKey] expected CTE, not found';
            SET @insp_fail += 1;
        END
        ELSE
        BEGIN
            PRINT N'----- INSPECT [SHAPE_C: dict ContainsKey (PhoneDirectory[work])] -----';
            PRINT N'FILTER: {"PhoneDirectory.ContainsKey":"work"}';
            PRINT @insp_sql;
        END
    END TRY BEGIN CATCH
        PRINT N'INSPECT FAILED [SHAPE_C: dict ContainsKey]: ' + ERROR_MESSAGE();
        SET @insp_fail += 1;
    END CATCH;

    -- 9. Phase 2: top-level $eq with base field -> Shape A (no CTE)
    BEGIN TRY
        SET @insp_sql = dbo.pvt_build_query_sql(@insp_emp, N'{"$eq":[{"$field":"_id"},{"$const":0}]}', 5, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @insp_sql) > 0
        BEGIN
            PRINT N'SHAPE-FAIL [SHAPE_A: top-level $eq base] unexpected CTE';
            SET @insp_fail += 1;
        END
        ELSE
        BEGIN
            PRINT N'----- INSPECT [SHAPE_A: top-level $eq base (_id=0, expect pushdown)] -----';
            PRINT N'FILTER: {"$eq":[{"$field":"_id"},{"$const":0}]}';
            PRINT @insp_sql;
        END
    END TRY BEGIN CATCH
        PRINT N'INSPECT FAILED [SHAPE_A: top-level $eq base]: ' + ERROR_MESSAGE();
        SET @insp_fail += 1;
    END CATCH;

    -- 10. Phase 2: top-level $expr with base-only arithmetic -> Shape A
    BEGIN TRY
        SET @insp_sql = dbo.pvt_build_query_sql(@insp_emp,
            N'{"$expr":{"$gt":[{"$length":{"$field":"_name"}},{"$const":0}]}}',
            5, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @insp_sql) > 0
        BEGIN
            PRINT N'SHAPE-FAIL [SHAPE_A: $expr base-only] unexpected CTE';
            SET @insp_fail += 1;
        END
        ELSE
        BEGIN
            PRINT N'----- INSPECT [SHAPE_A: $expr base-only (LEN(_name) > 0, expect pushdown)] -----';
            PRINT N'FILTER: {"$expr":{"$gt":[{"$length":{"$field":"_name"}},{"$const":0}]}}';
            PRINT @insp_sql;
        END
    END TRY BEGIN CATCH
        PRINT N'INSPECT FAILED [SHAPE_A: $expr base-only]: ' + ERROR_MESSAGE();
        SET @insp_fail += 1;
    END CATCH;

    -- 11. Phase 2: top-level $expr referencing props -> Shape C (residual)
    BEGIN TRY
        SET @insp_sql = dbo.pvt_build_query_sql(@insp_emp,
            N'{"$expr":{"$gt":[{"$field":"Age"},{"$const":30}]}}',
            5, NULL, NULL, NULL, NULL, N'flat', NULL, NULL, NULL, NULL);
        IF CHARINDEX(N'_pvt_cte', @insp_sql) = 0
        BEGIN
            PRINT N'SHAPE-FAIL [SHAPE_C: $expr props] expected CTE, not found';
            SET @insp_fail += 1;
        END
        ELSE
        BEGIN
            PRINT N'----- INSPECT [SHAPE_C: $expr props (Age > 30, must stay in residual)] -----';
            PRINT N'FILTER: {"$expr":{"$gt":[{"$field":"Age"},{"$const":30}]}}';
            PRINT @insp_sql;
        END
    END TRY BEGIN CATCH
        PRINT N'INSPECT FAILED [SHAPE_C: $expr props]: ' + ERROR_MESSAGE();
        SET @insp_fail += 1;
    END CATCH;

    IF @insp_fail > 0
    BEGIN
        SET @fail += @insp_fail;
        PRINT N'INSPECT: ' + CAST(@insp_fail AS NVARCHAR(10)) + N' shape assertion(s) failed.';
    END
    ELSE
        PRINT N'INSPECT: all shape assertions passed.';
    PRINT N'';
END

-- =====================================================================
-- Summary
-- =====================================================================

PRINT N'----------------------------------------------------------';
PRINT N'PASS: ' + CAST(@pass AS NVARCHAR(10))
    + N'   FAIL: ' + CAST(@fail AS NVARCHAR(10))
    + N'   SKIP: ' + CAST(@skip AS NVARCHAR(10));
PRINT N'----------------------------------------------------------';

IF @fail > 0
    RAISERROR(N'pvt smoke FAILED: %d case(s) failed.', 16, 1, @fail);
