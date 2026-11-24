# Upgrading redb

What to check and change when moving to a new version. Each section lists the changes that need action;
[CHANGELOG.md](CHANGELOG.md) has the full list of what changed.

## 4.7.5

### TimeSpan and TimeOnly are stored as ticks

`TimeSpan` and `TimeOnly` values are stored as their `Ticks` in the number column of `_values` (`_Long`), no longer as
text. They now compare, sort, group and take `Min`/`Max` correctly on every provider; as text, a value over a day and a
negative value compared and sorted wrong (`23:00:00` above `1.00:00:00`, `10.00:00:00` below `2.00:00:00`).

Read this before you upgrade a database that holds `TimeSpan` or `TimeOnly` values:

- **Every node and every image of the database runs 4.7.5**: the applications, and the Tsak and Identity images and
  archives, which carry the redb packages in `Libs/shared`. A node of 4.7.4 or older writes the values as text again,
  and at its start puts its own database module back over the new one. Packages published before 4.7.5 do not know the
  rule of the next section.
- **There is no way back except a backup.** Once the values are converted, a package before 4.7.5 does not read them.
- **Convert the stored values before the first start of 4.7.5**: stop every node, back up the database, run the
  statement for your database below, then start the nodes on 4.7.5. The statements work on a database of an older
  version, as they find the fields by their type name.

The schema follows by itself at the first start: the type rows of the two types say `Long` (on PostgreSQL and SQL Server
the SQL module does it, or `redb schema --upgrade`; on SQLite the file upgrade). **Values still stored as text are not
converted by the start.** Such a field reads as `00:00:00` (Pro) or fails to load (Free), a nullable one as `null`, and
an array of them as empty; saving the object again writes those empty values.

PostgreSQL:

```sql
UPDATE _values v
SET _Long = x.ticks, _String = NULL
FROM (SELECT o._id,
             CASE WHEN m[1] = '-' THEN -1 ELSE 1 END * (
                 (COALESCE(m[2], '0')::bigint * 86400 + m[3]::bigint * 3600 + m[4]::bigint * 60 + m[5]::bigint) * 10000000
                 + rpad(COALESCE(m[6], ''), 7, '0')::bigint) AS ticks
      FROM _values o
      JOIN _structures s ON s._id = o._id_structure
      JOIN _types t ON t._id = s._id_type AND t._name IN ('TimeSpan', 'TimeOnly')
      CROSS JOIN LATERAL regexp_match(o._String, '^(-)?(?:(\d+)\.)?(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d{1,7}))?$') m
      WHERE o._String IS NOT NULL AND m IS NOT NULL) x
WHERE v._id = x._id;
```

SQL Server:

```sql
UPDATE v SET _Long = x.ticks, _String = NULL
FROM _values v
JOIN _structures s ON s._id = v._id_structure
JOIN _types t ON t._id = s._id_type AND t._name IN (N'TimeSpan', N'TimeOnly')
CROSS APPLY (SELECT IIF(LEFT(v._String, 1) = N'-', 1, 0) AS neg,
                    IIF(LEFT(v._String, 1) = N'-', STUFF(v._String, 1, 1, N''), v._String) AS body) b
CROSS APPLY (SELECT IIF(CHARINDEX(N'.', b.body) BETWEEN 1 AND CHARINDEX(N':', b.body),
                        CAST(LEFT(b.body, CHARINDEX(N'.', b.body) - 1) AS bigint), 0) AS days,
                    IIF(CHARINDEX(N'.', b.body) BETWEEN 1 AND CHARINDEX(N':', b.body),
                        STUFF(b.body, 1, CHARINDEX(N'.', b.body), N''), b.body) AS rest) d
CROSS APPLY (SELECT (d.days * 864000000000
                     + DATEDIFF_BIG(NANOSECOND, CAST('00:00' AS time(7)), TRY_CAST(d.rest AS time(7))) / 100)
                    * IIF(b.neg = 1, -1, 1) AS ticks) x
WHERE v._String IS NOT NULL AND TRY_CAST(d.rest AS time(7)) IS NOT NULL;
```

SQLite:

```sql
UPDATE _values SET
    _Long = (SELECT sign * (days * 864000000000
                 + (CAST(substr(rest, 1, 2) AS INTEGER) * 3600 + CAST(substr(rest, 4, 2) AS INTEGER) * 60
                    + CAST(substr(rest, 7, 2) AS INTEGER)) * 10000000
                 + CAST(substr(substr(rest, 10) || '0000000', 1, 7) AS INTEGER))
             FROM (SELECT sign,
                          CASE WHEN instr(b, '.') BETWEEN 1 AND instr(b, ':') THEN CAST(substr(b, 1, instr(b, '.') - 1) AS INTEGER) ELSE 0 END AS days,
                          CASE WHEN instr(b, '.') BETWEEN 1 AND instr(b, ':') THEN substr(b, instr(b, '.') + 1) ELSE b END AS rest
                   FROM (SELECT CASE WHEN substr(_values._String, 1, 1) = '-' THEN -1 ELSE 1 END AS sign,
                                CASE WHEN substr(_values._String, 1, 1) = '-' THEN substr(_values._String, 2) ELSE _values._String END AS b))),
    _String = NULL
WHERE _String IS NOT NULL
  AND _String GLOB '*[0-9][0-9]:[0-9][0-9]:[0-9][0-9]*'
  AND _id_structure IN (SELECT s._id FROM _structures s JOIN _types t ON t._id = s._id_type
                        WHERE t._name IN ('TimeSpan', 'TimeOnly'));
```

The statements read the forms redb has written: `[-][d.]hh:mm:ss[.fffffff]` for `TimeSpan` and `hh:mm:ss[.fffffff]`
for `TimeOnly`. A value they cannot read stays as it was; this lists them:

```sql
SELECT v._id, v._String FROM _values v
JOIN _structures s ON s._id = v._id_structure JOIN _types t ON t._id = s._id_type
WHERE t._name IN ('TimeSpan', 'TimeOnly') AND v._String IS NOT NULL;
```

### A newer database module is left in place

From 4.7.5 on a start applies the module of its own package when the database holds an older one, as before, and no
longer over a newer one. The newer module states which packages it still serves (`pvt_module_compat()` on PostgreSQL
and SQL Server, the `_schema_compat` table of a SQLite file): a package among them starts and logs one warning, one that
is not stops with `RedbSchemaNewerException`, which names the package version to upgrade to, and changes nothing. The
upgrade script run by hand (`GetUpgradeScript()`, `redb schema --upgrade`) refuses a newer module the same way before
its first change. The module goes to `0.7.23.1` on PostgreSQL and `0.2.40.2` on SQL Server, a SQLite file to schema
version 13. The 4.7.5 module serves 4.7.5 and later packages only, because of the ticks above.

Action: none for an application. A host that tells start-up failures apart can catch `RedbSchemaNewerException`: the
application, not the database, needs upgrading. Packages published before 4.7.5 do not know the rule and still put
their own module back, which is why every node runs 4.7.5.

### Custom dialects: Query_WrapPvtWithObjectRows

Only for code that implements `ISqlDialect` itself; the dialects redb ships implement it. A Free list read now reads
the `_objects` rows of the objects it finds and loads their Props once, so `ISqlDialect` has one more member,
`Query_WrapPvtWithObjectRows(string innerSql)`: wrap the inner id list so that it returns the `_objects` row of every
id, in the order of the inner list, with the columns of `ObjectStorage_SelectObjectsByIds`. A dialect without the
v2-pvt module returns `null`. `Query_PvtModuleCompatFunction()` is new too and has a default (`null`, no compatibility
function).

### SQL Server: a delete counts the requested objects only

`DeleteAsync` over ids or objects returns the number of the requested objects it deleted, as on PostgreSQL and SQLite;
on SQL Server it used to add every descendant removed with them. Code that compared that number with the size of a
subtree reads the count of `DeleteSubtreeAsync` instead.

## 4.7.3

### SQL Server: a database hit by a second redb init

Up to this release, `redb init -p mssql` run on a database that already held the redb schema dropped every foreign key
and the id sequence `global_identity`, then stopped with "There is already an object named '_types'". The data stayed;
the next import failed at its end on `ALTER SEQUENCE`, and an application failed on its first new id. `redb init` now
refuses such a database before it runs anything. The SQL Server script itself, as printed by `redb schema` and as run
by `redb init`, now stops at its first statement on such a database too, also when a DBA runs it with sqlcmd or SSMS.
The script reports that refusal as a message: sqlcmd without `-b` still exits with 0, so a script that runs it must read
the output or call sqlcmd with `-b`.
PostgreSQL and SQLite were not damaged by a second run.

Check every SQL Server database on which `redb init` may have run twice:

```sql
SELECT name FROM sys.sequences;          -- must list global_identity
SELECT COUNT(*) FROM sys.foreign_keys;   -- must be 30
```

If the sequence is missing or the count is lower, rebuild the database from an export. Stop the applications that use
it first.

1. `redb export -p mssql -c "<damaged database>" -o backup.redb`
2. Create a new empty database and run `redb init -p mssql -c "<new database>"` once.
3. `redb import -p mssql -c "<new database>" -i backup.redb --clean`
4. Restart the id sequence past the imported ids. The export of a damaged database carries no sequence value, so the
   import leaves the new sequence at its start, below the imported ids:

   ```sql
   DECLARE @sql nvarchar(max) = (
       SELECT N'SELECT @max = MAX(id) FROM (' + STRING_AGG(CAST(N'SELECT MAX(_id) AS id FROM dbo.' + QUOTENAME(t.name) AS nvarchar(max)), N' UNION ALL ') + N') x'
       FROM sys.tables t JOIN sys.columns c ON c.object_id = t.object_id AND c.name = N'_id'
       WHERE t.schema_id = SCHEMA_ID(N'dbo'));
   DECLARE @max bigint;
   EXEC sp_executesql @sql, N'@max bigint OUTPUT', @max = @max OUTPUT;
   DECLARE @restart nvarchar(200) = N'ALTER SEQUENCE dbo.global_identity RESTART WITH '
       + CAST(CASE WHEN @max >= 1000000 THEN @max + 1 ELSE 1000000 END AS nvarchar(20));
   EXEC (@restart);
   ```

5. Check the new database with the two queries above, then point the applications at it.

## 4.6.0

### Grouping: refusals instead of empty or default answers

GroupBy, GroupByArray, their windows and their tree forms now throw where they used to answer with nothing or with a
value that was not asked for:

- `NotSupportedException` for a form that is not translated: a computed key or aggregate (`Agg.Sum(g, x => x.A * 2)`,
  `c => c.Name.Length`), `SelectMany` with a result selector or a dictionary in the array path of `GroupByArray`, a
  class as a key, an array of list items or object references, a string method in the condition of `Agg.Count`. A
  computed aggregate used to become `*`; an array path that was not a chain of fields became empty.
- `InvalidOperationException` when a provider returns no array of rows, or a row without a column the selector reads.
  A missing column used to become the default value (0 for a number), which hid NULL keys dropped by SQL Server.
- `ArgumentException` for a request without keys, where providers returned null and the query an empty list.

`Having(...).WithWindow(...)` now keeps the HAVING (it was dropped), and `Win.Ntile(n)` uses `n` (it was always 4).
A tree query's `GroupByArray` groups only its subtree (it grouped the whole scheme).

Action: none for queries in the supported forms. A query that now throws was returning a wrong answer; rewrite the
computation after the query, or group by a stored field.

### Custom query providers: one method for array groupings

Only for code that implements `IRedbQueryProvider` or `IRedbQueryable` itself:

- The four `ExecuteArrayGroupedAggregateAsync` overloads are replaced by `ExecuteArrayGroupQueryAsync(ArrayGroupQuery)`
  and `GetArrayGroupQuerySqlAsync(ArrayGroupQuery)`; tree providers add `ExecuteTreeArrayGroupQueryAsync` and
  `GetTreeArrayGroupQuerySqlAsync`. `ArrayGroupQuery` carries the array path, keys, aggregates, condition, HAVING and
  window; call `EnsureValid()` first.
- `GetGroupBySqlPreviewAsync` (two overloads) is part of `IRedbQueryProvider`; the grouped window methods
  (`ExecuteGroupedWindowQueryAsync`, `GetGroupedWindowSqlPreviewAsync` and their tree forms) take `havingJson`.
- `IRedbQueryable.GroupByArray<TItem, TKey>` and the grouping interfaces (`IRedbGrouping`, `IRedbGroupedQueryable`,
  `IGroupedWindowedQueryable`, `IGroupedWindowSpec`) no longer constrain the item type to `class, new()`: delete the
  constraint from an implementation, or it does not compile.
- `ISqlDialect.Query_AggregateArrayGroupedSql` is removed (every dialect threw from it).

### Custom list providers: GetListItemsByIdsAsync

Only for code that implements `IListProvider` or `ISqlDialect` itself:

- `IListProvider.GetListItemsByIdsAsync(IEnumerable<long> itemIds, CancellationToken)` returns the list items of the
  given ids: an id that names no item is left out, the order is free. A grouping whose key is a list item calls it
  once per result to turn the ids it grouped by into the items, so read them in one query.
- `ISqlDialect.ListItems_SelectByIds()` is the statement the stock list provider runs for it: the columns of
  `ListItems_SelectById` for an array of ids passed as the first parameter.

### Custom contexts: QueryRowsAsJsonAsync

Only for code that implements `IRedbContext` or derives from `RedbContextBase` itself:

- `IRedbContext.QueryRowsAsJsonAsync(string sql, object[] parameters, CancellationToken)` returns the rows of `sql` as
  a JSON array of objects keyed by column name, NULL values included, `[]` when there are none. It is one command and
  one telemetry span, like the other methods of the context. The SQLite providers read their grouping, window and
  aggregate results through it; they used to read them on the raw connection, which opened no span.
- `RedbContextBase` implements it inside the span over a new `protected abstract Task<string>
  QueryRowsAsJsonCoreAsync(string sql, object[] parameters, CancellationToken)`. A context derived from it implements
  that method on its connection: read every row and serialize each as an object of its columns. The stock PostgreSQL,
  SQL Server and SQLite contexts do so.
- A context that wraps another one (a decorator) forwards the call.

## 4.4.2

### Duplicate root structures

Nodes that synchronized one scheme at the same moment could write one root field of the scheme twice on PostgreSQL
and SQLite (SQL Server refused it). Scheme synchronization now runs under the schema lock, and a new unique index,
`UX__structures__root`, keeps one root field name per scheme. New databases get the index at creation. An existing
database gets it at the next module upgrade (PostgreSQL) or at the next start (SQLite), and duplicates are handled first:

- A duplicate that nothing points to (no value, no child structure with a value, no migration record) is removed
  with its child structures. The log names the scheme, the field and the removed ids.
- A field whose copies hold values on more than one copy is left alone, and the index is not built. The service
  keeps working, logs an error naming the scheme, the field and the structure ids at every start, and lists the field
  in `IRedbService.SchemaProblems` (a health check can show the service as degraded). Scheme synchronization never
  removes such a copy; it reports it the same way. The index is built by the first upgrade (PostgreSQL) or start
  (SQLite) after the duplicates are resolved.

Nothing is needed when no such error appears. To find the duplicates and the values on each copy:

```sql
SELECT s._id_scheme, s._name, s._id,
       (SELECT COUNT(*) FROM _values v WHERE v._id_structure = s._id) AS values_count
FROM _structures s
JOIN (SELECT _id_scheme, _name FROM _structures WHERE _id_parent IS NULL
      GROUP BY _id_scheme, _name HAVING COUNT(*) > 1) d
  ON d._id_scheme = s._id_scheme AND d._name = s._name
WHERE s._id_parent IS NULL
ORDER BY s._id_scheme, s._name, s._id;
```

Each object normally has its value on one copy only: the copy the node that saved it had in its cache. To resolve a
field, keep one copy (`:keep`) and move to it the values the other copy (`:dup`) holds for objects that have none on
`:keep`. Then delete `:dup`; deleting a structure deletes the values still on it. Make a backup first, and stop the
nodes of versions before this one: they may still write into the copy they cached.

```sql
-- objects with a value on both copies: decide for each which value is right before going on
SELECT d._id_object FROM _values d JOIN _values k ON k._id_object = d._id_object
WHERE d._id_structure = :dup AND k._id_structure = :keep;

UPDATE _values SET _id_structure = :keep
WHERE _id_structure = :dup
  AND _id_object NOT IN (SELECT _id_object FROM _values WHERE _id_structure = :keep);

DELETE FROM _structures WHERE _id = :dup;
```

For a field that is a nested class, an array or a dictionary, the values hang on the child structures and on the
element rows of each copy: move them child by child with the same statements before the copy is deleted.

## 4.4

### Npgsql 10

`redb.Postgres` and `redb.Postgres.Pro` bring Npgsql 10 (was 9), the version `Npgsql.EntityFrameworkCore.PostgreSQL`
10 requires; a process holds one Npgsql. redb itself is not affected, but your own code and logs on the same Npgsql are:

- `PostgresException.BatchCommand` is `null` by default, so the SQL of a failed command no longer travels in the
  exception into logs. Add `Include Failed Batched Command=true` to the connection string to get it back. redb does
  not change your connection string.
- With `Root Certificate` set in the connection string, the TLS chain is checked against that certificate only; the
  system certificate store is no longer consulted. A server whose chain ends in a CA of the system store, not in the
  given file, fails to connect: put the whole chain into the file, or drop `Root Certificate`.
- An untyped read of a `date` or `time` column (`GetValue`, `ExecuteScalar` to `object`) returns `DateOnly` /
  `TimeOnly`, not `DateTime` / `TimeSpan`. `ExecuteScalarAsync<DateTime>` and the row mapper of `IRedbContext` still
  hand back `DateTime` / `TimeSpan`, as on Npgsql 9.

The other drivers move too: Microsoft.Data.SqlClient 7.1.1 (a project that pins it below 7.0.3 gets a NU1605
downgrade error and has to raise the pin), Microsoft.Data.Sqlite 10.0.13 and native SQLite 3.53.3.

### Custom storage providers

Only code that builds a storage provider itself is affected. The interceptor parameter of the provider
constructors and of `CreateObjectStorageProvider` is now a `RedbSaveInterceptorPipeline` instead of the list of
interceptors. Interceptors that reach redb through DI, or subscribe through `IRedbSaveInterceptorRegistry`, need no
change.

### Empty values in sorting

PostgreSQL Pro now sorts objects without a value for the sort key the way LINQ, SQL Server and SQLite do: first under
`OrderBy`, last under `OrderByDescending`. PostgreSQL did the opposite. Sort keys over Props fields, expressions,
window orderings and grouped windows are affected; base columns, which are never empty, are not. A page that
relied on the old placement, for example to show filled values first in an ascending sort, has to filter out empty
values or sort by a key of its own.

### SQLite: index on text values

SQLite (schema version 12) adds `IX__values__structure_object_string`, from which SQLite Pro answers `Field == null`
over a text field. An existing file builds it on its first start with this version, and on a large file that takes a
while, the file busy meanwhile. Plan the first start of 4.4 on a large SQLite database for a quiet window.

### Value-leading indexes

Every value column gets an index led by the value, so a filter on a field reads one range of it:

- **PostgreSQL** (PVT module 0.7.16): `(_id_structure, <value>, _id_object) INCLUDE (_array_index)`, replacing
  `IX__values__{String,Long,DateTimeOffset,Numeric}_not_null` and `ix__values__array_null_structure`.
- **SQL Server** (PVT module 0.2.30): eight `IX__values__structure_<value>_object` replace eleven
  `IX__values__*_not_null` and `IX__values__*_filter` indexes.
- **SQLite** (schema step 11): the same shape, replacing the `_not_null` indexes, `ix__values__array_null_structure`
  and the four indexes SQLite Pro used to build at start-up.

An existing database builds them on the first start with this version, inside the module transaction, and **writes
to `_values` wait while they are built**: about a minute per index on 38 million rows on PostgreSQL, 84 s in all on
SQL Server, 135 s on SQLite. Plan the first start for a quiet window. The statements that bring the old indexes back
are in `v2-pvt/00_module_init.sql` of each provider; the aggregate form (planner off) read them and is 1.5-2x slower
on some filters without them. On SQL Server only a role allowed to alter `_values` rebuilds an index; a role without
schema rights gets `RedbSchemaOutdatedException`.
