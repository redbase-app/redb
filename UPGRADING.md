# Upgrading redb

What to check and change when moving to a new version. Each section lists the changes that need action;
[CHANGELOG.md](CHANGELOG.md) has the full list of what changed.

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
