# Upgrading redb

What to check and change when moving to a new version. Each section lists the changes that need action;
[CHANGELOG.md](CHANGELOG.md) has the full list of what changed.

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
