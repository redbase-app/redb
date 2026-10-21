# Changelog

All notable changes to RedBase will be documented in this file.
This changelog covers the **NuGet-published packages** only:

| Package | Edition |
|---------|---------|
| `redb.Core` | Free |
| `redb.Postgres` | Free |
| `redb.MSSql` | Free |
| `redb.SQLite` | Free |
| `redb.Export` | Free |
| `redb.Core.Pro` | Pro |
| `redb.Postgres.Pro` | Pro |
| `redb.MSSql.Pro` | Pro |
| `redb.SQLite.Pro` | Pro |
| `redb.Licensing` | Pro |
| `redb.CLI` | Tool |
| `redb.Templates` | Tool |

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Pending changes live in `changelog/unreleased/` until the release; `pwsh scripts/changelog.ps1 -Preview` shows them.

## [4.4.0] — 2026-10-21

### Breaking

- **Custom storage providers: the interceptor parameter of the provider constructors and of
  `CreateObjectStorageProvider` is a `RedbSaveInterceptorPipeline`.** Only code that builds a storage provider itself
  is affected. Packages: `redb.Core`. Upgrade: [UPGRADING.md](UPGRADING.md#custom-storage-providers).
- **`redb.Postgres` brings Npgsql 10, and three of its changes reach code that uses Npgsql directly.** A failed
  batch command no longer carries its SQL into the exception, `Root Certificate` no longer falls back to the system
  store, and an untyped read of `date` or `time` returns `DateOnly` / `TimeOnly`. The redb API itself is unaffected. Packages: `redb.Postgres`, `redb.Postgres.Pro`. Upgrade: [UPGRADING.md](UPGRADING.md#npgsql-10).
- **PostgreSQL Pro: empty values sort where LINQ, SQL Server and SQLite put them**: first under `OrderBy`, last under
  `OrderByDescending`. PostgreSQL did the opposite. Packages: `redb.Postgres.Pro`. Upgrade: [UPGRADING.md](UPGRADING.md#empty-values-in-sorting).

### Added

- **`IRedbObjectCache.RemoveObjectsOfTypes(Func<Type, bool>)`.** redb calls it when a collectible load context unloads;
  a custom cache that does not override it is cleared whole, so it keeps compiling and stays correct. Packages: `redb.Core`.
- **`IRedbSaveInterceptorRegistry`: save interceptors can subscribe at run time.** Code that cannot reach the DI
  container of the redb service, such as a module in its own load context, calls `Add(interceptor)` and disposes the
  returned handle when it stops. `AddRedb` and `AddRedbPro` register the registry. Packages: `redb.Core`, `redb.Core.Pro`.

### Changed

- **`redb` CLI exits with code 1 when `export`, `import`, `init` or `flat` fails.** They printed the error and exited
  with 0, so a calling script could not see the failure. Packages: `redb.CLI`.
- **`redb` CLI on System.CommandLine 2.0.13.** Commands, options, aliases and defaults are unchanged; help and parse
  errors are in English whatever the language of the operating system. Packages: `redb.CLI`.
- **When a collectible load context unloads, redb clears the caches of the .NET libraries**, as `dotnet watch` does,
  so System.Text.Json and the others let the context go. The first serialization after an unload runs cold; a failing
  handler is logged as a warning. Packages: `redb.Core`.
- **PostgreSQL Pro: `AllAsync` looks for one counterexample instead of counting twice.** One statement that stops at the
  first object for which the predicate is not TRUE replaces two counts (100 000 employees: 190-240 ms instead of
  290-420). A predicate over the object's own fields or a computed value, and the other providers, keep the two counts. Packages: `redb.Core`, `redb.Core.Pro`, `redb.Postgres.Pro`.
- **Polymorphic deserialization resolves a scheme id in the redb service current for the calling flow**, not in the
  one initialized last. A resolver set with `SystemTextJsonRedbSerializer.SetTypeResolver` still takes precedence. Packages: `redb.Core`.
- **A scheme read on its first query is logged at Debug, not Warning.** A scheme the database does not hold still
  fails the query. Packages: `redb.Core`.
- **Pro: an ordering written after `Select` sorts in the database, and a projection gets `ThenBy`.** `Select(...).OrderBy(...).Take(10)`
  now runs as `OrderBy(...).Take(10).Select(...)` when the key is a projected Props member or a field of the object; a computed
  key and the Free providers still sort in memory. `ThenBy` and `ThenByDescending` are new on a projection and need an `OrderBy` before them. Packages: `redb.Core`, `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **Pro: a filter on a dictionary entry by its key reads one row per object.** `Where(x => x.Dict.ContainsKey("k"))` and
  `Where(x => x.Dict["k"] == value)` no longer group every row of the dictionary in the scheme; a page of 100 over
  100 000 employees takes 90-100 ms instead of 270-300 on PostgreSQL and SQLite. Packages: `redb.Core.Pro`, `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **`redb flat` hands out keys and deletes in cascade, as redb does.** Keys default to one shared sequence, `flat_identity`,
  started above every copied key, and deleting a header row removes its Props, nested and collection rows. A database
  exported earlier keeps its old schema: export it again, and let new rows take their keys from the sequence. Packages: `redb.Export`, `redb.CLI`.
- **`redb flat` indexes every foreign key that no key or unique constraint already covers.** The indexes are
  created after the data is copied and are part of the `--ddl-only` script, whose closing line now counts them. Packages: `redb.Export`, `redb.CLI`.
- **SQL Server Pro: a filter with `Field == null` no longer checks every object of the scheme.** The objects without a value
  are found by one anti join over the index of the field, which stops when the page is full (a page of
  `Where(x => x.Field == null).Take(5)` over 100 000 employees with no such object: 300 ms -> 60 ms). Packages: `redb.MSSql.Pro`.
- **Database drivers: Npgsql 10.0.3, Microsoft.Data.SqlClient 7.1.1, Microsoft.Data.Sqlite 10.0.13, native SQLite
  3.53.3.** The security baseline pins MailKit and MimeKit 4.18.1, MessagePack 3.1.12 and
  System.Security.Cryptography.Xml / .Pkcs 10.0.13. A project that pins Microsoft.Data.SqlClient below 7.0.3 has to raise it. Packages: `redb.Postgres`, `redb.MSSql`, `redb.SQLite`. Upgrade: [UPGRADING.md](UPGRADING.md#npgsql-10).
- **PostgreSQL: JIT is off for redb connections.** The provider adds `-c jit=off` to the connection string Options
  unless they name `jit`: a page that takes 2.5 ms took 82 ms with JIT. Packages: `redb.Postgres`.
- **Value-leading indexes on PostgreSQL (PVT module 0.7.16), SQL Server (0.2.30) and SQLite (schema step 11)**: a
  filter on a field reads one range of its index. An existing database builds them on the first start with this
  version, and writes to `_values` wait meanwhile. Packages: `redb.Postgres`, `redb.MSSql`, `redb.SQLite`. Upgrade: [UPGRADING.md](UPGRADING.md#value-leading-indexes).
- **PostgreSQL Pro and SQLite Pro: an open range that is narrow by the data starts the streams** (`Weight > 7500` over
  689 thousand points: 0.8-1.2 s to 12-35 ms), aggregates, groupings and windows over it included. Packages: `redb.Postgres.Pro`, `redb.SQLite.Pro`.
- **PostgreSQL Pro: the streams form finds a narrow filter through the index of values**: one order number among
  689 thousand points, 2.4 s to 31 ms. A list without `Take` takes the streams form too. Packages: `redb.Postgres.Pro`.
- **PostgreSQL Pro and SQLite Pro: a page sorted by one Props field walks that field's index** (with `Take`, a number
  or a date as the key, no narrow condition): pages over 689 thousand points, seconds to 10-40 ms. Not under `WhereRedb`. Packages: `redb.Postgres.Pro`, `redb.SQLite.Pro`.
- **PostgreSQL Pro: queries narrowed by a Props value read the index of values.** Counts, existence checks, null checks,
  aggregates, groupings, windows and pages with `Take` take their objects from the index of the narrow condition instead
  of every object of the scheme, and an equality or `In` on a string field reads its index (100 000 employees: two to
  five times faster). A filter on the object's own fields (`WhereRedb`, `OrderByRedb`) keeps its form. Packages: `redb.Postgres.Pro`.
- **PostgreSQL Pro: a text field under `WhereRedb` is read for the objects of the window only.** A grouping, a null check
  or a substring search by a text field on objects narrowed by their own columns (a date window, a parent) no longer reads
  every value of the field in the scheme, so its time follows the window rather than the archive. Packages: `redb.Postgres.Pro`.
- **Pro: a page sorted by a Props field with `ThenBy` reads only the run of equal first keys it needs**, from the index
  of values, instead of the whole scheme when the first key repeats: `OrderByDescending(x => x.Salary).ThenBy(x => x.LastName).Take(100)`
  over 100 000 employees, about 290 ms to 60 ms on PostgreSQL, SQL Server and SQLite. The answer does not change. Packages: `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **SQLite: a new index `IX__values__structure_object_string` (schema version 12), and SQLite Pro answers `Field == null`
  over a text field from it** (850 ms to 70 ms over 100 000 objects). An existing file builds the index on its first
  start with this version, which takes a while on a large file. Packages: `redb.SQLite`, `redb.SQLite.Pro`. Upgrade: [UPGRADING.md](UPGRADING.md#sqlite-index-on-text-values).
- **SQLite Pro and SQL Server Pro: an OR of narrow branches stays on the aggregate form with the row prefilter**, as
  on PostgreSQL: three branches, 1.3-1.7 s to 27-95 ms. Packages: `redb.SQLite.Pro`, `redb.MSSql.Pro`.
- **SQLite Pro: counting by a null check reads the index of values.** `Count(x => x.Field == null)` and
  `Count(x => x.Field != null)` on their own no longer read the value of every object from the table
  (100 000 employees, a text field: 0.9-1 s -> 35-50 ms). Packages: `redb.SQLite.Pro`.

### Fixed

- **A module in a collectible `AssemblyLoadContext` unloads after it used redb.** Process-wide caches held the
  module's Props types for the life of the process; they hold them weakly now and drop the module's objects on unload.
  Nothing is needed from the host beyond `Unload()`. Packages: `redb.Core`, `redb.Core.Pro`.
- **Pro: the props cache no longer serves a referenced object that changed or went to the trash.** Before serving a cached
  object, the objects it references are checked against the database, so changes from another cluster node are seen too.
  A query caches referenced objects by their own ids; a graph saved with an id-only reference is not cached until loaded. Packages: `redb.Core`, `redb.Core.Pro`, `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **Pro: the PVT prefilter covers an OR of three or more conditions**, as it covered two: an order number searched in
  three fields over a month, 1.9 s to 0.1 s on PostgreSQL. Packages: `redb.Core.Pro`.
- **Pro: `!x.Dict.ContainsKey("k")` counts the objects that have no dictionary at all.** With the PVT prefilter off, or on a
  query that keeps the aggregate form, such objects were left out, so the same filter gave different sets depending on the form. Packages: `redb.Core.Pro`.
- **SQLite Pro: a filter on a field of a class inside a dictionary no longer scans the scheme for every match**
  (61 s to 0.1 s on the examples). Packages: `redb.SQLite.Pro`.
- **The module templates scale on Tsak.** `deploy/docker-compose.tsak.yml` of `redb-app`, `redb-bff`, `redb-chat` and
  `redb-worker` no longer sets `Tsak__Cluster__NodeId`, so each replica of `docker compose up --scale` names itself after
  its host; a fixed id had the second replica refused. Packages: `redb.Templates`.

## Earlier versions

Earlier releases are archived by line in [changelog/](changelog/): [4.3.x](changelog/CHANGELOG-4.3.x.md), [4.2.x](changelog/CHANGELOG-4.2.x.md), [4.1.x](changelog/CHANGELOG-4.1.x.md), [4.0.x](changelog/CHANGELOG-4.0.x.md), [3.x](changelog/CHANGELOG-3.x.md), [2.x](changelog/CHANGELOG-2.x.md), [1.x](changelog/CHANGELOG-1.x.md).
