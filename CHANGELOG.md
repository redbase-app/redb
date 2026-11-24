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

## [4.7.5] — 2026-11-24

### Breaking

- **`TimeSpan` and `TimeOnly` are stored as their ticks in a number column, so they compare, sort, group and take Min/Max
  correctly on every provider.** As text, values over a day and negative values compared wrong. Every node and image of a
  database must run 4.7.5, an older one writes text again; there is no way back except a backup. Convert the stored
  values before the first start with the statement UPGRADING.md gives; values left as text read as empty. Packages: `redb.Core`, `redb.Core.Pro`, `redb.Postgres`, `redb.MSSql`, `redb.SQLite`, `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`, `redb.Export`. Upgrade: [UPGRADING.md](UPGRADING.md#timespan-and-timeonly-are-stored-as-ticks).

### Added

- **`RedbSchemaNewerException`.** Thrown at start-up when the database carries a newer redb module that does not serve this
  package; it names the deployed and the own version and, when the module states it, the package version to upgrade to. Packages: `redb.Core`.
- **Pro: `Where(p => p.Lines.Any(l => l.Kind == "a" && l.Active))` over an array or a list of classes.** All conditions
  read one element, on every provider. The free providers now refuse the form with a clear `NotSupportedException`;
  before, SQL Server Free returned no object and PostgreSQL and SQLite Free failed in the database. Packages: `redb.Core`, `redb.Core.Pro`, `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **Tools can judge the schema version themselves and stop with the same exceptions as the start-up.**
  `redb.SQLite.RedbService.SchemaVersion` gives the file schema version this build writes into `PRAGMA user_version`, and
  `RedbSchemaOutdatedException` and `RedbSchemaNewerException` gain constructors that take the reason and the remedy text.
  They carry the fix of `redb import --parallel` into a database of an older schema. Packages: `redb.Core`, `redb.SQLite`.

### Changed

- **A package no longer puts its database module over a newer one.** From 4.7.5 on, a node that finds a newer module
  starts with a warning when that module serves it, and stops with `RedbSchemaNewerException` without changing anything
  when it does not; the upgrade script run by hand refuses a newer module the same way. Packages published before 4.7.5
  do not know this rule. The module goes to `0.7.23.1` (PostgreSQL) and `0.2.40.2` (SQL Server), a SQLite file to schema 13. Packages: `redb.Core`, `redb.Postgres`, `redb.Postgres.Pro`, `redb.MSSql`, `redb.MSSql.Pro`, `redb.SQLite`, `redb.SQLite.Pro`. Upgrade: [UPGRADING.md](UPGRADING.md#a-newer-database-module-is-left-in-place).
- **The start-up log no longer mentions buying anything.** The Pro welcome banner says that all Pro features are on,
  free of charge and with no key, without a link to go commercial. Packages: `redb.Core.Pro`.
- **`redb export` refuses a database of a newer schema than its own, and the `redb.Export` package depends on
  `redb.Postgres`, `redb.MSSql` and `redb.SQLite`.** An export from a newer database left out what the tool did not
  know of it without a word; it now stops with `RedbSchemaNewerException`: export with the CLI of the version the
  database runs. An older source stops with `RedbSchemaOutdatedException`. An import into a newer database goes on when
  the database states that it serves this version, as a service start does. The schema numbers come from the provider
  packages, hence the new dependencies. Packages: `redb.Export`, `redb.CLI`.
- **SQL Server: the count a delete returns no longer includes descendants.** `DeleteAsync` over ids or objects returns
  the requested objects it deleted, as on PostgreSQL and SQLite; it used to add every descendant removed with them. Packages: `redb.MSSql`, `redb.MSSql.Pro`.

### Fixed

- **A comparison of a field with a computed value failed with "variable 'p' ... is not defined".** A value of your own built
  from parts (`p.Title == tag + "-x"`, `p.Age == limit + 1`, `x ?? "d"`, a ternary), a field against an expression over
  fields (`p.A == p.B + 1`, `p.Full == p.First + " " + p.Last`) and a field against a field (`p.A > p.B`) now filter as LINQ
  does, on every provider. Packages: `redb.Core`.
- **SQLite Pro: a decimal read back from its REAL column lost digits or gained binary noise.** A value with more than
  15 significant digits came back cut (12345678901234.567 as 12345678901234.6), and on some runtimes every decimal came back
  with the exact binary value of the stored double (123.45 as 123.45000000000000284217094304). A decimal now reads back as
  the shortest number that the stored double stands for, on every runtime, also in Min, Max and Sum. Packages: `redb.Core`, `redb.SQLite`, `redb.SQLite.Pro`.
- **`DeleteSubtreeAsync` counts only the objects it deleted.** It answered the size of the subtree it had collected, so
  an object another session deleted in the meantime was counted too. Packages: `redb.Core`, `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.
- **A filter on a field named `Count` or `Length` reads the field.** Such a member was taken for the size of a collection
  or a string, and the query failed; the size of a real list or string is read as before. Packages: `redb.Core`.
- **Free: a list read builds the JSON of each object once.** The statement that finds the objects read each one with
  `get_object_json`, and the lazy loader built it again; it now reads their rows only, so a list page (and a tree read on
  SQLite) is faster, and Props come from the Props cache where it holds them. Packages: `redb.Core`, `redb.Postgres`, `redb.MSSql`, `redb.SQLite`. Upgrade: [UPGRADING.md](UPGRADING.md#custom-dialects-query_wrappvtwithobjectrows).
- **Free: `Query<T>().DeleteAsync()` works.** A query delete threw `NotSupportedException` on every Free provider; it now
  deletes the objects the filter selects, as a list read finds them, and returns how many it deleted, as on Pro. Packages: `redb.Core`, `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.
- **SQLite: a delete over a batch counts a parent and its child both.** The child, removed with its parent by the cascade
  before the statement reached it, was not counted. The same holds for a query delete and a tree delete by ids. Packages: `redb.Core`, `redb.SQLite`, `redb.SQLite.Pro`.
- **Free: `ToAggregateSqlStringAsync` shows the aggregate statement that runs.** It threw `NotSupportedException` on every
  Free provider; it now returns the statement the database module compiles for the same aggregates and `Where`, ready to
  run as it is. Packages: `redb.Core`, `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.
- **Free: `WhereHasAncestor<T>` / `WhereHasDescendant<T>` over a type without a scheme name the type.** They failed with
  "Legacy ... SQL path is removed"; they now throw `InvalidOperationException` when the query is built, with the type in
  the message, as `Query<T>()` does for such a type. The scheme is looked up as `Query<T>()` looks it up: a scheme this
  process has not synced is read from the database. Packages: `redb.Core`, `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.
- **`SumAsync`, `AverageAsync`, `MinAsync`, `MaxAsync` and `GetStatisticsAsync` over `p.Items[0].Rank` or
  `p.ByKey["k"].Rank` returned the object's own `Rank`, and a window `PartitionBy`/`OrderBy` over such a path used it too.**
  They now read the element or the entry, or refuse the path with `NotSupportedException`, and `p.Score.Value` of a
  nullable field works. Recheck the results of such queries. Packages: `redb.Core`.
- **SQL Server: concurrent saves of different objects deadlocked each other.** Up to 250 rows per table a save now inserts
  with statements and no longer deadlocks; a larger one keeps the bulk load and its retry. On every provider a save locks
  only the objects that existed before it, and a delete, soft-delete mark or purge batch outside a transaction is retried
  on a deadlock like a save; inside your transaction the deadlock reaches you (see "Deadlocks and retries" in the README). Packages: `redb.Core`, `redb.MSSql`.
- **A filter on `.Value` or `.HasValue` of a dictionary entry (`p.Scores["a"].Value == 1`) failed in the database with an
  empty field name.** It now filters by the entry. A sort through a dictionary entry, an element by index or a method
  call (`OrderBy(p => p.Items[0].Name)`) is refused with the same message as such a filter; it used to sort by the field
  of that name of the object itself. Packages: `redb.Core`.
- **`Where(p => list.Contains(p.F))` and its negation read every value of the list as the field stores it.** On Pro a
  null in the list was sent as 0, false or an empty text and could match objects holding that value, and a list of floats
  was cut to whole numbers; lists of chars, Guids, dates, times and enums failed. A null in the list is now ignored, and
  `!list.Contains(p.F)` over an empty list returns the objects that have a value, not those without one. Packages: `redb.Core`, `redb.Postgres`, `redb.MSSql`, `redb.SQLite`, `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **`Where(p => p.Arr.Any())` over an array or a list of simple values gives the right answer on every provider.** On
  PostgreSQL it failed with `function length(text[]) does not exist`, on SQLite an empty or missing array counted as
  non-empty, and on SQL Server `!p.Arr.Any()` returned no object. Packages: `redb.Core`.
- **`Where(p => p.Roles.Any(r => ...))` over list items answers only what it reads.** An operator other than `==`
  (`r.Value != x`, `r.Id > n`) was read as `==` and gave a wrong answer without a word; it is now refused with
  `NotSupportedException`. `!p.Roles.Any(r => r.Value == x)` also returns the objects with an empty or no list on
  PostgreSQL and in SQLite Pro, as it did on SQL Server and SQLite Free. Packages: `redb.Core`, `redb.Postgres`, `redb.Postgres.Pro`, `redb.SQLite.Pro`.
- **A list no longer hands out an object with null `Props` when it is deleted during the query.** The objects and their
  values are read by two statements; an object deleted in between came back without `Props`. It now leaves the result,
  so a page may come back shorter in that race. Packages: `redb.Core.Pro`, `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.
- **Pro: an object with a dictionary of a nullable value type (`Dictionary<string, int?>`) could not be loaded.** Every
  read of it failed with "cannot be converted to Nullable`1" on every provider. Such dictionaries now read back as saved,
  and a null in an array or list of a nullable value type (`int?[]`, `List<long?>`) keeps its position instead of being
  left out. Packages: `redb.Core.Pro`.
- **PostgreSQL: `redb import` refuses a file with references to rows it does not hold, as SQL Server does.** Such rows were
  loaded silently. The import now names each broken key, the number of rows and an example; a serial import changes
  nothing, a `--parallel` one has already written its rows. A file exported with `--schemes` whose objects point into a
  scheme left out is refused this way: export the schemes it references too. Packages: `redb.Export`, `redb.CLI`.
- **SQLite: `redb import` refuses a file with references to rows it does not hold, as PostgreSQL and SQL Server do.** Such
  rows were loaded silently. The import now names each broken key, the number of rows and an example, and changes
  nothing. A file exported with `--schemes` whose objects point into a scheme left out is refused this way: export the
  schemes it references too. Packages: `redb.Export`, `redb.CLI`.
- **`redb import --parallel` into a database of an older schema emptied the target and stopped; the import now refuses
  such a target before it touches anything.** Every import checks the target's schema first and stops with
  `RedbSchemaOutdatedException` naming the upgrade to run, or names `redb init` when the schema is missing. The check adds
  exception constructors and `redb.SQLite.RedbService.SchemaVersion`, and `redb init -p sqlite` now stamps the file. Packages: `redb.Export`, `redb.CLI`.
- **`redb import --parallel` attempts every finishing step and reports their failures together.** When one step after
  the load failed (the id sequence of a database that had lost it), the steps after it never ran: the secondary indexes
  of the large tables stayed out and, on SQL Server, every constraint and trigger stayed off. The sequence, the index
  rebuild and the constraints now each run whatever the others do, and the error names every step that failed. Packages: `redb.Export`, `redb.CLI`.
- **An import builds the scheme metadata cache of every imported scheme.** The first query of an application rebuilt it,
  which is slow on a large database and needs write rights. Packages: `redb.Export`, `redb.CLI`.
- **An import of a file written before TimeSpan and TimeOnly were stored as ticks no longer puts their text type back.**
  The target keeps its own built-in `_types` rows whatever the file says, and the import's last lines name the imported
  values stored as text, which the upgrade notes convert. Packages: `redb.Export`, `redb.CLI`.
- **A delete counts only what it deleted.** On SQL Server `DeleteAsync` answered true, and the batch overload counted,
  objects another session had already deleted; on SQL Server and PostgreSQL a soft delete moved an object already in the
  trash into a second trash and counted it. Packages: `redb.MSSql`, `redb.MSSql.Pro`, `redb.Postgres`, `redb.Postgres.Pro`.
- **SQL Server (Free): `WhereLevel(n)` filters.** An exact level of a query over the whole scheme was dropped from the
  filter, and every node came back; `WhereLevel(l => l > n)` worked. The module goes to `0.2.40.2` at the next start. Packages: `redb.MSSql`.
- **SQL Server (Free): a list read keeps the order of the query.** The database module dropped the ORDER BY of a query
  without paging, and the order of a page or of `OrderByDescending(key).DistinctBy(key)` was left to the plan. The module
  goes to `0.2.40.2`; it is applied on the next start, as every module update. Packages: `redb.MSSql`.
- **SQL Server (Free): a tree query returns its page in the order asked for.** The page was joined to the objects
  without carrying the order, and kept it only while the plan happened to be a nested loop. Packages: `redb.MSSql`.
- **PostgreSQL (Free): a list read keeps the order of the query in every plan.** The order was numbered above the inner
  query and held only while the plan read its rows in order. The module goes to `0.7.23.1`; it is applied on the next
  start, as every module update. Packages: `redb.Postgres`.
- **An aggregate over a field of an element of an array of classes by index (`Agg.Sum(x.Props.Items[0].Rank)`) returned 0
  or null.** It is now refused with `NotSupportedException`; redb Pro reads it. Recheck the results of such queries. Packages: `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.
- **PostgreSQL: the creation script (`GetSchemaScript`) run again on a deployed database stops at its first statement and
  changes nothing**, as the SQL Server script does since 4.7.3. Run it with `psql -v ON_ERROR_STOP=1`. On SQL Server a
  refused creation script no longer leaves a pooled session that runs no further command. Packages: `redb.Postgres`, `redb.Postgres.Pro`, `redb.MSSql`, `redb.MSSql.Pro`.
- **Pro: a query delete counts a parent and its child both, and a filter on base fields only works.** On SQLite the child
  the cascade removed with its parent was not counted, and a delete filtered only on base fields (`WhereRedb`) failed
  with "no such column: o._id". On SQL Server a query delete that matched an object with children failed on the
  `_id_parent` foreign key; it now deletes the subtree, as `DeleteAsync` does. Packages: `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **Pro: `TreeQuery<T>()` without a root reads every node of the scheme.** It started at the roots of `T`'s scheme and
  missed every node in a tree whose root is of another scheme; `WhereLevel` now counts from the real root of the tree, as
  documented and as on Free, and `WhereHasAncestor` finds those nodes too. Packages: `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **`WhereHasAncestor` and `WhereDescendantsOf` reach nodes below nodes of other schemes.** In a tree of several schemes
  Pro walked down only through nodes of one scheme (`WhereHasAncestor` on PostgreSQL and SQLite, `WhereDescendantsOf` on
  all three), so an ancestor or a descendant under a node of another scheme was missed and the list and its analytics
  came back short or empty. The tree below the start is now walked whole, through nodes of every scheme, as Free does. Packages: `redb.Postgres.Pro`, `redb.SQLite.Pro`, `redb.MSSql.Pro`.
- **Pro: `ToAggregateSqlStringAsync` over a dictionary shows the aggregate that runs.** An aggregate of
  `Dictionary.Values` or of one key (`x.Props.BonusByYear[2023]`) previewed as `SELECT 1 AS "_empty"`; the preview now
  reads the selector as `AggregateAsync` does. Packages: `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **Pro: `WhereHasAncestor<T>` / `WhereHasDescendant<T>` over a type without a scheme throw.** The filter was dropped
  and the query returned every node. It now throws `InvalidOperationException` naming the type when the query is built;
  make the type a `[RedbScheme]` class. A scheme this process has not synced is read from the database, as `Query<T>()`
  reads it. Packages: `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **Pro: `Agg.Sum(x.Props.Score.Value)` in `AggregateAsync` and `GetStatisticsAsync(p => p.Score.Value)` returned 0 on
  PostgreSQL and SQL Server Pro, and an aggregate over a dictionary entry or an element of an array of classes read none,
  or the largest of all entries.** They now read the field and the entry by its key. Recheck the results of such queries. Packages: `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **Pro: a filter `p.Scores["n"] != null` (or `== null`, `.HasValue`) on a dictionary entry holding null answered whether
  the dictionary had any entry.** On SQLite Pro, and on PostgreSQL and SQL Server Pro with the query planner on, an entry
  stored with a null value counted as set. It now tests the entry's own value. Packages: `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **PostgreSQL Pro: `DistinctBy` on a field some objects lack put the object without a value last when ascending.** It now
  comes first ascending and last descending, as in every other ordering and on the other providers. Packages: `redb.Postgres.Pro`.

## [4.7.4] — 2026-11-20

### Changed

- **redb.MSSql 4.7.4 and redb.CLI 4.7.4; the other redb packages stay 4.7.2.** redb.CLI carries the module of
  redb.MSSql 4.7.4, so `redb init` and `redb schema` install the fixed module; update both. The Tsak and Identity images
  and archives keep redb.MSSql 4.7.2 in `Libs/shared` until 4.8.0: on SQL Server Free put `redb.MSSql.dll` of the 4.7.4
  package there, or every node of the database puts the old module back at start. Affected: SQL Server with the Free provider only; the Pro provider is not affected.

### Fixed

- **SQL Server (Free): a filter on a base field with a long condition was dropped when the query also filtered on a
  props field or grouped, aggregated or used a window.** For example, `WhereRedb(o => ids.Contains(o.ParentId.Value))`
  with about 400 ids or more (the condition over 4,000 characters) returned objects outside the id list: every object
  the props filter let through. The module goes to `0.2.40.1` at the next start; run every node of a database on this
  release, since an older 4.7 package puts its own module back when it starts. Packages: `redb.MSSql`.

## [4.7.3] — 2026-11-19

### Changed

- **redb.CLI 4.7.3; the redb packages stay 4.7.2.** Only the redb.CLI tool and its standalone executables are released;
  they carry the SQL Server creation script with the guard below. redb.Core, the providers and redb.Export on nuget.org
  stay at 4.7.2 and are not republished.

### Fixed

- **`redb init` no longer damages a SQL Server database that already holds the redb schema.** Run a second time, it
  dropped every foreign key and the id sequence before it failed, and a later import or application failed on the
  missing sequence. On PostgreSQL and SQL Server `redb init` now refuses a database that holds the schema before it runs
  anything, and the SQL Server script printed by `redb schema` stops on such a database before its first statement. A SQL
  Server database where `redb init` ran twice may have been hit: the upgrade notes say how to check it and how to repair it. Packages: `redb.CLI`. Upgrade: [UPGRADING.md](UPGRADING.md#sql-server-a-database-hit-by-a-second-redb-init).

## [4.7.2] — 2026-11-13

### Fixed

- **SQLite: saving an object again after its transaction rolled back no longer overwrites another object.** Since 4.0.0,
  ids a save took inside a transaction that rolled back went to the next save too: saving the first object again silently
  replaced the second one's data, and later saves and deletes could fail with `UNIQUE constraint failed` on `_objects._id`
  or `_values._id`. SQLite now never hands out an id twice. On every provider the ids a save assigned inside a rolled-back
  transaction go back to 0 on the objects, and a scheme created there is created again. Packages: `redb.Core`, `redb.Core.Pro`, `redb.SQLite`, `redb.SQLite.Pro`.

## [4.7.1] — 2026-11-11

### Fixed

- **Constant and captured bool conditions in `Where`, `WhereRedb` and `AllAsync`.** A bool variable of the caller in a
  condition (`Where(x => includeAll || x.Age >= 32)`, `Where(x => !skip)`) failed with "Expression must be a property
  access"; it is now read as the value it holds. On the free providers `Where(x => true)`, `Where(x => false)` and
  `AllAsync` with such a predicate failed in the SQL module; they now keep every object or none. Packages: `redb.Core`.
- **A filter by an array or list element taken by index, or through a method call in the path, read another field.**
  `Where(x => x.Items[0].Name == "a")` and `Where(x => x.Items.First().Name == "a")` compared the field `Name` of the
  object itself and returned its rows, or failed later with "field not found". Such a filter now fails at once with
  `NotSupportedException` naming the path; use `Contains(...)` for an array of simple values, `Any(...)` for an array
  of list items, or keep the data in a dictionary
  and read the entry by its key. Packages: `redb.Core`.
- **Pro: a constant inside OR returned wrong rows.** `Where(x => x.Age >= 32 || true)` returned only the objects
  matching the comparison and `Where(x => x.Age >= 32 || false)` returned nothing; they now return every object and the
  matching ones, as LINQ does. The same holds for the `redbQuery` forms `x not in <empty list> OR ...` and
  `x in <empty list> OR ...`. On PostgreSQL Pro `AllAsync(x => true)` answered false for a set with objects; it answers
  true. A constant alone or inside AND answered right and is unchanged. Packages: `redb.Core`.
- **SQL Server: the database module never reached the version the package asks for** (4.6.1 to 4.7.0). Every start
  applied the module again, and a start with `AutoApplyDatabaseUpgrades = false` or under a role without schema rights
  stopped with `RedbSchemaOutdatedException`, also right after the upgrade script was applied. Update the package: the
  module goes to `0.2.40` once and later starts leave the schema alone. Packages: `redb.MSSql`, `redb.MSSql.Pro`.

## [4.7.0] — 2026-11-06

### Added

- **`dotnet new redb-route-xmljson`: XML to JSON and back without a class.** A Tsak module whose XML routes turn an
  order into JSON by an XSD (arrays, numbers, booleans) and an HTTP API that turns it back into namespaced XML and
  checks it; the same round trip in C# with `MarshalXmlJson` / `UnmarshalXmlJson`. Packages: `redb.Templates`.

### Changed

- **`redb-route-beans` shows the bean step and a bean call in a condition.** The order route prices a line with
  `<bean ref="#pricing" method="Gross(...)"/>` and marks bulk lines with `<filter expr="bean:pricing.IsBulk(header.qty)">`;
  `PriceCalculator` has the synchronous `IsBulk(int)` the condition calls. Packages: `redb.Templates`.

### Deprecated

- **.NET 8 and .NET 9 targets end with 4.7.** Microsoft's support for both ends on 10 November 2026, so 4.7.0 is the
  last release whose packages target them; from 4.8.0 the packages target .NET 10 and later. Fixes on the 4.7.x line
  are still built for .NET 8, 9 and 10 while those lines can be built without known vulnerabilities. Move your projects
  to .NET 10 before taking 4.8.

## Earlier versions

Earlier releases are archived by line in [changelog/](changelog/): [4.6.x](changelog/CHANGELOG-4.6.x.md), [4.5.x](changelog/CHANGELOG-4.5.x.md), [4.4.x](changelog/CHANGELOG-4.4.x.md), [4.3.x](changelog/CHANGELOG-4.3.x.md), [4.2.x](changelog/CHANGELOG-4.2.x.md), [4.1.x](changelog/CHANGELOG-4.1.x.md), [4.0.x](changelog/CHANGELOG-4.0.x.md), [3.x](changelog/CHANGELOG-3.x.md), [2.x](changelog/CHANGELOG-2.x.md), [1.x](changelog/CHANGELOG-1.x.md).
