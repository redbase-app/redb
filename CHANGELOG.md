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
