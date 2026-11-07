# redb.SQLite

SQLite provider for **RedBase (REDB)** — Entity Database for .NET.

[![NuGet](https://img.shields.io/nuget/v/redb.SQLite?label=NuGet&color=blue)](https://www.nuget.org/packages/redb.SQLite)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](../LICENSE)

## What's inside

- SQLite-optimized SQL generation
- Tree queries with recursive CTEs
- Schema initialization (`redbPostgre.sql`)
- Full LINQ-to-SQL translation
- Depends on `redb.Core` and `Sqlite`

## Installation

```bash
dotnet add package redb.SQLite
```

## Setup

```csharp
using redb.Core.Extensions;
using redb.SQLite.Extensions;

builder.Services.AddRedb(options => options
    .UseSqlite("Host=localhost;Port=5432;Database=mydb;Username=sqlite;Password=pass"));
```

## Page cache and file mapping

By default the provider leaves SQLite's own settings: a 2 MB page cache per connection and no memory-mapped
file (`SqliteDataSource.MmapSizeBytes` and `SqliteDataSource.CacheSizeBytes` are 0). On a database of a few
gigabytes that small cache is what most of a query's time goes into. The examples set both before the provider
is registered:

```csharp
using redb.SQLite.Data;

SqliteDataSource.MmapSizeBytes = 4L * 1024 * 1024 * 1024;   // PRAGMA mmap_size
SqliteDataSource.CacheSizeBytes = 64L * 1024 * 1024;        // PRAGMA cache_size, per connection
```

Measured on the examples database (2.5 GB, about 100 000 employees), one grouping over a nested array with a
window took 940-1060 ms with the defaults and 430-600 ms with these two settings.

The trade-off: a mapping takes address space, not memory of its own; the pages live in the operating system's
file cache and all connections share them. SQLite caps the mapping at its compile-time limit, and what lies
beyond the mapped part is read as before. The page cache is process memory, and every pooled connection has
its own, so 64 MB times the number of open connections. For a small database, or where memory is tight, keep
the defaults.

## Links

- Documentation (EN): [redbase.app](https://redbase.app)
- Documentation (RU): [redb.ru](https://redb.ru)
- API Reference: [redbase-app.github.io/redb](https://redbase-app.github.io/redb/)
- GitHub: [github.com/redbase-app/redb](https://github.com/redbase-app/redb)

## License

Apache License 2.0 — see [LICENSE](https://github.com/redbase-app/redb/blob/main/LICENSE).
