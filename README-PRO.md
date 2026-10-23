# RedBase Pro

**Data Platform for .NET** — the Pro engine of RedBase: SQL built in C#, a query planner, parallel materialization.

[![NuGet](https://img.shields.io/nuget/v/redb.Core.Pro?label=NuGet&color=blue)](https://www.nuget.org/packages/redb.Core.Pro)
[![.NET](https://img.shields.io/badge/.NET-8%20%7C%209%20%7C%2010-purple)](https://dotnet.microsoft.com)
[![Docs](https://img.shields.io/badge/docs-redbase.app-orange)](https://redbase.app)

RedBase Pro replaces the query engine of the open-source [RedBase](https://github.com/redbase-app/redb) core. The API is
the same: a project moves from Free to Pro by changing the packages and `AddRedb` to `AddRedbPro`.

**Contents:** [Free and Pro](#free-and-pro) · [Packages](#packages) · [Quick Start](#quick-start) ·
[New in 4.4](#new-in-44) · [Supported Databases](#supported-databases) · [Documentation](#documentation) ·
[License](#license)

---

## Free and Pro

Both editions take the same LINQ API: filters, nested fields, arrays and dictionaries, arithmetic, `Sql.Function<T>()`,
aggregation, `GroupBy`, window functions, trees, lists, users and permissions. They differ in how a query runs.

| | Free | Pro |
|---|---|---|
| Query engine | SQL functions of the module installed in the database (PL/pgSQL, T-SQL, a native loadable extension on SQLite) | SQL built in C# by the provider and sent as one statement |
| Materialization of Props | sequential | parallel |
| Query planner (`EnablePvtPrefilter`) | no | yes, on by default: the streams form reads a page, a count or an aggregate without pivoting every object of the scheme |
| `PropsSaveStrategy.ChangeTracking` | no (`DeleteInsert` only) | yes: a save writes only the values that changed |
| Data migrations of Props classes | no | `IRedbMigration<T>`, `MigrateAsync`, `AddRedbMigrations()` |
| `Distinct()` | by object | one object per Props hash |
| SQLite | needs the native extension | pure C#: Blazor WebAssembly and mobile (MAUI) |

```
Free:  LINQ → SQL module in the database → rows as JSON → Props
Pro:   LINQ → SQL built in C# → rows → Props, materialized in parallel
```

---

## Packages

| Package | NuGet | Description |
|---------|-------|-------------|
| `redb.Core.Pro` | [![NuGet](https://img.shields.io/nuget/v/redb.Core.Pro?label=)](https://www.nuget.org/packages/redb.Core.Pro) | Pro engine, change tracking, data migrations |
| `redb.Postgres.Pro` | [![NuGet](https://img.shields.io/nuget/v/redb.Postgres.Pro?label=)](https://www.nuget.org/packages/redb.Postgres.Pro) | PostgreSQL Pro provider |
| `redb.MSSql.Pro` | [![NuGet](https://img.shields.io/nuget/v/redb.MSSql.Pro?label=)](https://www.nuget.org/packages/redb.MSSql.Pro) | SQL Server Pro provider |
| `redb.SQLite.Pro` | [![NuGet](https://img.shields.io/nuget/v/redb.SQLite.Pro?label=)](https://www.nuget.org/packages/redb.SQLite.Pro) | SQLite Pro provider — pure C#, no native extension: Blazor WebAssembly & mobile |

```bash
dotnet add package redb.Postgres.Pro   # or redb.MSSql.Pro, redb.SQLite.Pro
```

Each provider package brings `redb.Core.Pro` as a transitive dependency.

## Quick Start

```csharp
using redb.Core;
using redb.Core.Models.Configuration;
using redb.Core.Pro.Extensions;      // AddRedbPro
using redb.Postgres.Pro.Extensions;  // UsePostgres; or redb.MSSql.Pro.Extensions, redb.SQLite.Pro.Extensions

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRedbPro(options => options
    .UsePostgres("Host=localhost;Database=mydb;Username=postgres;Password=pass")
    // .UseMsSql("Server=localhost;Database=mydb;User Id=sa;Password=pass;TrustServerCertificate=true")
    // .UseSqlite("Data Source=app.db")
    .Configure(c =>
    {
        c.PropsSaveStrategy = PropsSaveStrategy.ChangeTracking;
        c.EnablePropsCache = true;
    }));

var app = builder.Build();
var redb = app.Services.GetRequiredService<IRedbService>();
await redb.InitializeAsync();

var results = await redb.Query<EmployeeProps>()
    .Where(e => e.Salary > 75000m)
    .Take(100)
    .ToListAsync();

// Window functions — from E132_WindowRowNumber.cs
var ranked = await redb.Query<EmployeeProps>()
    .WithWindow(w => w
        .PartitionBy(x => x.Department)
        .OrderByDesc(x => x.Salary))
    .SelectAsync(x => new
    {
        Name = x.Props.FirstName,
        Department = x.Props.Department,
        Salary = x.Props.Salary,
        Rank = Win.RowNumber()
    });
```

## New in 4.4

Pro query engine:

- A window with `Take`, `Skip` and `OrderBy` returns the page; the window is computed over the whole filtered set on the
  fields it needs, and the other selected fields are read only for the rows returned. The same holds over a tree
  (`TreeQuery(...).Take(...).WithWindow(...)`), and `Win.Ntile(n)` over a tree uses `n`.
- `AnyAsync` over a tree stops at the first matching node.
- `DistinctBy` over a tree is faster on SQL Server and SQLite; `DistinctBy` keeps the ordering of the query, and
  `CountAsync` after `DistinctBy` counts the keys.

Both editions (core):

- Schema work at start-up and scheme synchronization run under a schema lock, so nodes sharing one database start
  together (`SchemaLockTimeout`).
- `IRedbService.SchemaProblems` and `CheckSchemaAsync()` report problems of the schema that need a person, for a health
  check.
- The tree services read a subtree or a chain of ancestors in one statement; several tree filters in one query all
  apply.

The full list is in [CHANGELOG.md](CHANGELOG.md); what to check when upgrading is in [UPGRADING.md](UPGRADING.md).

## Free — no license key

All Pro features work out of the box — **no license key required** (starting from version 3.3.0). Install the Pro
packages and use them, in development and in production alike. Pro is proprietary (closed-source) software,
distributed free of charge.

## Supported Databases

| Database | Version | Package |
|----------|---------|---------|
| PostgreSQL | 14+ | `redb.Postgres.Pro` |
| SQL Server | 2019+ | `redb.MSSql.Pro` |
| SQLite | 3.44+ | `redb.SQLite.Pro` — pure C#, runs in Blazor WebAssembly & mobile (MAUI) |

Target frameworks: .NET 8.0 (LTS), .NET 9.0, .NET 10.0.

## Documentation

| Resource | Link |
|----------|------|
| Website & Docs | [redbase.app](https://redbase.app) |
| Architecture (deep dive) | [redbase.app/architecture](https://redbase.app/architecture) |
| RedBase (Free edition, full API) | [README.md](README.md) |
| Upgrading | [UPGRADING.md](UPGRADING.md) |
| Changelog | [CHANGELOG.md](CHANGELOG.md) |

## License

RedBase Pro is proprietary software (closed-source), distributed **free of charge — no license key required** starting
from version 3.3.0. The license terms (`LICENSE-PRO.txt`) ship in every Pro package.
