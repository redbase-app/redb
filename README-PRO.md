# RedBase Pro

**Data Platform for .NET** — Pro edition with compiled queries, parallel materialization, and advanced analytics.

[![NuGet](https://img.shields.io/nuget/v/redb.Core.Pro?label=NuGet&color=blue)](https://www.nuget.org/packages/redb.Core.Pro)
[![.NET](https://img.shields.io/badge/.NET-8%20%7C%209%20%7C%2010-purple)](https://dotnet.microsoft.com)
[![Docs](https://img.shields.io/badge/docs-redbase.app-orange)](https://redbase.app)

RedBase Pro extends the open-source [RedBase](https://github.com/redbase-app/redb) core with performance-critical features for production workloads.

---

## What Pro Adds

| Feature | What it does |
|---------|-------------|
| **Compiled Query Execution** | C# LINQ expressions compiled to native SQL — no plpgsql interpreter, no JSON roundtrip |
| **Parallel Materialization** | `Parallel.ForEach` props loading — scales across CPU cores |
| **Change Tracking** | Automatic audit trail: who changed what, when, old/new values |
| **Projection Optimization** | Only requested `_values` rows loaded — `WHERE _id_structure = ANY($2)` |
| **Deep Nested Queries** | Filter by 3+ level nested properties (e.g. `order.Payment.Card.Bank.Country`) |
| **Arithmetic in WHERE** | `e.Salary * 12 > 1_000_000`, `Math.Abs(e.Age - 35) <= 5` |
| **Sql.Function&lt;T&gt;()** | Call any SQL function: `Sql.Function<int>("COALESCE", e.Age, 0)` |
| **Window Functions** | `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, `NTILE`, running aggregates with frames |
| **Schema Migrations** | Structured migration tools for evolving schemas in production |
| **Bulk Operations** | Optimized batch insert/update/delete |

### Free vs Pro — Query Pipeline

```
Free:  LINQ → plpgsql function → JSON → Deserialize → Props
Pro:   LINQ → ProSqlBuilder → native SQL + PVT CTE → Parallel.ForEach → Props
```

Pro compiles your LINQ expression into raw SQL at runtime. No intermediate JSON, no plpgsql interpreter. The query hits the database as a native `SELECT` with JOINs and WHERE clauses — same as hand-written SQL, but generated from C#.

---

## Packages

| Package | NuGet | Description |
|---------|-------|-------------|
| `redb.Core.Pro` | [![NuGet](https://img.shields.io/nuget/v/redb.Core.Pro?label=)](https://www.nuget.org/packages/redb.Core.Pro) | Pro abstractions, compiled query engine, change tracking |
| `redb.Postgres.Pro` | [![NuGet](https://img.shields.io/nuget/v/redb.Postgres.Pro?label=)](https://www.nuget.org/packages/redb.Postgres.Pro) | PostgreSQL Pro provider |
| `redb.MSSql.Pro` | [![NuGet](https://img.shields.io/nuget/v/redb.MSSql.Pro?label=)](https://www.nuget.org/packages/redb.MSSql.Pro) | SQL Server Pro provider |
| `redb.SQLite.Pro` | [![NuGet](https://img.shields.io/nuget/v/redb.SQLite.Pro?label=)](https://www.nuget.org/packages/redb.SQLite.Pro) | SQLite Pro provider — pure C#, no native extension: Blazor WebAssembly & mobile |

## Installation

```bash
# PostgreSQL
dotnet add package redb.Postgres.Pro

# SQL Server
dotnet add package redb.MSSql.Pro
```

Both provider packages include `redb.Core.Pro` as a transitive dependency.

## Quick Start

```csharp
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Pro.Extensions;
using redb.Postgres.Pro.Extensions;  // or redb.MSSql.Pro.Extensions

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRedbPro(options => options
    .UsePostgres("Host=localhost;Database=mydb;Username=postgres;Password=pass")
    // .UseMsSql("Server=localhost;Database=mydb;User Id=sa;Password=pass;TrustServerCertificate=true")
    .Configure(c =>
    {
        c.PropsSaveStrategy = PropsSaveStrategy.ChangeTracking;
        c.EnableLazyReferences = false;   // V4: true makes `virtual` references stubs at any depth
        c.EnablePropsCache = true;
    }));

var app = builder.Build();
var redb = app.Services.GetRequiredService<IRedbService>();
await redb.InitializeAsync();

// Pro: compiled queries, parallel materialization
var results = await redb.Query<EmployeeProps>()
    .Where(e => e.Salary > 75000m)
    .Take(100)
    .ToListAsync();

// Pro: window functions — from E132_WindowRowNumber.cs
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

## Free — no license key

All Pro features are fully functional out of the box — **no license key required** (starting from version 3.3.0). Just install the Pro NuGet packages and use them, in development and in production alike. Pro is proprietary (closed-source) software, distributed free of charge.

## Supported Databases

| Database | Version | Package |
|----------|---------|---------|
| PostgreSQL | 14+ | `redb.Postgres.Pro` |
| SQL Server | 2019+ | `redb.MSSql.Pro` |
| SQLite | 3.44+ | `redb.SQLite.Pro` — pure C#, runs in Blazor WebAssembly & mobile (MAUI) |

## Target Frameworks

- .NET 8.0 (LTS)
- .NET 9.0
- .NET 10.0

## Documentation

| Resource | Link |
|----------|------|
| Website & Docs | [redbase.app](https://redbase.app) |
| Architecture (deep dive) | [redbase.app/architecture](https://redbase.app/architecture) |
| Free edition | [github.com/redbase-app/redb](https://github.com/redbase-app/redb) |
| Changelog | [CHANGELOG.md](CHANGELOG.md) |

## License

RedBase Pro is proprietary software (closed-source), distributed **free of charge — no license key required** starting from version 3.3.0. See LICENSE-PRO.txt for terms.
