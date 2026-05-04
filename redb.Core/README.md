# redb.Core

Core library for **RedBase (REDB)** — Entity Database for .NET.

[![NuGet](https://img.shields.io/nuget/v/redb.Core?label=NuGet&color=blue)](https://www.nuget.org/packages/redb.Core)
[![Downloads](https://img.shields.io/nuget/dt/redb.Core?label=Downloads&color=green)](https://www.nuget.org/packages/redb.Core)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](../LICENSE)

## What's inside

- `IRedbService` — main service interface (CRUD, queries, trees, lists)
- LINQ query builder with SQL translation
- Schema management via `[RedbScheme]` attribute
- EAV storage engine with typed Props
- Tree structures (CTE-based), list items, object references
- Aggregation (Sum, Avg, Min, Max, GroupBy) and window functions
- Caching, serialization, security providers

## Installation

```bash
# You typically install a provider package, which pulls in redb.Core automatically:
dotnet add package redb.Postgres   # PostgreSQL
dotnet add package redb.MSSql      # SQL Server
```

## Quick Start

```csharp
using redb.Core;
using redb.Core.Extensions;
using redb.Postgres.Extensions;

builder.Services.AddRedb(options => options
    .UsePostgres("Host=localhost;Database=mydb;Username=postgres;Password=pass"));

var redb = app.Services.GetRequiredService<IRedbService>();
await redb.InitializeAsync();
await redb.SyncSchemeAsync<EmployeeProps>();

// Save
await redb.SaveAsync(new RedbObject<EmployeeProps> { Name = "Alice", Props = new() { Age = 28 } });

// Query
var results = await redb.Query<EmployeeProps>().Where(e => e.Salary > 75000m).ToListAsync();
```

## Links

- Documentation (EN): [redbase.app](https://redbase.app)
- Documentation (RU): [redb.ru](https://redb.ru)
- API Reference: [redbase-app.github.io/redb](https://redbase-app.github.io/redb/)
- Examples: [redb.Examples](../redb.Examples/)
- GitHub: [github.com/redbase-app/redb](https://github.com/redbase-app/redb)

## License

MIT
