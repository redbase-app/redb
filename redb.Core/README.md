# redb.Core

Core library for **RedBase (REDB)** — Entity Database for .NET.

[![NuGet](https://img.shields.io/nuget/v/redb.Core?label=NuGet&color=blue)](https://www.nuget.org/packages/redb.Core)
[![Downloads](https://img.shields.io/nuget/dt/redb.Core?label=Downloads&color=green)](https://www.nuget.org/packages/redb.Core)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](../LICENSE)

## What's inside

- `IRedbService` — main service interface (CRUD, queries, trees, lists)
- LINQ query builder with SQL translation
- Schema management via `[RedbScheme]` attribute
- Typed Props storage engine — every property maps to a typed column, not a JSON blob
- Tree structures (CTE-based), list items, object references
- Aggregation (Sum, Avg, Min, Max, GroupBy) and window functions
- Caching, serialization, security providers

## Platforms

Runs on any .NET 8/9/10 target, including **Blazor WebAssembly** and **mobile** (with the SQLite Pro
provider). Object and scheme hashing goes through `RedbMd5`, which uses the platform MD5 provider where
one exists and a managed RFC 1321 implementation where none does — browser-wasm ships no MD5 provider.
The two are bit-for-bit identical, so hashes written by a browser and by a server are interchangeable.

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

Apache License 2.0 — see [LICENSE](https://github.com/redbase-app/redb/blob/main/LICENSE).
