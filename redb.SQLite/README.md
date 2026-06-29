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

## Links

- Documentation (EN): [redbase.app](https://redbase.app)
- Documentation (RU): [redb.ru](https://redb.ru)
- API Reference: [redbase-app.github.io/redb](https://redbase-app.github.io/redb/)
- GitHub: [github.com/redbase-app/redb](https://github.com/redbase-app/redb)

## License

Apache License 2.0 — see [LICENSE](https://github.com/redbase-app/redb/blob/main/LICENSE).
