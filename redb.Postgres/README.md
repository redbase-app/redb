# redb.Postgres

PostgreSQL provider for **RedBase (REDB)** — Entity Database for .NET.

[![NuGet](https://img.shields.io/nuget/v/redb.Postgres?label=NuGet&color=blue)](https://www.nuget.org/packages/redb.Postgres)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](../LICENSE)

## What's inside

- PostgreSQL-optimized SQL generation
- Tree queries with recursive CTEs
- Schema initialization (`redbPostgre.sql`)
- Full LINQ-to-SQL translation
- Depends on `redb.Core` and `Npgsql`

## Installation

```bash
dotnet add package redb.Postgres
```

## Setup

```csharp
using redb.Core.Extensions;
using redb.Postgres.Extensions;

builder.Services.AddRedb(options => options
    .UsePostgres("Host=localhost;Port=5432;Database=mydb;Username=postgres;Password=pass"));
```

## Links

- Documentation (EN): [redbase.app](https://redbase.app)
- Documentation (RU): [redb.ru](https://redb.ru)
- API Reference: [redbase-app.github.io/redb](https://redbase-app.github.io/redb/)
- GitHub: [github.com/redbase-app/redb](https://github.com/redbase-app/redb)

## License

MIT
