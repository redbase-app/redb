# redb.MSSql

SQL Server provider for **RedBase (REDB)** — Entity Database for .NET.

[![NuGet](https://img.shields.io/nuget/v/redb.MSSql?label=NuGet&color=blue)](https://www.nuget.org/packages/redb.MSSql)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](../LICENSE)

## What's inside

- T-SQL-optimized query generation
- Tree queries with recursive CTEs
- Schema initialization (`redbMSSQL.sql`)
- Full LINQ-to-SQL translation
- Depends on `redb.Core` and `Microsoft.Data.SqlClient`

## Installation

```bash
dotnet add package redb.MSSql
```

## Setup

```csharp
using redb.Core.Extensions;
using redb.MSSql.Extensions;

builder.Services.AddRedb(options => options
    .UseMsSql("Server=localhost;Database=mydb;User Id=sa;Password=pass;TrustServerCertificate=true"));
```

## Links

- Documentation (EN): [redbase.app](https://redbase.app)
- Documentation (RU): [redb.ru](https://redb.ru)
- API Reference: [redbase-app.github.io/redb](https://redbase-app.github.io/redb/)
- GitHub: [github.com/redbase-app/redb](https://github.com/redbase-app/redb)

## License

Apache License 2.0 — see [LICENSE](https://github.com/redbase-app/redb/blob/main/LICENSE).
