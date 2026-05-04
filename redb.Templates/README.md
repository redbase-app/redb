# RedBase Project Templates

Create a new RedBase application in seconds:

```bash
dotnet new install redb.Templates
dotnet new redb -n MyApp
```

## Parameters

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `--db` | `postgres`, `mssql` | `postgres` | Database provider |
| `--pro` | `true`, `false` | `false` | Include Pro features (LINQ, analytics, change tracking) |

## Examples

```bash
# PostgreSQL + Free
dotnet new redb -n MyApp

# SQL Server + Free  
dotnet new redb -n MyApp --db mssql

# PostgreSQL + Pro
dotnet new redb -n MyApp --pro true

# SQL Server + Pro
dotnet new redb -n MyApp --db mssql --pro true
```

## What you get

A ready-to-run console app with:

- **CRUD** — create, load, update, delete
- **Tree hierarchy** — parent-child with cascade delete
- **Search** — `SearchAsync` by name
- **Pro (optional)** — LINQ queries, aggregation

Edit the connection string in `Program.cs` and run:

```bash
cd MyApp
dotnet run
```

## Links

- Documentation (EN): [redbase.app](https://redbase.app)
- Documentation (RU): [redb.ru](https://redb.ru)
- API Reference: [redbase-app.github.io/redb](https://redbase-app.github.io/redb/)
- Quick Start: [redbase.app/quick-start](https://redbase.app/quick-start)
- GitHub: [github.com/redbase-app/redb](https://github.com/redbase-app/redb)
- NuGet: [nuget.org/packages/redb.Templates](https://www.nuget.org/packages/redb.Templates)
