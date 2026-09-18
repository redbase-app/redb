# RedBase Project Templates

Create a new RedBase application in seconds:

```bash
dotnet new install redb.Templates
dotnet new redb -n MyApp
cd MyApp
dotnet run
```

The default project runs as is: SQLite creates its database file next to the app, and Pro is on. Pro is free of
charge for the whole 4.x line, no license key required.

## Parameters

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `--db` | `sqlite`, `postgres`, `mssql` | `sqlite` | Database provider |
| `--pro` | `true`, `false` | `true` | Pro packages: compiled LINQ, change tracking, aggregation |

## Examples

```bash
# SQLite + Pro (default)
dotnet new redb -n MyApp

# SQLite + Free
dotnet new redb -n MyApp --pro false

# PostgreSQL + Pro
dotnet new redb -n MyApp --db postgres

# SQL Server + Free
dotnet new redb -n MyApp --db mssql --pro false
```

For PostgreSQL and SQL Server, put your password into the connection string in `Program.cs` first. The database
named there must already exist: the app creates the RedBase tables in it, not the database itself.

## What you get

A console app that walks through the basics:

- **Initialize** — `InitializeAsync(ensureCreated: true)` creates the RedBase tables on the first run and syncs the
  schemes of your classes
- **Create, load, update, delete** — `RedbObject<Product>` with typed properties, an array among them
- **Query** — LINQ over your own properties: `Query<Product>().Where(...).OrderByDescending(...)`
- **Aggregation (Pro)** — `AverageAsync` computed in the database
- **Tree** — a three-level category hierarchy with `CreateChildAsync` and `LoadTreeAsync`; deleting the root
  deletes its children

To switch the database later, follow the comments in `RedbApp.csproj` and `Program.cs`.

## Links

- Documentation (EN): [redbase.app](https://redbase.app)
- Documentation (RU): [redb.ru](https://redb.ru)
- API Reference: [redbase-app.github.io/redb](https://redbase-app.github.io/redb/)
- Quick Start: [redbase.app/quick-start](https://redbase.app/quick-start)
- GitHub: [github.com/redbase-app/redb](https://github.com/redbase-app/redb)
- NuGet: [nuget.org/packages/redb.Templates](https://www.nuget.org/packages/redb.Templates)
