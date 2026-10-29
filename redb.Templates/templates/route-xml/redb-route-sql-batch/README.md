# RouteModule

A Tsak module that writes to a SQL database in batches, written in XML markup (`urn:redb:route:1.0`).

| Route | What it does |
|---|---|
| `modulectx-orders-in` | one test order every 200 ms into the in-memory queue `seda://modulectx-orders`; every seventh one has a negative amount |
| `modulectx-orders-batch` | collects 20 orders, or what came within 3 seconds, and writes them with one `INSERT` per row in one transaction; logs how many rows were written and which rows the database refused |
| `cs-modulectx-orders-batch` | C#: every 5 seconds builds a batch of 20 typed orders itself and writes it the same way; the seventh row is refused |

The table has `CHECK (amount >= 0)`. With `breakBatchOnError=false` every row runs under its own savepoint:
a refused row is undone alone and reported in `redbSql.batchErrors`, the other rows of the batch commit.
With `breakBatchOnError=true` (the default of the connector) the first refused row rolls back the whole
batch and the exchange fails with the database's exception.

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `context.xml` | the `main-db` connection (driver and connection string) and the `CREATE TABLE` that runs before the routes start |
| `routes/orders.route.xml` | the two routes |
| `routes/OrdersBatchRoute.cs` | the batch route in C# |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration, with the connection strings of all three databases |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Databases

The project is created for one database (`--db sqlite`, the default, or `--db postgres`, `--db mssql`). The
worker ships the three drivers and registers them under these names:

| Database | `ProviderName` in `context.xml` | `db.connection` |
|---|---|---|
| SQLite | `Microsoft.Data.Sqlite` | `Data Source=modulectx.db` (the default, a file next to the worker) |
| PostgreSQL | `Npgsql` | `Host=...;Port=5432;Database=...;Username=...;Password=...` |
| SQL Server | `Microsoft.Data.SqlClient` | `Server=...,1433;Database=...;User Id=...;Password=...;TrustServerCertificate=true` |

How the statements travel is the driver's choice: one `DbBatch` round trip per `batchSize` statements on
PostgreSQL and SQL Server when the batch stops at the first error, a reused command per row on SQLite and
whenever every row has its own savepoint. For PostgreSQL and SQL Server `db.connection` has no default, and
the module stops at init until the deployment supplies it.

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, the bean, the endpoint options and the configuration keys, then writes
`pkg/routemodule-1.0.0.tpkg`. No database is needed for the build.

## Run

1. Give the context its configuration: copy `config/context.sample.json` as `context.json` next to the
   package, or put the keys into the worker's appsettings under `Tsak:Contexts:modulectx`. The password
   of a server database goes into an environment variable.
2. Drop `pkg/routemodule-<version>.tpkg` into the `modules/` folder of a Tsak worker (`dotnet new
   tsak-worker` from `redb.Tsak.Templates`, or the `redb-tsak-worker` image).

The log then shows `batch written: 17 row(s)` every few seconds and a line per refused row.

## The same batch in C#

`routes/OrdersBatchRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.OrdersBatchRoute, RouteModule"/>`. On a timer of its own
(`batch.period`, 5 seconds) it writes 20 orders `cs-order-<tick>-<n>` into the same table through the same
`main-db` bean, with the same `batchSize` and `breakBatchOnError=false`, and logs the same two kinds of lines.

The difference to notice: there is no aggregator. The C# route builds the batch itself as a
`List<Order>` of a C# record, and each `:#name` binds from a property of the record (`OrderId`, `Amount`) instead
of from the headers of an aggregated exchange.

## Change it

- **Batch size.** `completionSize` and `completionTimeout` on `<aggregate>` decide how many rows one
  transaction holds; `batchSize` on the `sql:` endpoint is the number of statements per round trip.
- **Where the rows come from.** Each item of the batch binds `:#name` from its own headers
  (`groupedExchange`). A list of dictionaries, JSON objects or the rows of another `sql:` query works the
  same way, by key or property name.
- **Generated keys.** `INSERT ... RETURNING id` (PostgreSQL, SQLite) or `OUTPUT inserted.id` (SQL Server)
  with `outputType=SelectList` puts the keys into `redbSql.generatedKeys`.
- **All or nothing.** Leave `breakBatchOnError` out, and catch the failure with `onException` or a dead
  letter channel.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
