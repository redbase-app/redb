# RouteModule

A Tsak module that works with a SQL database, written in XML markup (`urn:redb:route:1.0`). It is the
outbox pattern: one route writes rows, a poll consumer reads the new ones and marks each as done or failed.

| Route | What it does |
|---|---|
| `modulectx-outbox-write` | inserts a test order into `orders_outbox` (status 0) every 5 seconds; every fifth one has amount 0 |
| `modulectx-outbox-read` | every 2 seconds reads up to 50 rows with status 0, one exchange per row; sets status 1 when the route finished the row, status 2 and the message in `error` when it failed |
| `cs-modulectx-outbox-report` | C#: every 30 seconds logs how many rows are new, done and failed (`report.period`) |

A failed row does not hold up the others and is not read again; the rows with status 2 wait for a person.

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `context.xml` | the `main-db` connection (driver and connection string) and the `CREATE TABLE` that runs before the routes start |
| `routes/outbox.route.xml` | the two routes |
| `routes/OutboxReportRoute.cs` | the report route in C# |
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

To switch later, change `ProviderName`, the connection string and the `CREATE TABLE` in `context.xml`: the
column types and the identity column differ between the three. For PostgreSQL and SQL Server
`db.connection` has no default, and the module stops at init until the deployment supplies it.

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

The log then shows `row N processed` and, for every fifth row, `row N rejected: no amount`.

## A report in C#

`routes/OutboxReportRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.OutboxReportRoute, RouteModule"/>`. Through the same `main-db` bean it
counts the rows per status on a timer of its own and logs `outbox: 0 new, 8 done, 2 failed`. It only reads the
table, so it never takes a row from the XML reader, and its query is the same on all three databases.

The difference to notice: the XML reader gets one exchange per row with the columns as headers; the C# route
sends a `SELECT` with `outputType=SelectList` and gets all rows as a list of column dictionaries, which it
folds into one line in C#.

## Change it

- **Your own table.** Replace `orders_outbox` in `context.xml` and in both routes; the poll query may
  select any columns, every column arrives as a header and in the body.
- **Parameters.** `:#name` in the SQL binds `param.name=...`, a header or a column of the polled row; a
  value never becomes SQL text.
- **Poll rate.** `delay` (ms between polls) and `maxMessagesPerPoll` on the poll endpoint.
- **Writing many rows at once.** See the `redb-route-sql-batch` template.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
