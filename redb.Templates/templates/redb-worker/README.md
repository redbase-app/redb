# RedbWorker

An integration worker on RedBase and redb.Route: order files arrive in a folder, are checked against an
XSD schema, stored in RedBase in one transaction, and answered with a receipt file. SQLite with Pro, so it
runs as is.

The same module runs in two places: in its own console host, and on a Tsak worker.

## Run

```bash
dotnet run --project RedbWorker.Host
```

On the first run the host creates `redbworker.db` and the folders under `data/` in the current directory.
In a second terminal:

```bash
cp samples/order-1001.xml data/inbox/        # accepted
cp samples/order-1001.xml data/inbox/again.xml   # duplicate: the same order id
cp samples/order-invalid.xml data/inbox/     # rejected: violates the schema
```

Every file taken from the inbox gets a receipt in `data/outbox` and moves to `data/archive`. A file
that violates the schema - a negative amount, a currency that is not three capitals - and a file that
is not XML at all are both answered with `Rejected`: the schema check wraps a parsing error in the same
`ValidationException`. A file that fails for another reason (the database is down, the runtime cannot
process it) moves to `data/error` and is picked up again on the next poll.

## Where things are

| File | What it does |
|------|--------------|
| `RedbWorker.Module/InitRoute.cs` | The module's entry point: components, schemes, routes. Tsak calls the same method |
| `RedbWorker.Module/Routes/OrderInboxRouteBuilder.cs` | inbox -> schema -> `.Transacted()` -> receipt |
| `RedbWorker.Module/Routes/ExceptionRouteBuilder.cs` | A schema violation becomes a Rejected receipt |
| `RedbWorker.Module/Services/OrderService.cs` | Stores the order; the order id is the object's unique key |
| `RedbWorker.Module/Xml/` | `Order.xsd` and the XML classes |
| `RedbWorker.Module/RedbWorker.Module.config.json` | The module's settings: folders |
| `RedbWorker.Host/Program.cs` | The own host: redb registration and settings, then `InitRoute.main` |
| `RedbWorker.Host/appsettings.json` | `ConnectionStrings:Redb` and the `Override` layer of the module settings |

## Settings

The module reads its settings from its route context. Both hosts fill it the same way: the module's
config file first, then `Tsak:Contexts:redbworker:Override`, which wins. So an environment variable such
as `Tsak__Contexts__redbworker__Override__Folders__Inbox=/data/inbox` works in both.

## RedBase settings

`RedbWorker.Host/Program.cs` turns on the PVT prefilter and change tracking. The props cache is off; the
comment next to it shows the three lines that turn it on. On Tsak the same settings are environment
variables of the worker (`Tsak__Redb__...`, see `deploy/docker-compose.tsak.yml`).

## Other transports

The route reads a local folder. For another source, replace the `From(...)` line:

- SFTP: package `redb.Route.Sftp`, `From(Sftp.Directory("orders").ConnectionFactory("partner"))`
- AS2: package `redb.Route.As2`, `From(As2.Receive(...))`
- a database table: package `redb.Route.Sql`, `From(Sql.Poll(...))`

## Another database

1. In `RedbWorker.Host.csproj` replace `redb.SQLite.Pro` with `redb.Postgres.Pro` or `redb.MSSql.Pro`.
2. In `Program.cs` replace `using redb.SQLite.Pro.Extensions` and `.UseSqlite(...)` as the comment there
   says.
3. Put the connection string into `appsettings.json`. The database must exist: RedBase creates its
   tables, not the database.

## Deploy

**Own host**

- `Dockerfile`: the host with SQLite and the folders on a volume.
  `docker build -t redbworker . && docker run -v redbworker-data:/data redbworker`
- `deploy/docker-compose.postgres.yml`, `deploy/docker-compose.mssql.yml`: the host with a database
  server. Switch the provider first (see above); each file has the commands at the top.

**Tsak**

1. `cp deploy/.env.example deploy/.env` and fill in the four values.
2. `pwsh deploy/pack-tpkg.ps1` packs the module into `deploy/output/RedbWorker.tpkg`.
3. `docker compose -f deploy/docker-compose.tsak.yml up -d` starts the Tsak stack with the module.
4. Put a sample into `runtime/inbox`; the dashboard is at http://localhost:8085.

`pack-tpkg.ps1` puts the manifest, the module config, the module DLL and every dependency the worker
does not ship itself into the package; `deploy/shipped-module-deps.txt` lists what the worker provides,
and `deploy/output/RedbWorker.tpkg.contents.txt` shows what went in. To run the module on a Tsak host
built locally from the `tsak-worker` template, drop the assemblies that host lacks
(`redb.Route.File.dll`, `redb.Route.GenericFile.dll`, ...) into its `Libs/shared`: the host resolves a
module's shared assemblies from `Libs/shared` next to the application, and that template copies
`Libs/**` to its output.
