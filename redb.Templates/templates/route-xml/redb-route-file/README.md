# RouteModule

A Tsak module that processes files from an inbox folder, written in XML markup (`urn:redb:route:1.0`). The folders
are on the worker's disk, on an FTP or on an SFTP server: the project was created with `--transport`
(`file`, `ftp` or `sftp`), which picks the connector, the URI scheme and the connection.

| Route | What it does |
|---|---|
| `modulectx-files-feed` | test input: every 20 seconds an order file into the inbox, every fifth one not XML; delete it in a real module |
| `modulectx-files-in` | every 5 seconds the new `*.xml` files of the inbox, one exchange each: checked against `resources/order.xsd`, a receipt written to the outbox; a finished file moves to `done/`, a failed one to `error/` |
| `cs-modulectx-files-feed`, `cs-modulectx-files-in` | C#: every 30 seconds a CSV order file into an inbox of its own (`inbox-cs`); the reader sums the amounts, writes a receipt to the same outbox, and a file with a line without an amount moves to `error/` |

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `context.xml` | for FTP and SFTP the `remote` connection: host, port, user, password; the C# route builder |
| `routes/files.route.xml` | the two routes |
| `routes/CsvFilesRoute.cs` | the CSV routes in C# |
| `resources/order.xsd` | the schema of an order file |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `deploy/docker-compose.files.yml` | the module on a Tsak worker, with a local FTP or SFTP server for those transports |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, every endpoint option against the connector, the beans, the resources and the
configuration keys, then writes `pkg/routemodule-1.0.0.tpkg`. No server is needed for the build.

## Try it locally

    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0
    cp deploy/.env.example deploy/.env          # FTP and SFTP: set FILES_PASSWORD
    docker compose -f deploy/docker-compose.files.yml up -d

The module log shows `processed order-N.xml: <order ...>`; every fifth file fails the schema check and moves to
`error/`. With `file` the folders are under `deploy/data`.

## Configuration

| Key | Default | What |
|---|---|---|
| `files.inbox`, `files.outbox` | `inbox`, `outbox` | the folders; relative to the worker's working folder (file) or to the login folder (FTP, SFTP) |
| `files.host`, `files.port` | `localhost`, 21 or 22 | FTP and SFTP: the server |
| `files.username`, `files.password` | none, required | FTP and SFTP: the login; the password from the environment |
| `files.period` | `20000` | the test feed's interval, ms |
| `files.csInbox`, `files.csPeriod` | `inbox-cs`, `30000` | the inbox of the C# routes and their feed's interval, ms |

In a deployment the password goes into the environment of the worker, in the `Override` section:

    Tsak__Contexts__modulectx__Override__files.password: ${FILES_PASSWORD}

## CSV files in C#

`routes/CsvFilesRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.CsvFilesRoute, RouteModule"/>`. It runs the same inbox pattern for CSV
files on a folder of its own, `inbox-cs`, with the same `done/` and `error/` folders and receipts into the same
outbox, so the two forms never read the same file. The log shows `processed orders-1: total 9` and, for every
third file, `refused orders-2: ...`.

The difference to notice: XML checks a file with a schema; C# reads the lines and sums the amounts with LINQ, and
a line that does not parse fails the file. The transport is chosen once for both forms: `--transport` switches
the scheme and the `connectionFactory=remote` option in the C# file as well.

## Change it

- **Files still being written.** `readLock=Changed` (waits until the size stops changing), or `doneFileName=${file:name}.done`
  when the sender writes a marker after the file.
- **Delete instead of move.** `delete=true` and no `moveTo`.
- **Order and volume.** `sortBy`, `maxMessagesPerPoll`, `recursive=true` with `maxDepth`.
- **Twice the same file.** `idempotent=true` remembers what was read.
- **FTPS, keys, host pinning.** `useFtps=true` (FTP); `PrivateKeyPath`, `ServerFingerprint` or `KnownHostsFile`
  with `StrictHostKeyChecking` on the `remote` bean (SFTP).

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
