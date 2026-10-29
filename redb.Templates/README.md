# RedBase Project Templates

Create a new RedBase application in seconds:

```bash
dotnet new install redb.Templates
dotnet new redb -n MyApp
cd MyApp
dotnet run
```

Every project runs as is: SQLite creates its database file next to the app, and Pro is on. Pro is free of
charge for the whole 4.x line, no license key required.

## Templates

| Short name | What it is |
|------------|------------|
| `redb` | Console app: create, load, update, query, tree |
| `redb-razor` | Razor Pages site: list with search, sorting and paging, create / edit form |
| `redb-blazor` | Blazor site (Interactive Server): products list and form, category tree |
| `redb-worker` | Integration worker: folder inbox, XSD check, one transaction, receipt; own host or a Tsak module |
| `redb-chat` | LLM chat with the history in RedBase; console and HTTP; options for tools and audit; own host or a Tsak module |
| `redb-app` | Blazor WebAssembly client and a redb.Route REST API with sign-in (JWT); the API runs in its own host or as a Tsak module |
| `redb-bff` | Blazor backend-for-frontend with a cookie session, and a backend of redb.Route controllers; the backend runs in its own host or as a Tsak module |
| `redb-identity-app` | An application on redb.Identity: a Tsak module next to Identity in one worker, whose API checks bearer tokens by introspection over direct-vm, and a Blazor web server that signs users in with OpenID Connect. Options: `--web`, `--framework` |
| `redb-route` | Tsak module with routes in XML markup: a timer route in XML and the same route in C#, the context configuration sample, packaging into `.tpkg` on build |
| `redb-route-rest` | Tsak module with an HTTP JSON API in XML markup: `<rest>` with GET and POST, a JSON schema check, 400 on a bad body, OpenAPI; the same API in C# under `/api/cs` |
| `redb-route-rabbitmq` | Tsak module for RabbitMQ in XML markup: one shared connection, a producer, a consumer with retries, a queue for the messages that still fail, request-reply in C#, a compose file for a local broker |
| `redb-route-kafka` | Tsak module for Kafka in XML markup: one cluster connection, a keyed producer, a consumer group with retries, a topic for the records that still fail, payments with running totals in C#, a compose file for a local single-node Kafka |
| `redb-route-eip` | Tsak module with the common integration patterns in XML markup, no broker needed: seda queue, idempotent consumer, throttle, wire tap, aggregator, content-based router, splitter, dead letter channel; the same chain in C# |
| `redb-route-sql` | Tsak module with the SQL outbox pattern in XML markup: a route writes rows, a poll consumer marks each done or failed, a status report in C#; `--db sqlite` (default), `postgres` or `mssql` |
| `redb-route-sql-batch` | Tsak module that writes to SQL in batches in XML markup: single messages aggregated into batches of 20, one transaction per batch, a refused row skipped and reported, a batch of typed records from C#; `--db sqlite` (default), `postgres` or `mssql` |
| `redb-route-scatter-gather` | Tsak module with scatter-gather in XML markup: one request to three recipients in parallel, a timeout, the best answer picked; a second request in C# with its own strategy function; no broker needed |
| `redb-route-validator-xslt` | Tsak module for XML documents in XML markup: an XSD check, an XSLT transformation, a rejected document routed with the reason; the same route in C# with LINQ to XML; no broker needed |
| `redb-route-beans` | Tsak module whose XML routes call a C# class of the project: a bean built from configuration, a method with expression arguments, a method that takes the exchange; a second route in C# with the same bean; no broker needed |
| `redb-route-redb` | Tsak module that stores redb objects from XML routes: a props class of the project as the scheme, save by a unique key, query, a named redb instance on Pro with ChangeTracking and switchable caches, a typed LINQ report in C#; `--db sqlite` (default), `postgres` or `mssql` |
| `redb-route-ldap` | Tsak module for LDAP in XML markup: a directory watch with an initial load, a search on a timer, a password check by bind, a user list in C#; a compose file with a seeded OpenLDAP |
| `redb-route-s3` | Tsak module for S3 in XML markup: objects written to a bucket, a poll consumer that reads the new ones and moves them to an archive bucket, CSV price lists in C#; a compose file with an S3-compatible store |
| `redb-route-email` | Tsak module that sends and reads e-mail in XML markup: an SMTP sender with retries, an IMAP reader that marks processed messages read, an HTML report sent from C#; a compose file with GreenMail |
| `redb-route-file` | Tsak module for files in XML markup: an inbox polled for new files, done and error folders, a receipt per file, CSV files through an inbox of their own in C#; `--transport file` (default), `ftp` or `sftp`, with a compose file for a local FTP or SFTP server |
| `redb-route-cron` | Tsak module with scheduled jobs in XML markup: a Quartz cron job with a time zone, no overlapping runs and a misfire rule, and a Quartz interval, a cleanup job in C#; no broker needed |
| `redb-route-amqp` | Tsak module for AMQP 1.0 in XML markup (ActiveMQ Artemis, Qpid, Azure Service Bus and others): one connection, a producer, a consumer with retries, a queue for the messages that still fail, request-reply in C#; a compose file with Artemis |
| `redb-route-mqtt` | Tsak module for MQTT 3.1.1 and 5 in XML markup: test sensors publishing with QoS 1, a retained status, a subscriber with topic wildcards that forwards readings over a limit, humidity averages in C#; a compose file with Mosquitto |
| `redb-route-as2` | Tsak module for AS2 in XML markup: sends a signed and encrypted document and verifies the signed MDN, receives with duplicate detection, XML invoices sent from C#; a compose file for a local exchange with OpenAS2 |
| `redb-route-as4` | Tsak module for AS4 (eDelivery) in XML markup: sends a signed and encrypted message and verifies the partner's signed receipt, receives with duplicate detection, credit notes sent from C#; a compose file for a local exchange with Holodeck B2B |

Every template configures RedBase the same way: with Pro, the PVT prefilter and change tracking are on, the props cache is
off and the comment next to it shows how to turn it on.

Every `redb-route-*` module shows its scenario in both forms a real project mixes: the XML route files, and a
`RouteBuilder` in `routes/*.cs` that `<routeBuilder type="...">` in `context.xml` adds to the same context, with the
same beans and configuration keys. The C# routes have ids that start with `cs-` and triggers, queues and folders of
their own, so the two forms never consume the same message twice.

## `redb`: console app

### Parameters

| Parameter | Values | Default | Description |
|-----------|--------|---------|-------------|
| `--db` | `sqlite`, `postgres`, `mssql` | `sqlite` | Database provider |
| `--pro` | `true`, `false` | `true` | Pro packages: compiled LINQ, change tracking, aggregation |

### Examples

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

### What you get

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
