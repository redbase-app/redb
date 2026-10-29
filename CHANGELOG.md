# Changelog

All notable changes to RedBase will be documented in this file.
This changelog covers the **NuGet-published packages** only:

| Package | Edition |
|---------|---------|
| `redb.Core` | Free |
| `redb.Postgres` | Free |
| `redb.MSSql` | Free |
| `redb.SQLite` | Free |
| `redb.Export` | Free |
| `redb.Core.Pro` | Pro |
| `redb.Postgres.Pro` | Pro |
| `redb.MSSql.Pro` | Pro |
| `redb.SQLite.Pro` | Pro |
| `redb.Licensing` | Pro |
| `redb.CLI` | Tool |
| `redb.Templates` | Tool |

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Pending changes live in `changelog/unreleased/` until the release; `pwsh scripts/changelog.ps1 -Preview` shows them.

## [4.5.0] — 2026-10-29

### Added

- **`IUserProvider.SetPasswordHashAsync(user, passwordHash)` stores a ready password hash.** Use it for an administrator
  password given in configuration as a BCrypt hash; the default hasher accepts it at login. Custom `IUserProvider`
  implementations keep compiling: the default member throws `NotSupportedException`. Packages: `redb.Core`.
- **`dotnet new redb-identity-app`: an application on redb.Identity.** A Tsak module in the same worker as
  Identity, whose API checks bearer tokens by introspection over direct-vm (cached, 401/403/503 as RFC 6750
  says), and a Blazor web server that signs users in with OpenID Connect. Options: `--web`, `--framework`. Packages: `redb.Templates`.
- **`dotnet new redb-route-eip` creates a Tsak module with the common integration patterns in XML markup.** A seda
  queue, an idempotent consumer, a throttle, a wire tap into an aggregator, a content-based router, a splitter and the
  context's dead letter channel, driven by a timer: it runs without a broker or a database. Packages: `redb.Templates`.
- **`dotnet new redb-route-kafka` creates a Tsak module for Kafka in XML markup.** One connection bean for all
  endpoints, a producer keyed by the order id, a consumer group that retries a failed record twice and then moves
  it to a topic of failed records, and a compose file for a local single-node Kafka. The brokers are a required key. Packages: `redb.Templates`.
- **`dotnet new redb-route-rabbitmq` creates a Tsak module for RabbitMQ in XML markup.** One connection bean for all
  endpoints, a producer, a consumer that retries a failed message twice and then moves it to a queue of failed
  messages, and a compose file for a local broker. The password is a required configuration key. Packages: `redb.Templates`.
- **`dotnet new redb-route-rest` creates a Tsak module with an HTTP JSON API in XML markup.** A `<rest>` block with
  a GET and a POST, the POST body checked against a JSON schema from `resources/`, a 400 answer on a bad body and
  the OpenAPI document at `/api/openapi.json`. Packages: `redb.Templates`.
- **`dotnet new redb-route-sql-batch` creates a Tsak module that writes to SQL in batches in XML markup.** Single
  messages are aggregated into batches of 20 and written in one transaction; a row the database refuses is undone
  under its savepoint and reported, the others commit. `--db sqlite` (the default), `postgres` or `mssql`. Packages: `redb.Templates`.
- **`dotnet new redb-route-sql` creates a Tsak module with the SQL outbox pattern in XML markup.** A route writes
  rows, a poll consumer reads the new ones and marks each done, or failed with the reason. `--db sqlite` (the default,
  no server needed), `postgres` or `mssql` picks the driver, the connection string and the table definition. Packages: `redb.Templates`.
- **`dotnet new redb-route` creates a Tsak module with routes in XML markup.** A timer route, `context.xml`, the
  context configuration sample and the `redb-route-xml` tool as a local tool; `dotnet build -p:PackRouteOnBuild=true`
  checks the markup and writes the `.tpkg`. Options: `--context`, `--framework`. Packages: `redb.Templates`.
- **Every `redb-route-*` template puts an `AGENTS.md` into the project.** It tells an AI coding agent what the
  project is, where the schema and the connector options come from, how keys, secrets and beans are written, how to
  check a change and how the module is deployed, plus a section on the routes of that template. Packages: `redb.Templates`.
- **`dotnet new redb-route-as2` creates a Tsak module for AS2 in XML markup.** It sends a signed and encrypted
  document and verifies the partner's signed MDN, and receives with duplicate detection. A compose file makes test keys
  and runs a local exchange with OpenAS2 in both directions. Packages: `redb.Templates`.
- **`dotnet new redb-route-as4` creates a Tsak module for AS4 (eDelivery) in XML markup.** A route that sends a
  signed and encrypted message and waits for the partner's signed receipt, a receiving route with duplicate detection,
  and a compose file for a local exchange with Holodeck B2B. The key file, its password and the partner's
  certificate are required configuration keys. Packages: `redb.Templates`.
- **`dotnet new redb-route-beans` creates a Tsak module whose XML routes call a C# class of the project.** The bean is
  built in `context.xml` from configuration (constructor argument and property), the routes call a method with
  expression arguments and one that takes the exchange; the project's assembly travels in the package. Packages: `redb.Templates`.
- **`dotnet new redb-route-cron` creates a Tsak module with scheduled jobs in XML markup.** A job on a Quartz cron
  expression in a configured time zone, never started beside a run still going and skipping runs missed while the
  worker was down, and an interval on the same scheduler. Packages: `redb.Templates`.
- **`dotnet new redb-route-email` creates a Tsak module that sends and reads e-mail in XML markup.** A sender over SMTP
  with retries and a reader of the unseen messages of an IMAP inbox that marks each read once processed, with one
  connection bean per server. A compose file runs GreenMail and the module on a worker. Packages: `redb.Templates`.
- **`dotnet new redb-route-file` creates a Tsak module that processes files in XML markup.** An inbox polled for new
  files, each checked against a schema, moved to a done or an error folder, with a receipt written to an outbox.
  `--transport file` (the default), `ftp` or `sftp` picks the connector; a compose file runs a local FTP or SFTP server. Packages: `redb.Templates`.
- **`dotnet new redb-route-ldap` creates a Tsak module for LDAP in XML markup.** A watch that loads the user entries
  once and then follows their changes, a search on a timer and a password check by bind, all through one connection
  bean. A compose file starts OpenLDAP with test entries and the module on a worker. Packages: `redb.Templates`.
- **`dotnet new redb-route-redb` creates a Tsak module that stores redb objects from XML routes.** A props class of
  the project is the scheme, synced at context start; the routes save objects by a unique key and query them through
  a named redb instance of the context on Pro with ChangeTracking. `--db sqlite` (the default), `postgres` or `mssql`. Packages: `redb.Templates`.
- **`dotnet new redb-route-s3` creates a Tsak module for S3 in XML markup.** A route writes objects into a bucket, a
  poll consumer reads the new ones and moves each to an archive bucket once it is processed. One connection bean serves
  AWS S3 and S3-compatible stores; a compose file runs a local store and the module on a worker. Packages: `redb.Templates`.
- **`dotnet new redb-route-scatter-gather` creates a Tsak module with scatter-gather in XML markup.** One request
  goes to three recipients in parallel, the answers that arrive within the timeout are folded into the best price,
  a recipient that is too slow is left out. It runs without a broker. Packages: `redb.Templates`.
- **`dotnet new redb-route-validator-xslt` creates a Tsak module that checks XML against an XSD and transforms it
  with XSLT, in XML markup.** The schema and the stylesheet live in `resources/` and travel in the package; a document
  the schema refuses goes to its own route with the reason. It runs without a broker. Packages: `redb.Templates`.
- **`dotnet new redb-route-amqp` creates a Tsak module for AMQP 1.0 in XML markup.** One connection bean for all
  endpoints, a producer, a consumer that retries a failed message twice and then moves it to a queue of failed
  messages, and a compose file that runs ActiveMQ Artemis and the module on a worker. Packages: `redb.Templates`.
- **`dotnet new redb-route-mqtt` creates a Tsak module for MQTT in XML markup.** One connection bean, test sensors
  that publish with QoS 1, a retained status topic, a subscriber with topic wildcards that forwards readings over a
  limit, and a compose file that runs Mosquitto and the module on a worker. Packages: `redb.Templates`.
- **Every `redb-route-*` template shows its scenario in XML markup and in C#.** Next to the XML route files each
  module has a `RouteBuilder` in `routes/*.cs`, added to the same context by `<routeBuilder type="...">` in
  `context.xml`, with the same beans and configuration keys; the package carries the module's DLL. The README of each
  template names the one difference to notice between the two forms. Packages: `redb.Templates`.

### Changed

- **The props cache fills a loaded graph in one pass.** Loading an object with nested references no longer walks its Props twice, so the load path is faster; cache contents, hash checks and invalidation stay the same. Packages: `redb.Core`.

### Fixed

- **`DistinctRedb()` is applied by the free providers.** It returned every object; it now keeps one object per value of
  the object's own fields, as the Pro providers do. Packages: `redb.Core`, `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.
- **A cached object is no longer served with a reference that has changed or left for the trash.** The free providers
  returned the graph they had cached, so an object loaded before a soft-delete kept showing the trashed target as live. Packages: `redb.Core`, `redb.Postgres`, `redb.MSSql`, `redb.SQLite`.
- **After `SaveAsync` and `SaveByUniqueAsync` the object carries the dates its row holds.** An object without dates
  (deserialized from JSON, say) saved onto an existing row came back with `DateCreate` and `DateModify` equal to
  0001-01-01; now it has the stored creation date and the new modification date, as a reload shows. Packages: `redb.Core`, `redb.Postgres`, `redb.MSSql`, `redb.SQLite`, `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **`Redb.Maintenance` works on SQL Server for a login without `VIEW SERVER STATE`.** `GetStatisticsWindowAsync` and
  `GetIndexStatsAsync` failed with a permission error; now the usage counters and their start time come back null, and
  tables, indexes and sizes are read as before. Packages: `redb.MSSql`.
- **`AggregateAsync` with a field of the object next to Props fields works on the free SQL Server and SQLite
  providers.** `x => new { SumId = Agg.Sum(x.Id), Total = Agg.Sum(x.Props.Salary) }` failed: SQL Server with a
  conversion error, SQLite with `pvt_build_aggregate_sql: unsupported`. Packages: `redb.MSSql`, `redb.SQLite`.
- **`DistinctBy` and `DistinctRedb` work over trees on SQL Server.** A tree query with either failed with
  "Invalid column name" for the field it ordered or ranked by, and `DistinctRedb()` partitioned by the first of its
  columns only. The ordering and every key are now evaluated where the pivot columns resolve. Packages: `redb.MSSql`.
- **The SQL preview of a tree query works on SQL Server.** `ToSqlStringAsync()` failed with
  "Could not find stored procedure 'dbo.get_search_tree_sql_preview_base'"; it now returns the statement
  `dbo.pvt_build_query_sql` compiles for the query. Packages: `redb.MSSql`.
- **`DistinctRedb()` works again on SQL Server.** Since 4.4.2 `Query<T>().DistinctRedb()` failed with "The column '_name'
  was specified multiple times for '_ranked'". Packages: `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **`Distinct()` and `DistinctRedb()` work over trees and keep the ordering of the query.** Over a tree, SQL Server returned
  every node and its `CountAsync` failed, and `DistinctRedb()` failed on PostgreSQL and SQLite ("_id_parent is ambiguous");
  on PostgreSQL the rows came back in the order of the distinct key, so `OrderBy` and `Skip`/`Take` cut the wrong page. Packages: `redb.Postgres.Pro`, `redb.MSSql.Pro`, `redb.SQLite.Pro`.
- **`DistinctBy` and `DistinctRedb` keep the page the query asks for.** The rows came back in ascending key order
  whatever the `OrderBy` said, so `Skip` and `Take` cut the wrong page, and `DistinctRedb()` deduplicated by the
  first of its fields only. Both now follow the query, as the Pro provider does. Packages: `redb.SQLite`.
- **`Skip` without `Take`, and the SQL preview of a tree query, work on SQLite.** A query with `Skip` alone failed with
  `near "OFFSET": syntax error`, and `ToSqlStringAsync()` of a `TreeQuery` failed with
  `no such function: get_search_tree_sql_preview_base`. Packages: `redb.SQLite`.
- **`Skip` without `Take` works over a tree on SQLite.** `TreeQuery(...).Skip(n)` failed with `near "OFFSET": syntax error`. Packages: `redb.SQLite.Pro`.

## Earlier versions

Earlier releases are archived by line in [changelog/](changelog/): [4.4.x](changelog/CHANGELOG-4.4.x.md), [4.3.x](changelog/CHANGELOG-4.3.x.md), [4.2.x](changelog/CHANGELOG-4.2.x.md), [4.1.x](changelog/CHANGELOG-4.1.x.md), [4.0.x](changelog/CHANGELOG-4.0.x.md), [3.x](changelog/CHANGELOG-3.x.md), [2.x](changelog/CHANGELOG-2.x.md), [1.x](changelog/CHANGELOG-1.x.md).
