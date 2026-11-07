# RouteModule

A Tsak module with scheduled jobs, written in XML markup (`urn:redb:route:1.0`) on Quartz.NET:

| Route | What it does |
|---|---|
| `modulectx-report` | a job on a cron expression, every 30 seconds here, in the configured time zone; a run is never started beside one still running, runs missed while the worker was down are skipped |
| `modulectx-heartbeat` | an interval on the same scheduler, every minute |
| `cs-modulectx-cleanup` | C#: a cleanup job on a cron expression of its own, at second 15 of every minute here, in the same time zone |

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `routes/schedules.route.xml` | the two jobs |
| `routes/CleanupJobRoute.cs` | the cleanup job in C# |
| `context.xml` | the C# route builder; room for beans and the one-time init pipeline |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, the schedule options against the connector and the configuration keys, then writes
`pkg/routemodule-1.0.0.tpkg`.

## Run

Drop `pkg/routemodule-<version>.tpkg` into the `modules/` folder of a Tsak worker (`dotnet new tsak-worker`
from `redb.Tsak.Templates`, or the `redb-tsak-worker` image). The log shows `report job started` and
`report job finished` every 30 seconds and `heartbeat` every minute.

## Cron expressions

Quartz syntax, six or seven fields: seconds, minutes, hours, day of month, month, day of week, optional year. One of
the two day fields is `?`.

| Expression | When |
|---|---|
| `0/30 * * * * ?` | every 30 seconds |
| `0 0 6 ? * MON-FRI` | at 06:00 on working days |
| `0 */15 8-18 * * ?` | every 15 minutes from 08:00 to 18:59 |
| `0 0 2 1 * ?` | at 02:00 on the first of every month |

## Configuration

| Key | Default | What |
|---|---|---|
| `report.schedule` | `0/30 * * * * ?` | the cron expression of the report job |
| `report.timeZone` | `UTC` | the time zone the expression is read in (`Europe/Moscow`) |
| `heartbeat.period` | `60000` | the interval, ms |
| `cleanup.schedule` | `15 * * * * ?` | the cron expression of the C# cleanup job |

## A job in C#

`routes/CleanupJobRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.CleanupJobRoute, RouteModule"/>`. Its job has a name and a schedule of its
own (`cleanup.schedule`) on the same Quartz scheduler, with the same time zone, `stateful` and misfire rule as
the report job. The log shows `cleanup job: removing what is older than <yesterday>` at second 15 of every minute,
between the report runs.

The difference to notice: the endpoint URI is the same text in both forms; what changes is the work of the job,
which in C# is code (`DateOnly` arithmetic here, a query or a file cleanup in a real module) instead of steps.

## Change it

- **Missed runs.** `misfireInstruction=CronFireOnceNow` runs one missed occurrence at start instead of skipping it.
- **Overlap.** Without `stateful=true` a long run and the next one may run together.
- **A cluster.** With a shared Quartz store on the workers a job runs on one node; `timer://` fires on every node.
- **A start and an end.** `startAt` and `endAt` bound the schedule; `customCalendar` excludes holidays.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
