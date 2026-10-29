# RouteModule

A Tsak module that stores objects in redb from XML routes (`urn:redb:route:1.0`). The scheme is a C# class of
the project; the routes save and query its objects through a named redb instance of the context, on Pro with
ChangeTracking.

| Part | What it does |
|---|---|
| `Props/OrderProps.cs` | the props class with `[RedbScheme]`: customer, amount, status |
| `context.xml` | `<redb storage="local"><syncScheme .../></redb>`: creates or updates the scheme once at context start; adds the C# route builder |
| `modulectx-orders-save` | every 3 seconds saves one of five orders again with a new amount: `redbSave byUnique="true"` finds it by `value_unique` and updates it, the store keeps five objects |
| `modulectx-orders-query` | every 10 seconds `redbQuery` for the orders over 100, largest first; the condition runs in the database |
| `cs-modulectx-orders-report` | C#: every 15 seconds a typed LINQ query for the open orders of ACME, their number and total |

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run. The worker ships redb, its providers and Pro: the module references `redb.Core` only for
`[RedbScheme]`, and its own assembly (the props classes) travels in the package.

## The redb instance

`storage="local"` names a redb instance of the context, set in `config/context.sample.json` under
`Contexts.modulectx`:

| Key | Here | What |
|---|---|---|
| `RedbInstanceName` | `local` | the instance whose `EnsureCreated` runs: it creates the redb tables in an empty database |
| `Redb.local.Provider` | `sqlite` (by `--db`) | `sqlite`, `postgres` or `mssql` |
| `Redb.local.ConnectionString` | a file next to the worker | the database of the instance |
| `Redb.local.UsePro` | `true` | the Pro engine |
| `Redb.local.PropsSaveStrategy` | `ChangeTracking` | a save writes only the values that changed instead of deleting and inserting the object's whole value set (`DeleteInsert`, the Free default); needs Pro |
| `Redb.local.EnablePropsCache` | `false` | the props cache; size `PropsCacheMaxSize` to the objects the module works with before turning it on, a cache smaller than that evicts what it needs |
| `Redb.local.EnableListCache` | `true` | the list cache |
| `Redb.local.SkipHashValidationOnCacheCheck` | `false` | keep `false` on a cluster: a cache hit is checked against the object's hash in the database |

Another database is two values, `Provider` and `ConnectionString`; the project is created with `--db sqlite`
(the default), `--db postgres` or `--db mssql`, which only fills them in. The password of a server database
goes into the environment of the worker:

    Tsak__Contexts__modulectx__Override__Redb__local__ConnectionString: ${ORDERS_DB}

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup and the redb elements against the props class, then writes
`pkg/routemodule-1.0.0.tpkg` with `RouteModule.dll` inside.

## Run

Copy `config/context.sample.json` as `context.json` next to the package (or put `Contexts.modulectx` into the
worker's appsettings under `Tsak:`), and drop `pkg/routemodule-<version>.tpkg` into the `modules/` folder of a
Tsak worker (`dotnet new tsak-worker` from `redb.Tsak.Templates`, or the `redb-tsak-worker` image). The worker log
shows `Created named IRedbService 'local' (provider=sqlite, pro=True)`, then `saved order-N` and
`orders over 100: N`.

## A typed query in C#

`routes/OrdersReportRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.OrdersReportRoute, RouteModule"/>`. On a timer of its own
(`report.period`, 15 seconds) it reads the same objects through the same instance `local` and logs
`open orders of ACME: 5, total 1500`. It only reads, so it never competes with the XML writer.

The difference to notice: XML writes the condition as text (`where="Amount &gt; 100"`), translated and checked
when the route is built; C# gets the `IRedbService` of the instance (`ProcessWithRedb("local", ...)`) and writes
`redb.Query<OrderProps>().Where(o => o.Customer == "ACME" && o.Status == "new")`, checked by the compiler, with
all of LINQ at hand.

## Change it

- **Your own objects.** Add props classes to `Props/`, one `<syncScheme>` each in `context.xml`.
- **Reading.** `<redbGet id="${header.id}" type="..."/>` loads one object; `<redbQuery>` takes `where`,
  `whereRedb` (base fields: `Id`, `Name`, `DateCreate`, ...), `orderBy`, `take`, `skip` and
  `outputType` `List`, `First`, `Count` or `Any`.
- **Deleting.** `<redbDelete id="${header.id}" storage="local"/>`.
- **The default instance.** Leave `storage` out to use the worker's own redb (`Tsak:Redb` in its appsettings).

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
