redb objects (`redb.Route.Core`), `Props/OrderProps.cs` and `routes/orders.route.xml`: `OrderProps` with
`[RedbScheme]` is the scheme; `<redb storage="local"><syncScheme type="RouteModule.Props.OrderProps, RouteModule"/></redb>`
in `context.xml` brings it up to date at context start. The writer saves one of five orders with
`<redbSave ... byUnique="true" storage="local"/>` (the body is `{"name":..., "value_unique":..., "properties":{...}}`,
the same `value_unique` updates the same object); the reader runs `<redbQuery ... where="Amount &gt; 100" orderBy="Amount">`.

Facts to keep: `storage="local"` names the redb instance of the context, configured in `config/context.sample.json`
under `Contexts.modulectx.Redb.local` (Provider, ConnectionString, UsePro, PropsSaveStrategy, EnsureCreated, cache
keys), with `RedbInstanceName` naming the one whose `EnsureCreated` runs; `PropsSaveStrategy` `ChangeTracking` needs
`UsePro` true; the worker ships redb and its providers, the module references `redb.Core` only for `[RedbScheme]`;
`RoutePackageIncludeAssembly` puts the props classes into the package; `where` and `orderBy` run in the database, an
unknown property refuses the route; `<idempotentRepository>` inside `<redb storage=...>` is not used in this version.

Usually changed: the props classes, the query, the provider and connection string, the props cache.

`routes/OrdersReportRoute.cs` is a read-only C# report on its own timer (`report.period`):
`ProcessWithRedb("local", ...)` gives the `IRedbService` of the same named instance, and the route runs a typed
`redb.Query<OrderProps>()` LINQ query. Use it for what the `where=` text of `redbQuery` cannot say.
