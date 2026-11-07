# RouteModule

A Tsak module that shows scatter-gather in XML markup (`urn:redb:route:1.0`): one request goes to several
recipients at once, the answers that arrive in time are folded into one result. It needs no broker.

| Route | What it does |
|---|---|
| `modulectx-quote` | every 3 seconds asks three suppliers for a price at once, waits at most one second and logs the lowest price |
| `modulectx-supplier-a` | answers at once |
| `modulectx-supplier-b` | answers after 300 ms, sometimes cheaper than A |
| `modulectx-supplier-c` | the cheapest, but needs 2 seconds: the request gives up on it and goes on with the others |
| `cs-modulectx-quote` | C#: every 5 seconds asks the same three suppliers for 50 pieces and logs the whole best answer |

The log shows `best price for item-1: 95` and `Scatter-gather: endpoint direct://modulectx-supplier-c timed out`.

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `routes/quotes.route.xml` | the request and the three suppliers |
| `routes/QuoteRoute.cs` | a second request in C#, to the same suppliers |
| `context.xml` | the C# route builder; room for beans and the one-time init pipeline |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, the endpoint options, bean references and the configuration keys, then
writes `pkg/routemodule-1.0.0.tpkg`.

## Run

Drop `pkg/routemodule-<version>.tpkg` into the `modules/` folder of a Tsak worker (`dotnet new tsak-worker`
from `redb.Tsak.Templates`, or the `redb-tsak-worker` image).

## The same request in C#

`routes/QuoteRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.QuoteRoute, RouteModule"/>`. On a timer of its own (`bulk.period`,
5 seconds) it sends a request for 50 pieces to the three supplier routes of the XML file, with the same
one-second timeout, and logs `best price for item-1 x 50: {"supplier":"B","price":95}`. The C# route only
sends to the suppliers; the XML routes stay their only consumers.

The difference to notice: XML names a strategy (`min:${jpath('$.price')}`) and keeps the price alone; C#
passes a function, `(best, next) => Price(next) < Price(best) ? next : best`, which keeps the whole answer of
the winning supplier.

## Change it

- **Real recipients.** A `<recipient uri="...">` is any endpoint that answers: `https://` with
  `redb.Route.Http`, a request-reply over `rabbitmq://`, a `sql:` query, a `direct://` route of the module.
- **The result.** `strategy` decides it: `min:<expression>` and `max:<expression>` keep a number,
  `groupedBody` keeps every answer as a list, `useLatest` the last one, `concat:<separator>` joins them.
- **Waiting.** `timeout` bounds the whole request; without it the request waits for every recipient.
  `stopOnException="true"` gives up at the first failing recipient instead of ignoring it.
- **Order.** `parallel="false"` asks the recipients one after the other.
- **Request period.** `quotes.period` in the context configuration, 3000 ms by default.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
