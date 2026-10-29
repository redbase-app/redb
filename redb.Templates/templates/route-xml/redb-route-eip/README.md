# RouteModule

A Tsak module that shows the common integration patterns in XML markup (`urn:redb:route:1.0`). It needs
no broker and no database: test orders come from a timer.

| Pattern | Where | What you see in the log |
|---|---|---|
| In-memory queue (`seda://`) | `modulectx-source` to `modulectx-intake` | the producer goes on at once, two consumers read the queue |
| Idempotent consumer | `modulectx-intake` | an order id that came before is dropped (every fourth tick repeats one) |
| Throttle | `modulectx-intake` | at most 5 orders a second pass |
| Wire tap | `modulectx-intake` to `modulectx-audit` | a copy of every order goes to the audit, the main flow does not wait for it |
| Aggregator | `modulectx-audit` | `audit book: orders 0, 2, 4`: three ids per order type in one line |
| Content-based router (`choice`) | `modulectx-intake` | books and music go to their own routes |
| Splitter | `modulectx-books` | one line per order line: `book order 0: 2 x B-2` |
| Dead letter channel | `context.xml`, `modulectx-dead` | an order of type `toy` has no route and ends here with the reason |

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `context.xml` | the repository of seen order ids and the dead letter channel of the context; the C# route builder |
| `routes/orders.route.xml` | the routes |
| `routes/OrdersRoute.cs` | the same routes in C# |
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
from `redb.Tsak.Templates`, or the `redb-tsak-worker` image). The log shows the lines of the table above.

## The same route in C#

`routes/OrdersRoute.cs` is `routes/orders.route.xml` written as a `RouteBuilder`: the same patterns in the same
order, on its own timer, seda queue and `direct:` names (`cs-modulectx-...`). `context.xml` adds it with
`<routeBuilder type="RouteModule.Routes.OrdersRoute, RouteModule"/>`. Both chains run with the module: every
line of the table above also comes from the `cs-` routes, with order ids `cs-order-<n>`.

The difference to notice: the router branches and the generated headers are C# lambdas
(`.When(exchange => ...)`, `.SetHeader("orderType", exchange => ...)`) instead of expression text. What the two
forms share is declared once in `context.xml`: the `seen-orders` repository (the C# keys get the prefix `cs-` so
they never count as duplicates of the XML ones) and the dead letter channel, which catches a failure of either
form.

## Change it

- **Real input.** Replace the timer route with a connector, for example `rabbitmq://`, `kafka://`,
  `file://` or an HTTP endpoint; the intake stays as it is.
- **Idempotency across restarts and nodes.** `seen-orders` keeps the ids in the memory of one worker.
  On a cluster use a shared repository: `SqlIdempotentRepository` of `redb.Route.Sql`, or the redb-backed
  one of `redb.Route.Core`.
- **Aggregation.** `strategy` takes `concat:<separator>`, `groupedBody`, `useLatest`, `sum:<expression>`,
  `max:`, `min:`, `intoHeader:<name>`; `completionSize`, `completionTimeout` and `completion` (an
  expression) end a group.
- **Throttle per key.** `key="${header.customer}"` on `<throttle>` gives every key its own limit.
- **Order period.** `orders.period` in the context configuration, 2000 ms by default.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
