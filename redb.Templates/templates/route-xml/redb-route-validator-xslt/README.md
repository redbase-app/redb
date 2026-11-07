# RouteModule

A Tsak module that checks XML documents against a schema and transforms them with XSLT, written in XML
markup (`urn:redb:route:1.0`). It needs no broker.

| Route | What it does |
|---|---|
| `modulectx-orders` | every 3 seconds builds an order document, checks it against `resources/order.xsd`, turns it into an invoice with `resources/order-to-invoice.xslt` and logs it; every fourth order has a line with quantity 0 |
| `modulectx-rejected` | logs the documents the schema refused, with the reason |
| `cs-modulectx-orders`, `cs-modulectx-rejected` | C#: the same two routes on a timer of their own (every 5 seconds), order ids `cs-order-<n>` |

The log shows `invoice: <invoice order="order-0" customer="ACME" total="39">...` and, for every fourth
order, `order rejected: Validation failed: ... The 'qty' attribute is invalid`.

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `routes/orders.route.xml` | the check, the transformation and the error handling |
| `routes/OrdersRoute.cs` | the same routes in C# |
| `resources/order.xsd` | the schema of an order, for both forms |
| `resources/order-to-invoice.xslt` | the stylesheet: positions with their amounts and the total, for both forms |
| `context.xml` | the C# route builder; room for beans and the one-time init pipeline |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup and that every file a route names is under `resources/`, then writes
`pkg/routemodule-1.0.0.tpkg` with the `resources/` folder inside.

## Run

Drop `pkg/routemodule-<version>.tpkg` into the `modules/` folder of a Tsak worker (`dotnet new tsak-worker`
from `redb.Tsak.Templates`, or the `redb-tsak-worker` image).

## The same route in C#

`routes/OrdersRoute.cs` is `routes/orders.route.xml` written as a `RouteBuilder`, on a timer of its own
(`cs.orders.period`, 5 seconds) and a rejected route of its own. `context.xml` adds it with
`<routeBuilder type="RouteModule.Routes.OrdersRoute, RouteModule"/>`. Both forms run with the module and log the
same lines, the C# ones for `cs-order-<n>`.

The difference to notice: XML writes the document as text and names the files of `resources/`; C# builds the
document with LINQ to XML (`new XElement("order", ...)`), and gets the same two files compiled into the
assembly (`EmbeddedResource` in `RouteModule.csproj`), passed as text to `ValidateXsd` and `XsltContent`.

## Change it

- **Real input.** Replace the timer with `file://inbox`, `sftp://`, an HTTP endpoint or a queue; the
  check and the transformation stay.
- **The schema.** `validateXsd` takes `file` and, for a namespaced schema, `targetNamespace`.
  `throwOnFailure="false"` lets the document through and only reports.
- **The stylesheet.** The engine is XSLT 1.0; `output` picks what the body becomes (text, bytes or a
  document).
- **JSON.** `<validateJsonSchema file="resources/x.schema.json"/>` is the same step for JSON bodies.
- **Rejected documents.** `modulectx-rejected` only logs; write them to an error folder or answer the
  sender instead.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
