# RouteModule

A Tsak module that turns XML documents into JSON and back without a class for them, written in XML markup
(`urn:redb:route:1.0`). An XSD decides which elements are arrays, which values are numbers and booleans and, on the
way back, the namespace and the order of every element. It needs no broker.

| Route | What it does |
|---|---|
| `modulectx-orders` | every 3 seconds builds an XML order, checks it against `resources/order.xsd`, turns it into JSON and posts it to the module's own API |
| `POST /api/orders` | port 5181: turns the JSON back into XML, checks it against the same schema, answers 202; a body that is not an order gets 400 with the reason in the log |
| `cs-modulectx-orders` | C#: the same round trip on a timer of its own (every 5 seconds), XML to JSON and straight back, order ids `cs-order-<n>` |

The log shows, for an order of one item:

    json: {"Order":{"@id":"order-0","Customer":"ACME","Express":true,"Items":{"Item":[{"Sku":"A-1","Qty":2,"Price":9.50}]}}}
    xml: <?xml version="1.0" encoding="utf-8"?><o:Order xmlns:o="urn:example:orders" id="order-0"><o:Customer>ACME</o:Customer>...
    api answered: {"accepted":true}

`Item` is an array although the order has one, because the schema says it repeats; `Qty` and `Price` are numbers and
`Express` a boolean, because of their schema types.

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## When a class exists, use it

This module is for documents without a class: a partner format you only pass on, a schema too large to map by hand,
or a gateway that turns any XML into JSON for a JavaScript consumer. When the document has a class, the typed path
validates and names every field: `<unmarshal format="application/xml" target="Acme.Order, Acme"/>`, then
`<marshal format="application/json"/>`.

## Layout

| Path | What |
|---|---|
| `context.xml` | the `orders-json` data format and the C# route builder |
| `routes/orders.route.xml` | the XML order, its check, XML to JSON, the POST |
| `routes/api.route.xml` | the API: JSON to XML, the check, 202 or 400 |
| `routes/OrdersRoute.cs` | the same round trip in C# |
| `resources/order.xsd` | the schema of an order, for both forms |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration: `orders.period`, `cs.orders.period` |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## The data format

`context.xml` declares one named instance and the routes refer to it by name:

```xml
<dataFormat name="orders-json" type="redb.Route.DataFormats.XmlJson.XmlJsonDataFormat, redb.Route.DataFormats.XmlJson"
            schema="resources/order.xsd" namespaces="Drop" prefixes="o=urn:example:orders"/>

<marshal format="orders-json"/>                              <!-- XML to JSON -->
<unmarshal format="orders-json" target="System.String"/>     <!-- JSON to XML, as text -->
```

The JSON is in Json.NET's notation: an attribute is `"@id"`, the text of an element with attributes `"#text"`.
`namespaces="Drop"` writes local names (`"Order"`); without it the names keep their prefixes and the declarations
appear as `"@xmlns:p"` keys. On the way back the schema gives every element its namespace and puts the children in
the order of the sequence, whatever the order of the keys; `prefixes=` names the prefix (without it, `ns0`).

`marshal` writes UTF-8 bytes, so the route converts them to text before the log. After `marshal` the content type is
`application/json`, which the API's `consumes` expects.

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, that `resources/order.xsd` is there and compiles, and that every `arrays=` path names
an element of the schema, then writes `pkg/routemodule-1.0.0.tpkg` with the `resources/` folder inside.

## Run

Drop `pkg/routemodule-<version>.tpkg` into the `modules/` folder of a Tsak worker (`dotnet new tsak-worker`
from `redb.Tsak.Templates`, or the `redb-tsak-worker` image). The worker ships `redb.Route.DataFormats.XmlJson`, so
the package carries only this project's assembly.

Post an order of your own, and one the schema refuses:

    curl -X POST -H "Content-Type: application/json" http://localhost:5181/api/orders \
         -d '{"Order":{"@id":"o-1","Customer":"ACME","Express":false,"Items":{"Item":[{"Sku":"A-1","Qty":1,"Price":5}]}}}'
    curl -X POST -H "Content-Type: application/json" http://localhost:5181/api/orders \
         -d '{"Order":{"@id":"o-2","Customer":"ACME","Discount":5}}'

The second answers 400; the log says `order refused: xmljson: /Order/Discount: the content of /Order in the schema
has no place for 'Discount'`.

## The same round trip in C#

`routes/OrdersRoute.cs` is a `RouteBuilder` next to the XML routes, on a timer of its own (`cs.orders.period`,
5 seconds). `context.xml` adds it with `<routeBuilder type="RouteModule.Routes.OrdersRoute, RouteModule"/>`.

The difference to notice: XML names one shared instance in `context.xml`; C# passes the same options on each step,
`MarshalXmlJson(o => ...)` and `UnmarshalXmlJson<string>(o => ...)`, with the schema compiled once from the file of
`resources/` built into the assembly (`EmbeddedResource` in `RouteModule.csproj`) and handed over as `SchemaSet`.
The C# route skips the HTTP hop and turns the JSON straight back.

## Change it

- **Real input.** Replace the timer with `file://inbox`, `sftp://` or a queue, and the POST with the partner's
  URL; the checks and the conversion stay.
- **Your schema.** Put it under `resources/` and point `schema=` at it. A schema with imports reads them from the
  package only, never from the network: `schemaRoot=` is the folder they may come from.
- **Arrays the schema does not know.** Content under `xs:any` has no schema answer: list the repeating elements,
  `arrays="//Item,//Tag"`. Without a schema, `arrays=` is the only rule besides the document itself.
- **Names.** `namespaces="Keep"` keeps the prefixes in JSON; `textKey` and `attributePrefix` change `#text` and `@`
  (`$` for a WSO2 consumer); `indent="true"` for readable output.
- **Required elements.** JSON to XML places the keys the JSON has; whether the required ones are all there is
  `validateXsd`'s job, which is why the API checks after the step.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
