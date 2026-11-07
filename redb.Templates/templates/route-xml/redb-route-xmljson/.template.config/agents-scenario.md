XML to JSON and back without a class, no broker. `context.xml` declares
`<dataFormat name="orders-json" type="redb.Route.DataFormats.XmlJson.XmlJsonDataFormat, redb.Route.DataFormats.XmlJson"
schema="resources/order.xsd" namespaces="Drop" prefixes="o=urn:example:orders"/>`. `routes/orders.route.xml` builds an
XML order on a timer, checks it with `validateXsd` (`targetNamespace="urn:example:orders"`), turns it into JSON with
`<marshal format="orders-json"/>` and posts it to the module's own API on port 5181. `routes/api.route.xml` is that
API: `<unmarshal format="orders-json" target="System.String"/>`, `validateXsd` again, 202; `onException` answers 400
for `JsonException`, `InvalidOperationException` (a key the schema has no place for) and `ValidationException`.

Facts to keep: the schema decides arrays (`maxOccurs` above 1, so one `Item` is still an array), JSON numbers and
booleans from the XSD types, and on the way back the namespace and the order of the children; `arrays=` adds element
paths the schema does not describe (`xs:any`); the notation is Json.NET's (`@attr`, `#text`). `marshal` writes bytes:
`convertBody type="System.String"` before a log. JSON to XML does not check required elements, `validateXsd` after it
does. Options go on the named instance, not on the step. When the document has a class, the typed
`unmarshal`/`marshal` with `application/xml` and `application/json` is the better path.

`routes/OrdersRoute.cs` runs the same round trip in C# on its own timer (`cs.orders.period`), id `cs-modulectx-orders`:
`MarshalXmlJson(o => ...)` and `UnmarshalXmlJson<string>(o => ...)` with `SchemaSet` compiled from the embedded
`order.xsd`, without the HTTP hop.

Usually changed: the timer becomes `file://`, `sftp://` or a queue and the POST a partner URL; the schema; `arrays=`
for content under `xs:any`.
