XML documents without a broker, `routes/orders.route.xml`: a timer builds an order document,
`<validateXsd file="resources/order.xsd" throwOnFailure="true"/>` checks it and
`<xslt file="resources/order-to-invoice.xslt"/>` turns it into an invoice. A document the schema refuses
throws `redb.Route.Validation.ValidationException`; `onException` (handled) passes it with the reason in the
header `rejectReason` to `direct://modulectx-rejected`.

Facts to keep: stylesheets and schemas are files under `resources/` and are named by that path; the
built-in engine is XSLT 1.0 (`XslCompiledTransform`); `validateJsonSchema` is the same step for JSON.

Usually changed: the timer becomes `file://`, `sftp://`, an HTTP endpoint or a queue; the schema and the
stylesheet; what happens to a rejected document (an error folder, a reply to the sender).

`routes/OrdersRoute.cs` runs the same check and transformation in C# on its own timer (`cs.orders.period`) and its
own `direct:` rejected route: the document comes from LINQ to XML, the schema and stylesheet are the same files
of `resources/` as `EmbeddedResource` (`order.xsd`, `order-to-invoice.xslt`), passed to `ValidateXsd` and
`XsltContent`. Edit the files once; both forms read them.
