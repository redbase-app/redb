Integration patterns without a broker, `routes/orders.route.xml`: a timer feeds `seda://modulectx-orders`;
the intake route applies `<idempotentConsumer>` (bean `seen-orders` in `context.xml`, in memory),
`<throttle>`, `<wireTap>` into an `<aggregate>` per order type, and `<choice>` by order type; the book
route splits the order lines with `<split expr="${jpath('$.lines')}">`. A type no branch handles throws
and ends in the context's `<deadLetterChannel>` (`direct://modulectx-dead`).

Usually changed: the timer becomes a real input; `seen-orders` becomes a shared repository on a cluster;
the aggregation strategy and completion.

`routes/OrdersRoute.cs` is the same chain in C# on its own timer, seda queue and `direct:` names (`cs-modulectx-...`),
with C# lambdas for the router and the generated headers. It shares the `seen-orders` repository (keys prefixed
`cs-`) and the dead letter channel of `context.xml` with the XML routes.
