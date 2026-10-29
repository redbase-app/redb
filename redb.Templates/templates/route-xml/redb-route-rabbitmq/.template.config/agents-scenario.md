RabbitMQ (`redb.Route.RabbitMQ`), `routes/orders.route.xml`: a producer to the queue `orders`, a consumer
of `orders` and a consumer of `orders-failed`. The connection is the bean `rabbit`
(`RabbitMQConnectionFactory`) in `context.xml`; every URI says `connectionFactory=rabbit&amp;declare=true`.
`onException` retries a failed message twice and then sends the original to `orders-failed`.

Keys: `rabbit.host`, `rabbit.port`, `rabbit.vhost`, `rabbit.username` with defaults, `rabbit.password`
required. A message body arrives as bytes: keep `<convertBody type="System.String"/>` before JSON
expressions. `deploy/docker-compose.rabbitmq.yml` starts a local broker.

Usually changed: queue names, an exchange (`rabbitmq:?exchange=...&amp;exchangeType=topic&amp;routingKey=...`),
`prefetchCount`, the retry count.

`routes/QuoteRpcRoute.cs` adds request-reply in C# on its own queue `cs-quotes` and timer: the client sends with
`replyTo=true&timeout={{rpc.timeout:5}}` and reads the reply as the body of the next step; the server route ends
with the reply as its body. Keep the server free of retries and of a handled `OnException`: a handled error would
send an empty or stale reply, an unhandled one shows as the client's timeout.
