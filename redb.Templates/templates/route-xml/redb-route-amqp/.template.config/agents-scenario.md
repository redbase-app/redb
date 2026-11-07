AMQP 1.0 (`redb.Route.Amqp`, AMQPNetLite; ActiveMQ Artemis, ActiveMQ Classic, Qpid, Azure Service Bus, Amazon MQ),
`routes/orders.route.xml`: a producer to the queue `orders`, a consumer of `orders` and a consumer of `orders.failed`.
The connection is the bean `amqp` (`AmqpConnectionFactory`: Host, Port, User, Password, Ssl) in `context.xml`; every
URI says `connectionFactory=amqp`. `onException` retries a failed message twice and then sends the original to
`orders.failed`.

Facts to keep: `routingType` `ANYCAST` (a queue, one consumer per message, the default) or `MULTICAST` (a topic,
every subscriber); `credit` is the number of unsettled messages a consumer takes at a time; a message body may arrive
as bytes, keep `<convertBody type="System.String"/>` before JSON expressions; `amqp.password` is a required key from
the environment. `deploy/docker-compose.amqp.yml` runs Artemis.

Usually changed: the addresses, `routingType` for a topic, `filterSelector` for a selective consumer, the retry count,
TLS on the bean.

`routes/QuoteRpcRoute.cs` adds request-reply in C# on its own queue `cs.quotes` and timer: the client sends with
`replyTo=true&timeout={{rpc.timeout:5}}` and reads the reply as the body of the next step; the server route ends
with the reply as its body. Keep the server free of retries and of a handled `OnException`: a handled error would
send an empty or stale reply, an unhandled one shows as the client's timeout.
