Kafka (`redb.Route.Kafka`), `routes/orders.route.xml`: a producer to the topic `orders` keyed by the header
`redbKafka.Key`, a consumer of `orders` in the group `{{kafka.group:modulectx}}` and a consumer of
`orders-failed`. The connection is the bean `kafka` (`KafkaConnectionFactory`) in `context.xml`; every URI
says `connectionFactory=kafka`. `onException` retries a failed record twice and then sends the original to
`orders-failed`; the offset is committed after the route.

Keys: `kafka.brokers` required, `kafka.group` with a default. A record arrives as bytes: keep
`<convertBody type="System.String"/>` before JSON expressions. Headers of a consumed record:
`redbKafka.Topic`, `redbKafka.Partition`, `redbKafka.Offset`, `redbKafka.Key`.
`deploy/docker-compose.kafka.yml` starts a local single-node Kafka.

Usually changed: topic names, the group, security properties of the `kafka` bean, the retry count.

`routes/PaymentsRoute.cs` adds payments in C# on their own topic `cs-payments` (key = customer) and consumer group
`cs-modulectx`: the consumer keeps a running total per customer in the builder's dictionary. Never point a C#
consumer at the XML topics.
