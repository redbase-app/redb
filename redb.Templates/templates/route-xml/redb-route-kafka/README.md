# RouteModule

A Tsak module that works with Kafka, written in XML markup (`urn:redb:route:1.0`) and in C#:

| Route | What it does |
|---|---|
| `modulectx-orders-out` | publishes a test order to the topic `orders` every 5 seconds, keyed by the order id; every fifth one has amount 0 |
| `modulectx-orders-in` | reads `orders` in the consumer group `modulectx` and logs each order with its partition and offset; an order with amount 0 fails |
| `modulectx-orders-failed` | logs what ended up in the topic `orders-failed` |
| `cs-modulectx-payments-out` | C#: publishes a test payment to the topic `cs-payments` every 4 seconds, keyed by customer |
| `cs-modulectx-payments-in` | C#: reads `cs-payments` in the consumer group `cs-modulectx` and logs a running total per customer |

A failed record is tried again twice, one second apart (`onException` in `routes/orders.route.xml`). If it
still fails, the original record goes to `orders-failed` and its offset is committed, so one bad record
does not stop the partition. The offset of every record is committed after the route finished with it:
a record is not lost when the worker stops in the middle.

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `context.xml` | the `kafka` connection: brokers and consumer group from configuration; the C# route builder |
| `routes/orders.route.xml` | the three routes and the error handling |
| `routes/PaymentsRoute.cs` | the payment routes in C# |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `deploy/docker-compose.kafka.yml` | a local single-node Kafka (KRaft) |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, every `kafka://` option against the connector, the `kafka` bean and the
configuration keys, then writes `pkg/routemodule-1.0.0.tpkg`. No broker is needed for the build.

## Run

1. Start a broker: `docker compose -f deploy/docker-compose.kafka.yml up -d` (brokers `localhost:9092`).
2. Give the context its configuration: copy `config/context.sample.json` as `context.json` next to the
   package, or put the keys into the worker's appsettings under `Tsak:Contexts:modulectx`.
   `kafka.brokers` has no default: without it the module stops at init and names the key.
3. Drop `pkg/routemodule-<version>.tpkg` into the `modules/` folder of a Tsak worker (`dotnet new
   tsak-worker` from `redb.Tsak.Templates`, or the `redb-tsak-worker` image).

The log then shows `order processed` every 5 seconds and, for every fifth order, two retries and
`failed order`.

## Configuration

| Key | Default | What |
|---|---|---|
| `kafka.brokers` | none, required | bootstrap servers, `host:port[,host:port]` |
| `kafka.group` | `modulectx` | consumer group of the module |
| `orders.period` | `5000` | producer interval, ms |
| `payments.period` | `4000` | payment producer interval, ms |

## Payments in C#

`routes/PaymentsRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.PaymentsRoute, RouteModule"/>`. It uses the same `kafka` connection and
its own timer, topic and consumer group, so it never reads what the XML routes read. The log shows lines like
`payment of customer-1: total 5 (partition 0, offset 1)`.

The difference to notice: the running total is C# state of the route builder (a dictionary per customer),
which markup has no place for, and the record key header is the constant `KafkaHeaders.Key` instead of the text
`redbKafka.Key`. The totals are those of the partitions this worker reads; with several workers each one sums
its own customers.

## Change it

- **Topics.** `kafka://<topic>?connectionFactory=kafka`. The broker of the compose file creates a topic on
  first use; a production cluster usually wants them created up front.
- **Where a new group starts.** `AutoOffsetReset` on the `kafka` bean, `Earliest` here.
- **Exactly once.** Kafka transactions (`transactionalIdPrefix`) move the output and the consumed offset
  together; the `redb.Route.Kafka` package README shows the setup.
- **Security.** `SecurityProtocol`, `SaslMechanism`, `SaslUsername`, `SaslPassword`, `Ssl*` on the `kafka`
  bean; take the password from a `{{key}}` without a default.
- **Retries.** `maximumRedeliveries` and `redeliveryDelay` on `onException`.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
