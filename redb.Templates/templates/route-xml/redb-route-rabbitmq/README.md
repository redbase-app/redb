# RouteModule

A Tsak module that works with RabbitMQ, written in XML markup (`urn:redb:route:1.0`) and in C#:

| Route | What it does |
|---|---|
| `modulectx-orders-out` | publishes a test order to the queue `orders` every 5 seconds; every fifth one has amount 0 |
| `modulectx-orders-in` | reads `orders` and logs each order; an order with amount 0 fails |
| `modulectx-orders-failed` | logs what ended up in the queue `orders-failed` |
| `cs-modulectx-quote-client` | C#: asks for a price quote on the queue `cs-quotes` every 7 seconds and waits for the reply |
| `cs-modulectx-quote-server` | C#: answers the quote requests of `cs-quotes` |

A failed message is tried again twice, one second apart (`onException` in `routes/orders.route.xml`). If it
still fails, the original message goes to `orders-failed` and the delivery is acknowledged, so one bad
message does not block the queue.

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `context.xml` | the `rabbit` connection: host, port, virtual host, user, password from configuration; the C# route builder |
| `routes/orders.route.xml` | the three routes and the error handling |
| `routes/QuoteRpcRoute.cs` | the request-reply routes in C# |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `deploy/docker-compose.rabbitmq.yml` | a local RabbitMQ with the management UI |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, every `rabbitmq://` option against the connector, the `rabbit` bean and the
configuration keys, then writes `pkg/routemodule-1.0.0.tpkg`. No broker is needed for the build.

## Run

1. Start a broker: `docker compose -f deploy/docker-compose.rabbitmq.yml up -d`
   (management UI on http://localhost:15672, guest / guest).
2. Give the context its configuration: copy `config/context.sample.json` as `context.json` next to the
   package, or put the keys into the worker's appsettings under `Tsak:Contexts:modulectx`.
   `rabbit.password` has no default: without it the module stops at init and names the key.
3. Drop `pkg/routemodule-<version>.tpkg` into the `modules/` folder of a Tsak worker (`dotnet new
   tsak-worker` from `redb.Tsak.Templates`, or the `redb-tsak-worker` image).

The log then shows `order processed` every 5 seconds and, for every fifth order, two retries and
`failed order`.

## Configuration

| Key | Default | What |
|---|---|---|
| `rabbit.host` | `localhost` | broker host |
| `rabbit.port` | `5672` | AMQP port |
| `rabbit.vhost` | `/` | virtual host |
| `rabbit.username` | `guest` | user |
| `rabbit.password` | none, required | password; in production from an environment variable |
| `orders.period` | `5000` | producer interval, ms |
| `rpc.period` | `7000` | quote request interval, ms |
| `rpc.timeout` | `5` | how long the client waits for a reply, s |

The guest user of RabbitMQ works only from localhost; a broker on another machine needs its own user.

## Request-reply in C#

`routes/QuoteRpcRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.QuoteRpcRoute, RouteModule"/>`. It uses the same `rabbit` connection and
its own timer and queue, so it never reads what the XML routes read. The log shows
`quote requested: {"id":"quote-1","amount":1}` from the server and `quote received: {"id":"quote-1","gross":1.20}`
from the client.

The difference to notice: the client sends with `replyTo=true&timeout={{rpc.timeout:5}}`, so the send waits for
the reply (a reply queue of its own and a correlation id) and the step after it reads the reply as the body. The
server needs no option: a request carries its reply address, and the body the route ends with goes back. The
server has no retries and no handled error on purpose; a request that fails is not delivered again, the client
sees its timeout.

## Change it

- **Queues.** `rabbitmq://<queue>?connectionFactory=rabbit&declare=true` declares the queue at start.
  Leave `declare` out when the queues are created by the broker's administrators.
- **Exchanges.** `rabbitmq:?exchange=events&exchangeType=topic&routingKey=order.*&connectionFactory=rabbit&declare=true`
  reads a queue of its own bound to the exchange.
- **Parallelism.** `prefetchCount` limits the unacknowledged messages per consumer;
  `concurrentConsumers` runs several consumers on one queue.
- **TLS.** `Ssl`, `SslServerName`, `SslCaCertPath` on the `rabbit` bean.
- **Retries.** `maximumRedeliveries` and `redeliveryDelay` on `onException`.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
