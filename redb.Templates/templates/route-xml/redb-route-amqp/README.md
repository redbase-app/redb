# RouteModule

A Tsak module that works with an AMQP 1.0 broker (ActiveMQ Artemis, ActiveMQ Classic, Qpid, Azure Service Bus,
Amazon MQ and others), written in XML markup (`urn:redb:route:1.0`):

| Route | What it does |
|---|---|
| `modulectx-orders-out` | sends a test order to the queue `orders` every 5 seconds; every fifth one has amount 0 |
| `modulectx-orders-in` | reads `orders` and logs each order; an order with amount 0 fails |
| `modulectx-orders-failed` | logs what ended up in the queue `orders.failed` |
| `cs-modulectx-quote-client` | C#: asks for a price quote on the queue `cs.quotes` every 7 seconds and waits for the reply |
| `cs-modulectx-quote-server` | C#: answers the quote requests of `cs.quotes` |

A failed message is tried again twice, one second apart (`onException` in `routes/orders.route.xml`). If it still
fails, the original message goes to `orders.failed` and the delivery is accepted, so one bad message does not block
the queue.

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `context.xml` | the `amqp` connection: host, port, user, password; the C# route builder |
| `routes/orders.route.xml` | the three routes and the error handling |
| `routes/QuoteRpcRoute.cs` | the request-reply routes in C# |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `deploy/docker-compose.amqp.yml` | a local ActiveMQ Artemis, and the module on a Tsak worker |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, every `amqp://` option against the connector, the `amqp` bean and the configuration
keys, then writes `pkg/routemodule-1.0.0.tpkg`. No broker is needed for the build.

## Try it with Artemis

    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0
    cp deploy/.env.example deploy/.env          # then set AMQP_PASSWORD
    docker compose -f deploy/docker-compose.amqp.yml up -d

The module log shows `order processed` every 5 seconds and, for every fifth order, two retries and `failed order`.
The Artemis console is on http://localhost:8161.

The module needs the broker up when it starts: a consumer that cannot reach it fails its route and the context.
The compose file therefore starts the module only when Artemis answers on 5672 (`healthcheck` and
`condition: service_healthy`); do the same in a deployment.

## Configuration

| Key | Default | What |
|---|---|---|
| `amqp.host`, `amqp.port` | `localhost`, `5672` | the broker |
| `amqp.user` | none, required | the user |
| `amqp.password` | none, required | its password; from the environment |
| `orders.period` | `5000` | the producer's interval, ms |
| `rpc.period` | `7000` | quote request interval, ms |
| `rpc.timeout` | `5` | how long the client waits for a reply, s |

In a deployment the password goes into the environment of the worker, in the `Override` section:

    Tsak__Contexts__modulectx__Override__amqp.password: ${AMQP_PASSWORD}

## Request-reply in C#

`routes/QuoteRpcRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.QuoteRpcRoute, RouteModule"/>`. It uses the same `amqp` connection and its
own timer and queue, so it never reads what the XML routes read. The log shows
`quote requested: {"id":"quote-1","amount":1}` from the server and `quote received: {"id":"quote-1","gross":1.20}`
from the client.

The difference to notice: the client sends with `replyTo=true&timeout={{rpc.timeout:5}}`, so the send waits for
the reply (the producer link gets a temporary reply address of its own, and the request a correlation id; no
reply queue to declare) and the step after it reads the reply as
the body. The server needs no option: a request carries its reply address, and the body the route ends with goes
back. The server has no retries and no handled error on purpose; a request that fails is not delivered again,
the client sees its timeout.

## Change it

- **A topic.** `routingType=MULTICAST` on the producer and the consumers: every subscriber gets each message.
- **Selective consumers.** `filterSelector=` with a JMS-style selector over the message properties.
- **Parallelism.** `credit` (unsettled messages per consumer) and `concurrentConsumers`.
- **TLS.** `Ssl` on the `amqp` bean (port 5671); `SslCertPath` for a client certificate.
- **Several sends at once.** `transacted=true` sends all messages of an exchange in one broker transaction.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
