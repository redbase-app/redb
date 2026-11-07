# RouteModule

A Tsak module that works with an MQTT broker, written in XML markup (`urn:redb:route:1.0`) and in C#:

| Route | What it does |
|---|---|
| `modulectx-sensors-out` | test sensors: every 5 seconds a temperature reading of two rooms on `sensors/<room>/temperature`, QoS 1 |
| `modulectx-status` | the module's status every minute on `modules/modulectx/status`, retained |
| `modulectx-sensors-in` | subscribes to `sensors/+/temperature`, logs each reading, publishes the ones over 30 degrees to `alerts/temperature` |
| `modulectx-alerts` | logs the alerts |
| `cs-modulectx-humidity-out` | C#: every 7 seconds a humidity reading of two rooms on `sensors/<room>/humidity`, QoS 1 |
| `cs-modulectx-humidity-in` | C#: subscribes to `sensors/+/humidity` and logs the moving average of each room |

The module is a package (`.tpkg`) that a Tsak worker loads from its `modules/` folder; there is no `Main`
to run.

## Layout

| Path | What |
|---|---|
| `context.xml` | the `mqtt` connection: host, port, user, password; the C# route builder |
| `routes/sensors.route.xml` | the four routes |
| `routes/HumidityRoute.cs` | the humidity routes in C# |
| `config/routemodule.config.json` | identity inside the package: context name and auto start |
| `config/context.sample.json` | deployment sample of the context configuration |
| `deploy/docker-compose.mqtt.yml` | a local Mosquitto, and the module on a Tsak worker |
| `.config/dotnet-tools.json` | the `redb-route-xml` tool, same version as the redb packages |

## Build the package

    dotnet tool restore
    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0

The build checks the markup, every `mqtt://` option against the connector, the `mqtt` bean and the configuration
keys, then writes `pkg/routemodule-1.0.0.tpkg`. No broker is needed for the build.

## Try it with Mosquitto

    dotnet build -p:PackRouteOnBuild=true -p:Version=1.0.0
    cp deploy/.env.example deploy/.env          # then set MQTT_PASSWORD
    docker compose -f deploy/docker-compose.mqtt.yml up -d

The module log shows `sensors/room-1/temperature: 24 C` and the like every 5 seconds, and `alert: ...` for the
readings over 30.

The module needs the broker up when it starts: a subscription that cannot reach it fails its route and the
context. The compose file therefore starts the module only when Mosquitto takes a login (`healthcheck` and
`condition: service_healthy`); do the same in a deployment.

## Configuration

| Key | Default | What |
|---|---|---|
| `mqtt.host`, `mqtt.port` | `localhost`, `1883` | the broker; 8883 with `UseTls` on the bean |
| `mqtt.username` | none, required | the user |
| `mqtt.password` | none, required | its password; from the environment |
| `sensors.period` | `5000` | the test sensors' interval, ms |
| `humidity.period` | `7000` | the C# humidity sensors' interval, ms |

In a deployment the password goes into the environment of the worker, in the `Override` section:

    Tsak__Contexts__modulectx__Override__mqtt.password: ${MQTT_PASSWORD}

## Humidity in C#

`routes/HumidityRoute.cs` is a `RouteBuilder` that `context.xml` adds with
`<routeBuilder type="RouteModule.Routes.HumidityRoute, RouteModule"/>`. It publishes and subscribes on topics of
its own, `sensors/<room>/humidity`, through the same `mqtt` bean; the XML subscriber takes
`sensors/+/temperature` only. The log shows `sensors/room-1/humidity: average 40.5 % over the last readings`.

The difference to notice: the moving average is C# state of the route builder (the last five readings per
room), which markup has no place for; the received topic is read with the constant `MqttHeaders.Topic`.

## Change it

- **Topics.** The topic is the URI path; `+` matches one level, `#` everything below.
- **Delivery.** `qos=0` (at most once), `1` (at least once), `2` (exactly once).
- **Several workers.** `sharedSubscription=<group>` shares one subscription, each message goes to one worker.
- **MQTT 5.** `messageExpiryInterval`, `responseTopic` and `contentType` on the publisher.
- **TLS.** `UseTls` on the `mqtt` bean.

## Editor

The `redb Route XML` extension for VS Code (`redb.redb-route-xml`) binds the schema by namespace and
completes elements and endpoint options. Without the extension, generate the schema from the build output
and point the XML extension at it:

    dotnet redb-route-xml xsd bin/Debug/net10.0 --out schema

Documentation: [redbase.app](https://redbase.app)
