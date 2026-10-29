MQTT (`redb.Route.MqttNet`, MQTTnet, MQTT 3.1.1 and 5), `routes/sensors.route.xml`: test sensors publish to
`sensors/<room>/temperature` (QoS 1), a retained status goes to `modules/modulectx/status`, a subscriber of
`sensors/+/temperature` logs each reading and publishes the ones over 30 degrees to `alerts/temperature`, which a fourth
route logs. The connection is the bean `mqtt` (`MqttConnectionFactory`: Server, Port, Username, Password, UseTls) in
`context.xml`; every URI says `connectionFactory=mqtt`.

Facts to keep: the topic is the URI path; a producer says `mode=Publish` (the default mode is Subscribe); `+` matches
one level, `#` the rest; no `ClientId` on the shared bean (two connections with one id push each other off); the
received topic is the header `redbMqtt.topic`; a message arrives as bytes, keep `<convertBody type="System.String"/>`
before JSON expressions; `mqtt.password` is a required key from the environment.
`deploy/docker-compose.mqtt.yml` runs Mosquitto.

Usually changed: the topics, QoS, `retain`, `sharedSubscription` to share one subscription between workers, TLS.

`routes/HumidityRoute.cs` adds humidity sensors in C# on their own topics `sensors/<room>/humidity` (timer
`humidity.period`) and a subscriber of `sensors/+/humidity` that keeps a moving average per room in the builder.
A C# subscription must not overlap the XML one (`sensors/+/temperature`).
