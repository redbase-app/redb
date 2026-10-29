using System.Collections.Concurrent;
using System.Text.Json;
using redb.Route.Core;
using redb.Route.MqttNet;

namespace RouteModule.Routes;

/// <summary>
/// A sibling of routes/sensors.route.xml in C#: humidity sensors on topics of their own,
/// sensors/&lt;room&gt;/humidity, and a subscriber that keeps a moving average per room. The same mqtt connection
/// bean; the XML subscriber reads sensors/+/temperature only, so no message is read by both forms.
/// </summary>
public sealed class HumidityRoute : RouteBuilder
{
    // The last five readings of every room.
    private readonly ConcurrentDictionary<string, Queue<int>> _readings = new();

    protected override void Configure()
    {
        // Test sensors: every {{humidity.period:7000}} ms a humidity reading of room-1 and room-2, QoS 1.
        From("timer://cs-modulectx-humidity?period={{humidity.period:7000}}")
            .RouteId("cs-modulectx-humidity-out")
            .Description("Publishes test humidity readings")
            .SetBody(Expr("""{"room":"room-1","percent":${header.CamelTimerCounter % 20 + 40}}"""))
            .To("mqtt://sensors/room-1/humidity?connectionFactory=mqtt&mode=Publish&qos=1&contentType=application/json")
            .SetBody(Expr("""{"room":"room-2","percent":${header.CamelTimerCounter % 9 + 55}}"""))
            .To("mqtt://sensors/room-2/humidity?connectionFactory=mqtt&mode=Publish&qos=1&contentType=application/json");

        // Every room's humidity: the average of its last five readings.
        From("mqtt://sensors/+/humidity?connectionFactory=mqtt&qos=1")
            .RouteId("cs-modulectx-humidity-in")
            .Description("Keeps a moving average of the humidity per room")
            .ConvertBody(typeof(string))
            .Process(exchange =>
            {
                using var json = JsonDocument.Parse((string)exchange.In.Body!);
                var room = json.RootElement.GetProperty("room").GetString()!;
                var last = _readings.GetOrAdd(room, _ => new Queue<int>());
                lock (last)
                {
                    last.Enqueue(json.RootElement.GetProperty("percent").GetInt32());
                    if (last.Count > 5)
                        last.Dequeue();
                    exchange.In.Headers["average"] = Math.Round(last.Average(), 1);
                }
                exchange.In.Headers["topic"] = exchange.In.Headers[MqttHeaders.Topic];
            })
            .Log("${header.topic}: average ${header.average} % over the last readings");
    }
}
