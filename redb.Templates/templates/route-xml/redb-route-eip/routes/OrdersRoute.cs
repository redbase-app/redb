using Microsoft.Extensions.Logging;
using redb.Route.Abstractions;
using redb.Route.Aggregation;
using redb.Route.Core;

namespace RouteModule.Routes;

/// <summary>
/// The patterns of routes/orders.route.xml written in C#: the same chain of seda queue, idempotent consumer,
/// throttle, wire tap, content-based router, splitter and aggregator, on a timer, a queue and direct: names of its
/// own. Both chains run with the module; a failure of either one ends in the dead letter channel of context.xml.
/// </summary>
public sealed class OrdersRoute : RouteBuilder
{
    protected override void Configure()
    {
        // Test traffic: an order every {{orders.period:2000}} ms. Every fourth tick repeats the previous order id,
        // every seventh order has a type no branch handles.
        From("timer://cs-modulectx-source?period={{orders.period:2000}}")
            .RouteId("cs-modulectx-source")
            .Description("Generates test orders, the C# form")
            .SetHeader("orderId", exchange =>
            {
                var tick = Tick(exchange);
                return tick % 4 == 3 ? tick - 1 : tick;
            })
            .SetHeader("orderType", exchange => Tick(exchange) switch
            {
                var t when t % 7 == 6 => "toy",
                var t when t % 2 == 0 => "book",
                _ => "music",
            })
            .SetBody(Expr("""{"id":"cs-order-${header.orderId}","type":"${header.orderType}","lines":[{"sku":"A-1","qty":1},{"sku":"B-2","qty":2}]}"""))
            .To("seda://cs-modulectx-orders");

        // Intake: drop repeated order ids, at most 5 orders a second, a copy to the audit, a branch by order type.
        // The ids get the prefix cs- in the shared seen-orders repository, so they never clash with the XML ones.
        From("seda://cs-modulectx-orders?concurrentConsumers=2")
            .RouteId("cs-modulectx-intake")
            .Description("Deduplicates, throttles, audits and routes the orders, the C# form")
            .IdempotentConsumer("cs-${header.orderId}", "seen-orders", true)
                .Throttle(5).Period(TimeSpan.FromSeconds(1))
                    .WireTap("direct://cs-modulectx-audit")
                    .Choice()
                        .When(exchange => OrderType(exchange) == "book")
                            .To("direct://cs-modulectx-books")
                        .When(exchange => OrderType(exchange) == "music")
                            .To("direct://cs-modulectx-music")
                        .Otherwise()
                            .ThrowException(typeof(InvalidOperationException), "no route for this order type")
                    .EndChoice()
                .EndThrottle()
            .EndIdempotentConsumer();

        // Splitter: one exchange per order line.
        From("direct://cs-modulectx-books")
            .RouteId("cs-modulectx-books")
            .Description("Handles a book order line by line, the C# form")
            .Split(Expr("${jpath('$.lines')}"))
                .Log("book order ${header.orderId}: ${jpath('$.qty')} x ${jpath('$.sku')}")
            .EndSplit();

        From("direct://cs-modulectx-music")
            .RouteId("cs-modulectx-music")
            .Description("Handles a music order, the C# form")
            .Log("music order ${header.orderId}: ${body}");

        // Aggregator: the audit copies per order type, three at a time or whatever arrived within 10 seconds.
        From("direct://cs-modulectx-audit")
            .RouteId("cs-modulectx-audit")
            .Description("Collects the audit copies per order type, the C# form")
            .SetBody(Expr("${header.orderId}"))
            .Aggregate("${header.orderType}", AggregationStrategies.ByName("concat:, "), null, 3, TimeSpan.FromSeconds(10))
                .ForceCompletionOnStop()
                .Log("audit ${header.orderType}: orders ${body}", LogLevel.Information)
            .EndAggregate();
    }

    private static int Tick(IExchange exchange) => Convert.ToInt32(exchange.In.Headers["CamelTimerCounter"]);

    private static string? OrderType(IExchange exchange) => exchange.In.Headers["orderType"] as string;
}
