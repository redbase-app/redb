using System.Text.Json;
using redb.Route.Abstractions;
using redb.Route.Core;

namespace RouteModule.Routes;

/// <summary>
/// The quote request of routes/quotes.route.xml written in C#, on a timer of its own: the same three suppliers
/// of the XML file, the same timeout, and the best answer picked by a C# function instead of a strategy name.
/// </summary>
public sealed class QuoteRoute : RouteBuilder
{
    protected override void Configure()
    {
        // Every {{bulk.period:5000}} ms a bulk request (50 pieces) goes to the three supplier routes of
        // the XML file at once; what arrives within one second is folded pair by pair into the lowest price.
        From("timer://cs-modulectx-quote?period={{bulk.period:5000}}")
            .RouteId("cs-modulectx-quote")
            .Description("Asks three suppliers for a price and keeps the best one, the C# form")
            .SetHeader("item", Expr("item-${header.CamelTimerCounter % 3}"))
            .SetBody(Expr("""{"item":"${header.item}","qty":50}"""))
            .ScatterGather(scatter => scatter
                .Recipients("direct://modulectx-supplier-a", "direct://modulectx-supplier-b", "direct://modulectx-supplier-c")
                .AggregationStrategy((best, next) => Price(next) < Price(best) ? next : best)
                .Timeout(TimeSpan.FromSeconds(1))
                .ParallelProcessing(true))
            .Log("best price for ${header.item} x 50: ${body}");
    }

    // The price of a supplier answer {"supplier":..,"price":..}.
    private static decimal Price(IExchange answer)
    {
        using var json = JsonDocument.Parse(answer.In.Body?.ToString() ?? "{}");
        return json.RootElement.GetProperty("price").GetDecimal();
    }
}
