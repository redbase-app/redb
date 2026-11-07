using Microsoft.Extensions.Logging;
using redb.Route.Abstractions;
using redb.Route.Core;
using RouteModule.Beans;

namespace RouteModule.Routes;

/// <summary>
/// A sibling of routes/orders.route.xml in C#: price quotes on a timer of their own, with the same
/// <c>pricing</c> bean of context.xml. Where the XML calls the bean by name and method text, C# calls it as a
/// typed object.
/// </summary>
public sealed class QuotesRoute : RouteBuilder
{
    protected override void Configure()
    {
        // A quote the bean refuses (quantity 0 throws in Gross): logged, the exchange ends.
        OnException<ArgumentOutOfRangeException>()
            .Handled()
            .Log("quote ${header.quoteId} refused: ${exception.message}", LogLevel.Warning);

        // Every {{quotes.period:10000}} ms a quote; every fourth one has quantity 0.
        From("timer://cs-modulectx-quotes?period={{quotes.period:10000}}")
            .RouteId("cs-modulectx-quotes")
            .Description("Prices a quote with the C# bean, called as a typed object")
            .SetHeader("quoteId", Expr("quote-${header.CamelTimerCounter}"))
            .Process(exchange =>
            {
                var pricing = Pricing(exchange);
                var counter = Convert.ToInt32(exchange.In.Headers["CamelTimerCounter"]);
                var qty = counter % 4 == 3 ? 0 : counter % 4 + 1;
                const decimal price = 8.00m;

                // The same two methods as the XML route: Gross for the amount, Stamp for currency and vat.
                exchange.In.Headers["qty"] = qty;
                exchange.In.Headers["price"] = price;
                exchange.In.Headers["gross"] = pricing.Gross(qty, price);
                pricing.Stamp(exchange);
            })
            .Log("quote ${header.quoteId}: ${header.qty} x ${header.price} + VAT ${header.vat} = ${header.gross} ${header.currency}");
    }

    // The bean context.xml registered as "pricing"; the route context is stamped on every exchange.
    private static PriceCalculator Pricing(IExchange exchange) =>
        exchange.Context?.GetFromRegistry<PriceCalculator>("pricing")
        ?? throw new InvalidOperationException("The bean 'pricing' of context.xml is not in the route context.");
}
