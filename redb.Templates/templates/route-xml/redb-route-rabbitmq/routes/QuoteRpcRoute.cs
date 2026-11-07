using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using redb.Route.Core;

namespace RouteModule.Routes;

/// <summary>
/// The request-reply half of the scenario in C#, next to the publish, consume and retry routes of
/// routes/orders.route.xml: a client asks for a price quote on the queue cs-quotes and waits for the reply, a
/// server answers it. Its own timer and its own queue, the same rabbit connection bean.
/// </summary>
public sealed class QuoteRpcRoute : RouteBuilder
{
    protected override void Configure()
    {
        // Client: every {{rpc.period:7000}} ms a quote request. replyTo=true makes the send a request-reply: the
        // connector publishes with a reply queue of its own and a correlation id, waits up to {{rpc.timeout:5}}
        // seconds and puts the reply into the exchange, so the next step reads the reply as the body.
        From("timer://cs-modulectx-quotes?period={{rpc.period:7000}}")
            .RouteId("cs-modulectx-quote-client")
            .Description("Asks for a price quote and waits for the reply")
            // No reply in time: the server failed or is not running. Logged, the next tick asks again.
            .OnException<TimeoutException>()
                .Handled()
                .Log("no quote within the timeout: ${exception.message}", LogLevel.Warning)
            .EndOnException()
            .SetBody(Expr("""{"id":"quote-${header.CamelTimerCounter}","amount":${header.CamelTimerCounter}}"""))
            .To("rabbitmq://cs-quotes?connectionFactory=rabbit&declare=true&replyTo=true&timeout={{rpc.timeout:5}}")
            .ConvertBody(typeof(string))
            .Log("quote received: ${body}");

        // Server: a request on cs-quotes carries a reply address, so the connector sends the body this route ends
        // with back to the client. No retries and no handled error here: a failed request is not delivered again,
        // the client sees its timeout instead.
        From("rabbitmq://cs-quotes?connectionFactory=rabbit&declare=true")
            .RouteId("cs-modulectx-quote-server")
            .Description("Answers a price quote request")
            .ConvertBody(typeof(string))
            .Log("quote requested: ${body}")
            // The reply is the last body of the route.
            .Process(exchange => exchange.In.Body = Quote((string)exchange.In.Body!));
    }

    // The quote of a request {"id":..,"amount":..}: the amount with 20 % VAT.
    private static string Quote(string request)
    {
        using var json = JsonDocument.Parse(request);
        var id = json.RootElement.GetProperty("id").GetString();
        var amount = json.RootElement.GetProperty("amount").GetDecimal();
        var gross = Math.Round(amount * 1.20m, 2).ToString(CultureInfo.InvariantCulture);
        return $$"""{"id":"{{id}}","gross":{{gross}}}""";
    }
}
