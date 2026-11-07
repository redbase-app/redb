using Microsoft.Extensions.Logging;
using redb.Route.Core;
using redb.Route.Sql;

namespace RouteModule.Routes;

/// <summary>
/// The batch write of routes/orders.route.xml in C#, on a timer of its own: instead of aggregating single orders,
/// the route builds the batch itself as a list of typed records and writes it into the same orders table through
/// the same main-db bean, with the same savepoint per row.
/// </summary>
public sealed class OrdersBatchRoute : RouteBuilder
{
    /// <summary>One order of a batch; :#orderId and :#amount bind from its properties.</summary>
    public sealed record Order(string OrderId, int Amount);

    protected override void Configure()
    {
        // Every {{batch.period:5000}} ms a batch of 20 orders; the seventh one has a negative amount, which the
        // CHECK of the table refuses.
        From("timer://cs-modulectx-batch?period={{batch.period:5000}}")
            .RouteId("cs-modulectx-orders-batch")
            .Description("Writes a batch of typed orders, the C# form")
            .SetBody(exchange =>
            {
                var tick = Convert.ToInt32(exchange.In.Headers["CamelTimerCounter"]);
                return Enumerable.Range(1, 20)
                    .Select(i => new Order($"cs-order-{tick}-{i}", i == 7 ? -i : i))
                    .ToList();
            })
            .To("sql:INSERT INTO orders (order_id, amount) VALUES (:#orderId, :#amount)?dataSource=#main-db&batchSize=100&breakBatchOnError=false")
            .Log("batch written: ${header.redbSql.updateCount} row(s)")
            // The refused rows, one log line each; the header name is the constant SqlHeaders.BatchErrors.
            .Filter(exchange => exchange.In.Headers.TryGetValue(SqlHeaders.BatchErrors, out var errors) && errors != null)
                .Split(Expr("${header.redbSql.batchErrors}"))
                    .Log("row ${body.Index} of the batch refused: ${body.Message}", LogLevel.Warning);
    }
}
