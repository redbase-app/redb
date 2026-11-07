using System.Globalization;
using redb.Route.Core;
using redb.Route.RedbCore.Extensions;
using RouteModule.Props;

namespace RouteModule.Routes;

/// <summary>
/// A sibling of routes/orders.route.xml in C#: a report on the order objects, on a timer of its own, through the
/// same named redb instance local. It only reads, with a typed LINQ query on the props class.
/// </summary>
public sealed class OrdersReportRoute : RouteBuilder
{
    protected override void Configure()
    {
        // Every {{report.period:15000}} ms the new orders of ACME, their number and their total amount.
        From("timer://cs-modulectx-orders-report?period={{report.period:15000}}&delay=7000")
            .RouteId("cs-modulectx-orders-report")
            .Description("Reports the open orders with a typed query")
            .ProcessWithRedb("local", async (redb, exchange, ct) =>
            {
                var open = await redb.Query<OrderProps>()
                    .Where(o => o.Customer == "ACME" && o.Status == "new")
                    .ToListAsync(ct);
                // The scale of a decimal depends on the database; 0.## prints 30, not 30.000000000000000000.
                var total = open.Sum(o => o.Props.Amount).ToString("0.##", CultureInfo.InvariantCulture);
                exchange.In.Body = $"open orders of ACME: {open.Count}, total {total}";
            })
            .Log("${body}");
    }
}
