using redb.Route.Core;

namespace RouteModule.Routes;

/// <summary>
/// A sibling of routes/outbox.route.xml in C#: a report of orders_outbox on a timer of its own, through the same
/// main-db bean. It only reads the table, so it never takes a row from the XML reader.
/// </summary>
public sealed class OutboxReportRoute : RouteBuilder
{
    protected override void Configure()
    {
        // Every {{report.period:30000}} ms the number of rows per status. The query is the same on SQLite,
        // PostgreSQL and SQL Server; the rows arrive as a list of column dictionaries.
        From("timer://cs-modulectx-report?period={{report.period:30000}}")
            .RouteId("cs-modulectx-outbox-report")
            .Description("Logs how many outbox rows are new, done and failed")
            .To("sql:SELECT status, COUNT(*) AS n FROM orders_outbox GROUP BY status?dataSource=#main-db&outputType=SelectList")
            .Process(exchange =>
            {
                var counts = new int[3];
                foreach (var row in (IEnumerable<IDictionary<string, object?>>)exchange.In.Body!)
                    counts[Convert.ToInt32(row["status"])] = Convert.ToInt32(row["n"]);
                exchange.In.Body = $"outbox: {counts[0]} new, {counts[1]} done, {counts[2]} failed";
            })
            .Log("${body}");
    }
}
