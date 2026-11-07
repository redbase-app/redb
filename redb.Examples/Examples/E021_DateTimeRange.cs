using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees by date range.
/// Find employees hired within specific period.
/// </summary>
[ExampleMeta("E021", "Where - DateTime Range", "Query",
    ExampleTier.Free, 1, "Where", "DateTime", "Range")]
public class E021_DateTimeRange : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var startDate = DateTime.Today.AddYears(-3);
        var endDate = DateTime.Today.AddYears(-1);

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.HireDate >= startDate && e.HireDate < endDate)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E021", "Where - DateTime Range", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: HireDate in [{startDate:yyyy-MM-dd}, {endDate:yyyy-MM-dd})"]);
    }
}
