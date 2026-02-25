using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees by hire date.
/// Find employees hired after a specific date.
/// </summary>
[ExampleMeta("E020", "Where - DateTime Greater", "Query",
    ExampleTier.Free, 1, "Where", "DateTime", "HireDate")]
public class E020_DateTimeGreater : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find employees hired in last 2 years
        var cutoffDate = DateTime.Today.AddYears(-2);

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.HireDate >= cutoffDate)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E020", "Where - DateTime Greater", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: HireDate >= {cutoffDate:yyyy-MM-dd}"]);
    }
}
