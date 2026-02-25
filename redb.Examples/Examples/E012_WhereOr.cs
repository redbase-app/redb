using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees using OR condition.
/// Find juniors (under 30) OR high earners (salary over 90k).
/// </summary>
[ExampleMeta("E012", "Where - OR Condition", "Query",
    ExampleTier.Free, 1, "Where", "Query", "OR")]
public class E012_WhereOr : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Age < 30 || e.Salary > 90000m)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E012", "Where - OR Condition", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: Age < 30 OR Salary > 90000"]);
    }
}
