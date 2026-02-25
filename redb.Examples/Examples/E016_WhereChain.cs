using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees using chained Where() calls.
/// Each Where() adds another condition.
/// </summary>
[ExampleMeta("E016", "Where - Chained Filters", "Query",
    ExampleTier.Free, 2, "Where", "Chain")]
public class E016_WhereChain : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Chained Where() calls - equivalent to AND
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Age >= 30)
            .Where(e => e.Salary > 60000m)
            .Where(e => e.Department != "Support")
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E016", "Where - Chained Filters", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: .Where(Age).Where(Salary).Where(Dept)"]);
    }
}
