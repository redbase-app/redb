using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Example: Where with AND - filter by age and salary.
/// </summary>
[ExampleMeta("E011", "Where - AND Condition", "Query",
    ExampleTier.Free, 2, "Where", "Query", "AND")]
public class E011_WhereAnd : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Age >= 30 && e.Salary > 70000m)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E011", "Where - AND Condition", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: Age >= 30 AND Salary > 70000"]);
    }
}
