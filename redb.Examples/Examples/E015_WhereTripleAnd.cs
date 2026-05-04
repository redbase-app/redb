using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees using triple AND condition.
/// Find experienced developers with high salary.
/// </summary>
[ExampleMeta("E015", "Where - Triple AND", "Query",
    ExampleTier.Free, 2, "Where", "AND", "Complex")]
public class E015_WhereTripleAnd : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Position != "" && e.Age >= 30 && e.Salary > 70000m)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E015", "Where - Triple AND", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: Position != '' AND Age >= 30 AND Salary > 70k"]);
    }
}
