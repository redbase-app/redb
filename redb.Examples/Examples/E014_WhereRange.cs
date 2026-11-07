using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees by salary range.
/// Find mid-level earners between 60k and 90k.
/// </summary>
[ExampleMeta("E014", "Where - Range Condition", "Query",
    ExampleTier.Free, 1, "Where", "Query", "Range")]
public class E014_WhereRange : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Salary >= 60000m && e.Salary < 90000m)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E014", "Where - Range Condition", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: 60k <= Salary < 90k"]);
    }
}
