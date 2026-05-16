using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query employees combining array Contains with other conditions (Pro).
/// Find senior C# developers over 30.
/// </summary>
[ExampleMeta("E053", "Array Contains + Conditions", "Query",
    ExampleTier.Free, 2, "Array", "Contains", "AND", "Pro")]
public class E053_ArrayWithConditions : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Pro: Combine array Contains with other conditions
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Skills!.Contains("C#") && e.Age >= 30 && e.Salary > 70000m)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E053", "Array Contains + Conditions", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: Skills has 'C#' AND Age >= 30 AND Salary > 70k"]);
    }
}
