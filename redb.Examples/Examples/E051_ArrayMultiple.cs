using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query employees with multiple skills using Array.Contains with OR.
/// Find developers who know C# OR Python.
/// </summary>
[ExampleMeta("E051", "Array Contains - Multiple OR", "Query",
    ExampleTier.Free, 2, "Array", "Contains", "OR", "Pro")]
public class E051_ArrayMultiple : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Pro: Multiple array Contains with OR
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Skills!.Contains("C#") || e.Skills!.Contains("Python"))
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E051", "Array Contains - Multiple OR", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: Skills has 'C#' OR 'Python'"]);
    }
}
