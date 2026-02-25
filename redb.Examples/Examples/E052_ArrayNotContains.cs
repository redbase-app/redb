using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query employees excluding specific skill.
/// Find developers who don't have "intern" skill.
/// </summary>
[ExampleMeta("E052", "Array NOT Contains", "Query",
    ExampleTier.Free, 2, "Array", "Contains", "NOT", "Pro")]
public class E052_ArrayNotContains : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Pro: NOT Contains - exclude employees with specific skill
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Skills!.Contains("C#") && !e.Skills!.Contains("intern"))
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E052", "Array NOT Contains", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: Skills has 'C#' AND NOT 'intern'"]);
    }
}
