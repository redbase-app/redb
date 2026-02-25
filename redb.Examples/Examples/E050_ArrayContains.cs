using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query employees by skill using Array.Contains.
/// Find all C# developers.
/// </summary>
[ExampleMeta("E050", "Array Contains - Find by Skill", "Query",
    ExampleTier.Free, 2, "Array", "Contains", "Skills", "Pro")]
public class E050_ArrayContains : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Pro: Query array property with Contains
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Skills!.Contains("C#"))
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E050", "Array Contains - Find by Skill", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: Skills.Contains('C#')"]);
    }
}
