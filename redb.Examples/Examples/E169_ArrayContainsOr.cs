using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates filtering by multiple array elements using Contains with OR.
/// Finds employees who have any of the specified skills.
/// Pro feature: multiple array operations in single query.
/// </summary>
[ExampleMeta("E169", "Array - Contains OR", "Query",
    ExampleTier.Free, 169, "Array", "Contains", "OR", RelatedApis = ["IRedbQueryable.Where", "Array.Contains"])]
public class E169_ArrayContainsOr : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find employees who have C# OR Python skill
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Skills != null && 
                (e.Skills.Contains("C#") || e.Skills.Contains("Python")))
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => e.Skills != null && 
                (e.Skills.Contains("C#") || e.Skills.Contains("Python")))
            .CountAsync();

        sw.Stop();

        var csharpCount = results.Count(e => e.Props.Skills?.Contains("C#") == true);
        var pythonCount = results.Count(e => e.Props.Skills?.Contains("Python") == true);

        return Ok("E169", "Array - Contains OR", ExampleTier.Free, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: Skills.Contains(\"C#\") OR Skills.Contains(\"Python\")",
             $"Found: {totalCount} employees",
             $"C# devs: {csharpCount}, Python devs: {pythonCount}"]);
    }
}
