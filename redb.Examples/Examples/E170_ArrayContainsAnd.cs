using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates filtering by multiple array elements using Contains with AND.
/// Finds employees who have ALL of the specified skills.
/// Pro feature: multiple array operations with AND logic.
/// </summary>
[ExampleMeta("E170", "Array - Contains AND", "Query",
    ExampleTier.Free, 170, "Array", "Contains", "AND", RelatedApis = ["IRedbQueryable.Where", "Array.Contains"])]
public class E170_ArrayContainsAnd : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find employees who have BOTH C# AND SQL skills (full-stack)
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Skills != null && 
                e.Skills.Contains("C#") && e.Skills.Contains("SQL"))
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => e.Skills != null && 
                e.Skills.Contains("C#") && e.Skills.Contains("SQL"))
            .CountAsync();

        sw.Stop();

        var skillSamples = results.Take(3)
            .Select(e => $"{e.Props.FirstName}: [{string.Join(", ", e.Props.Skills ?? [])}]");

        return Ok("E170", "Array - Contains AND", ExampleTier.Free, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: Skills.Contains(\"C#\") AND Skills.Contains(\"SQL\")",
             $"Found: {totalCount} full-stack employees",
             $"Samples: {string.Join("; ", skillSamples)}"]);
    }
}
