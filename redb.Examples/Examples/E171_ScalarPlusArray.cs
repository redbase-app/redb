using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates combining scalar field filter with array Contains filter.
/// Finds senior developers (Age > 30) who have specific skills.
/// Pro feature: mixed scalar and array expressions in single query.
/// </summary>
[ExampleMeta("E171", "Scalar + Array Combined", "Query",
    ExampleTier.Free, 171, "Array", "Scalar", "Combined", RelatedApis = ["IRedbQueryable.Where", "Array.Contains"])]
public class E171_ScalarPlusArray : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find senior developers: Age > 30 AND has C# skill
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Age > 30 && e.Skills != null && e.Skills.Contains("C#"))
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => e.Age > 30 && e.Skills != null && e.Skills.Contains("C#"))
            .CountAsync();

        sw.Stop();

        var avgAge = results.Any() ? results.Average(e => e.Props.Age) : 0;
        var sample = results.FirstOrDefault();

        return Ok("E171", "Scalar + Array Combined", ExampleTier.Free, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: Age > 30 AND Skills.Contains(\"C#\")",
             $"Found: {totalCount} senior C# developers",
             $"Avg age: {avgAge:F1}, Sample: {sample?.Props.FirstName} ({sample?.Props.Age})"]);
    }
}
