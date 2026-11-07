using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates filtering by array element using Contains().
/// Server-side: = ANY(pvt."Skills") in PostgreSQL, STRING_SPLIT in MSSQL.
/// Pro feature: array operations in PVT.
/// </summary>
[ExampleMeta("E168", "Array - Contains", "Query",
    ExampleTier.Free, 168, "Array", "Contains", "Filter", RelatedApis = ["IRedbQueryable.Where", "Array.Contains"])]
public class E168_ArrayContains : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find employees who have "C#" skill
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Skills != null && e.Skills.Contains("C#"))
            .Take(100);

        // Uncomment to see generated SQL (contains ANY or STRING_SPLIT):
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => e.Skills != null && e.Skills.Contains("C#"))
            .CountAsync();

        sw.Stop();

        var skillSamples = results.Take(3)
            .Select(e => $"{e.Props.FirstName}: [{string.Join(", ", e.Props.Skills?.Take(3) ?? [])}]");

        return Ok("E168", "Array - Contains", ExampleTier.Free, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: Skills.Contains(\"C#\")",
             $"Found: {totalCount} employees",
             $"Samples: {string.Join("; ", skillSamples)}"]);
    }
}
