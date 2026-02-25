using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter using case-insensitive String.Contains.
/// Uses StringComparison.OrdinalIgnoreCase for accent/case agnostic search.
/// </summary>
[ExampleMeta("E032", "Where - Contains IgnoreCase", "Query",
    ExampleTier.Free, 32, "Where", "String", "Contains", "IgnoreCase", RelatedApis = ["IRedbQueryable.Where"])]
public class E032_StringContainsIgnoreCase : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Case-insensitive search: "smith" matches "Smith", "SMITH", etc.
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.LastName.Contains("smith", StringComparison.OrdinalIgnoreCase))
            .Take(100);

        // Uncomment to see generated SQL:
        //var sql = await query.ToSqlStringAsync();
        //Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        var sample = result.FirstOrDefault()?.Props.LastName ?? "N/A";

        return Ok("E032", "Where - Contains IgnoreCase", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: 'smith' (case-insensitive)", $"Found: {sample}"]);
    }
}
