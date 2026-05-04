using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates Trim() function in Where clause to filter out empty or whitespace-only values.
/// Finds employees with meaningful first names (not empty after trimming).
/// Pro feature: server-side TRIM() function.
/// </summary>
[ExampleMeta("E154", "Where - String Trim", "Query",
    ExampleTier.Pro, 154, "String", "Trim", "Validation", RelatedApis = ["IRedbQueryable.Where", "String.Trim"])]
public class E154_StringTrim : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find employees with non-empty trimmed first names longer than 3 chars
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.FirstName.Trim().Length > 3)
            .Take(100);

        // Uncomment to see generated SQL (contains TRIM):
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => e.FirstName.Trim().Length > 3)
            .CountAsync();

        sw.Stop();

        var names = results.Take(5).Select(e => $"{e.Props.FirstName} ({e.Props.FirstName.Length} chars)");

        return Ok("E154", "Where - String Trim", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: FirstName.Trim().Length > 3",
             $"Found: {totalCount} employees",
             $"Examples: {string.Join(", ", names)}"]);
    }
}
