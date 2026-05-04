using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates case-insensitive string search using ToLower() in Where clause.
/// Finds employees whose last name contains "s" regardless of case (Smith, Jones, etc.).
/// Pro feature: server-side LOWER() function.
/// </summary>
[ExampleMeta("E153", "Where - String ToLower", "Query",
    ExampleTier.Pro, 153, "String", "ToLower", "CaseInsensitive", RelatedApis = ["IRedbQueryable.Where", "String.ToLower"])]
public class E153_StringToLower : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Case-insensitive search: LastName contains "s" (Smith, Jones, Anderson, etc.)
        var query = redb.Query<EmployeeProps>()
            .Where(e => e.LastName.ToLower().Contains("s"))
            .Take(100);

        // Uncomment to see generated SQL (contains LOWER):
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();
        var totalCount = await redb.Query<EmployeeProps>()
            .Where(e => e.LastName.ToLower().Contains("s"))
            .CountAsync();

        sw.Stop();

        var names = results.Take(3).Select(e => e.Props.LastName);

        return Ok("E153", "Where - String ToLower", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            [$"Filter: LastName.ToLower().Contains(\"s\")",
             $"Found: {totalCount} employees",
             $"Examples: {string.Join(", ", names)}"]);
    }
}
