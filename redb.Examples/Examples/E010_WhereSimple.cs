using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Example: Simple Where filter by salary.
/// </summary>
[ExampleMeta("E010", "Where - Filter by Salary", "Query",
    ExampleTier.Free, 1, "Where", "Query", "Filter",
    RelatedApis = ["IRedbQueryable.Where", "IRedbService.Query"])]
public class E010_WhereSimple : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.Salary > 75000m)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E010", "Where - Filter by Salary", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: Salary > 75000"]);
    }
}
