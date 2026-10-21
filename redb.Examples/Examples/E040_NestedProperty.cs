using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees by nested property.
/// Find employees who live in London.
/// </summary>
[ExampleMeta("E040", "Where - Nested Property", "Query",
    ExampleTier.Free, 2, "Where", "Nested", "Address")]
public class E040_NestedProperty : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.HomeAddress!.City == "London")
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E040", "Where - Nested Property", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: HomeAddress.City == 'London'"]);
    }
}
