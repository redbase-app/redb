using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter employees by deeply nested property.
/// Find employees on high floors in their home building.
/// </summary>
[ExampleMeta("E041", "Where - Deep Nested Property", "Query",
    ExampleTier.Pro, 2, "Where", "Nested", "Building")]
public class E041_DeepNested : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>()
            .Where(e => e.HomeAddress!.Building!.Floor > 10)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var result = await query.ToListAsync();
        sw.Stop();

        return Ok("E041", "Where - Deep Nested Property", ExampleTier.Pro, sw.ElapsedMilliseconds, result.Count,
            [$"Filter: HomeAddress.Building.Floor > 10"]);
    }
}
