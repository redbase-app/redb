using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Calculates average of a numeric field using AverageAsync.
/// Server-side aggregation without loading objects.
/// </summary>
[ExampleMeta("E125", "AverageAsync - Mean", "Analytics",
    ExampleTier.Free, 125, "Average", "Avg", "Aggregation", "Analytics", RelatedApis = ["IRedbQueryable.AverageAsync"])]
public class E125_AverageAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>();

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var avgAge = await query.AverageAsync(e => e.Age);

        sw.Stop();

        return Ok("E125", "AverageAsync - Mean", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"AVG(Age) = {avgAge:F2}", "Server-side aggregation"]);
    }
}
