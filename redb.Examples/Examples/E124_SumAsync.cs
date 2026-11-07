using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Calculates sum of a numeric field using SumAsync.
/// Server-side aggregation without loading objects, over a field of the employee and over a field
/// two nested classes deep (HomeAddress.Building.Floor).
/// </summary>
[ExampleMeta("E124", "SumAsync - Total", "Analytics",
    ExampleTier.Free, 124, "Sum", "Aggregation", "Analytics", RelatedApis = ["IRedbQueryable.SumAsync"])]
public class E124_SumAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var query = redb.Query<EmployeeProps>();

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var totalSalary = await query.SumAsync(e => e.Salary);
        var totalFloor = await query.SumAsync(e => e.HomeAddress!.Building!.Floor);

        sw.Stop();

        return Ok("E124", "SumAsync - Total", ExampleTier.Free, sw.ElapsedMilliseconds, 2,
            [$"SUM(Salary) = {totalSalary:N0}", $"SUM(HomeAddress.Building.Floor) = {totalFloor:N0}", "Server-side aggregation"]);
    }
}
