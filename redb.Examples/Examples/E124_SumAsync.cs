using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Calculates sum of a numeric field using SumAsync.
/// Server-side aggregation without loading objects.
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

        sw.Stop();

        return Ok("E124", "SumAsync - Total", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"SUM(Salary) = {totalSalary:N0}", "Server-side aggregation"]);
    }
}
