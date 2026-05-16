using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// AverageAsync with Where filter applied before aggregation.
/// Filter employees by age range, then calculate average salary.
/// Tests Pro filter handling in single Aggregation.
/// </summary>
[ExampleMeta("E193", "AverageAsync - With Filter", "Analytics",
    ExampleTier.Free, 193, "Average", "Where", "Filter", "Aggregation", RelatedApis = ["IRedbQueryable.AverageAsync", "IRedbQueryable.Where"])]
public class E193_AverageAsyncFiltered : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Average salary for employees aged 30-40
        var avgSalaryMiddle = await redb.Query<EmployeeProps>()
            .Where(x => x.Age >= 30 && x.Age <= 40)
            .AverageAsync(x => x.Salary);

        // Compare with overall average
        var avgSalaryAll = await redb.Query<EmployeeProps>()
            .AverageAsync(x => x.Salary);

        sw.Stop();

        return Ok("E193", "AverageAsync - With Filter", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Age 30-40 AVG: {avgSalaryMiddle:N0}", $"All AVG: {avgSalaryAll:N0}"]);
    }
}
