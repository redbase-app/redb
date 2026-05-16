using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// SumAsync with Where filter applied before aggregation.
/// Filter employees by department, then calculate total salary.
/// Tests Pro filter handling in Aggregation.
/// </summary>
[ExampleMeta("E190", "SumAsync - With Filter", "Analytics",
    ExampleTier.Free, 190, "Sum", "Where", "Filter", "Aggregation", RelatedApis = ["IRedbQueryable.SumAsync", "IRedbQueryable.Where"])]
public class E190_SumAsyncFiltered : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Uncomment to see generated SQL:
        //var sql = await redb.Query<EmployeeProps>()
        //    .Where(x => x.Department == "Engineering")
        //    .ToSqlStringAsync();
        //Console.WriteLine(sql);

        // Sum salaries only for Engineering department
        var engineeringSalary = await redb.Query<EmployeeProps>()
            .Where(x => x.Department == "Engineering")
            .SumAsync(x => x.Salary);

        // Compare with total (no filter)
        var totalSalary = await redb.Query<EmployeeProps>()
            .SumAsync(x => x.Salary);

        sw.Stop();

        return Ok("E190", "SumAsync - With Filter", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Engineering SUM: {engineeringSalary:N0}", $"Total SUM: {totalSalary:N0}", $"Ratio: {(totalSalary > 0 ? engineeringSalary / totalSalary * 100 : 0):F1}%"]);
    }
}
