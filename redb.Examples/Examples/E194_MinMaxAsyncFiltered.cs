using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// MinAsync/MaxAsync with Where filter applied before aggregation.
/// Filter employees by department, then find salary range.
/// Tests Pro filter handling in single Aggregation.
/// </summary>
[ExampleMeta("E194", "MinMaxAsync - With Filter", "Analytics",
    ExampleTier.Free, 194, "Min", "Max", "Where", "Filter", "Aggregation", RelatedApis = ["IRedbQueryable.MinAsync", "IRedbQueryable.MaxAsync", "IRedbQueryable.Where"])]
public class E194_MinMaxAsyncFiltered : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Salary range for Sales department only
        var query = redb.Query<EmployeeProps>()
            .Where(x => x.Department == "Sales");

        var minSalary = await query.MinAsync(x => x.Salary);
        var maxSalary = await query.MaxAsync(x => x.Salary);

        // Compare with overall range
        var minAll = await redb.Query<EmployeeProps>().MinAsync(x => x.Salary);
        var maxAll = await redb.Query<EmployeeProps>().MaxAsync(x => x.Salary);

        sw.Stop();

        return Ok("E194", "MinMaxAsync - With Filter", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
        [
            $"Sales: {minSalary:N0} - {maxSalary:N0}",
            $"All: {minAll:N0} - {maxAll:N0}"
        ]);
    }
}
