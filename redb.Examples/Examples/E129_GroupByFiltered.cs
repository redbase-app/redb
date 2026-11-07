using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// GroupBy with Where filter applied before grouping.
/// Filter employees by salary, then group by department; one aggregate reads a field two nested
/// classes deep (HomeAddress.Building.Floor).
/// </summary>
[ExampleMeta("E129", "GroupBy - With Filter", "Analytics",
    ExampleTier.Free, 129, "GroupBy", "Where", "Filter", "Analytics", RelatedApis = ["IRedbQueryable.GroupBy", "IRedbQueryable.Where"])]
public class E129_GroupByFiltered : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Uncomment to see generated SQL:
        //var sql = await redb.Query<EmployeeProps>()
        //    .Where(x => x.Salary > 50000)
        //    .GroupBy(x => x.Department)
        //    .ToSqlStringAsync(g => new { Dept = g.Key, Count = Agg.Count(g) });
        //Console.WriteLine(sql);

        var byDeptFiltered = await redb.Query<EmployeeProps>()
            .Where(x => x.Salary > 50000m)
            .GroupBy(x => x.Department)
            .SelectAsync(g => new
            {
                Department = g.Key,
                TotalSalary = Agg.Sum(g, x => x.Salary),
                MaxFloor = Agg.Max(g, x => x.HomeAddress!.Building!.Floor),
                Count = Agg.Count(g)
            });

        sw.Stop();

        var first = byDeptFiltered.FirstOrDefault();
        return Ok("E129", "GroupBy - With Filter", ExampleTier.Free, sw.ElapsedMilliseconds, byDeptFiltered.Count,
            [$"Filter: Salary > 50k", $"Groups: {byDeptFiltered.Count}, First: {first?.Department ?? "N/A"}, Max home floor: {first?.MaxFloor}"]);
    }
}
