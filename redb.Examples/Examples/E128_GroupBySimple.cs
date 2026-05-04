using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Groups objects by a field and calculates aggregates per group.
/// SQL: GROUP BY Department with SUM, AVG, COUNT.
/// </summary>
[ExampleMeta("E128", "GroupBy - Simple", "Analytics",
    ExampleTier.Free, 128, "GroupBy", "Aggregation", "Analytics", RelatedApis = ["IRedbQueryable.GroupBy", "IGroupedQueryable.SelectAsync"])]
public class E128_GroupBySimple : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Uncomment to see generated SQL:
        // var sql = await redb.Query<EmployeeProps>()
        //     .GroupBy(x => x.Department)
        //     .ToSqlStringAsync(g => new { Dept = g.Key, Total = Agg.Sum(g, x => x.Salary) });
        // Console.WriteLine(sql);

        var byDept = await redb.Query<EmployeeProps>()
            .GroupBy(x => x.Department)
            .SelectAsync(g => new
            {
                Department = g.Key,
                TotalSalary = Agg.Sum(g, x => x.Salary),
                AvgAge = Agg.Average(g, x => x.Age),
                Count = Agg.Count(g)
            });

        sw.Stop();

        var first = byDept.FirstOrDefault();
        return Ok("E128", "GroupBy - Simple", ExampleTier.Free, sw.ElapsedMilliseconds, byDept.Count,
            [$"Groups: {byDept.Count}", $"First: {first?.Department ?? "N/A"}, Count: {first?.Count ?? 0}"]);
    }
}
