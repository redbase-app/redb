using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// GroupBy + Window with Where filter applied before grouping.
/// Filter employees by salary, group by department, then rank groups.
/// Tests Pro filter handling in GroupedWindow queries.
/// </summary>
[ExampleMeta("E195", "GroupBy + Window - With Filter", "Analytics",
    ExampleTier.Pro, 195, "GroupBy", "Window", "Where", "Filter", "Rank", RelatedApis = ["IRedbGroupedQueryable.WithWindow", "IRedbQueryable.Where"])]
public class E195_GroupByWindowFiltered : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Filter high earners, group by department, rank by total salary
        var ranked = await redb.Query<EmployeeProps>()
            .Where(x => x.Salary > 60000m)
            .GroupBy(x => x.Department)
            .WithWindow(w => w.OrderByDesc(g => Agg.Sum(g, x => x.Salary)))
            .SelectAsync(g => new
            {
                Department = g.Key,
                TotalSalary = Agg.Sum(g, x => x.Salary),
                Count = Agg.Count(g),
                Rank = Win.Rank()
            });

        //var sql = await redb.Query<EmployeeProps>()
        //    .Where(x => x.Salary > 60000m)
        //    .GroupBy(x => x.Department)
        //    .WithWindow(w => w.OrderByDesc(g => Agg.Sum(g, x => x.Salary)))
        //    .ToSqlStringAsync(g => new
        //    {
        //        Department = g.Key,
        //        TotalSalary = Agg.Sum(g, x => x.Salary),
        //        Count = Agg.Count(g),
        //        Rank = Win.Rank()
        //    });
        //Console.WriteLine(sql);

        sw.Stop();

        var top = ranked.FirstOrDefault();
        return Ok("E195", "GroupBy + Window - With Filter", ExampleTier.Pro, sw.ElapsedMilliseconds, ranked.Count,
        [
            $"Filter: Salary > 60k",
            $"Groups: {ranked.Count}",
            $"#1: {top?.Department ?? "N/A"} (Total: {top?.TotalSalary:N0})"
        ]);
    }
}
