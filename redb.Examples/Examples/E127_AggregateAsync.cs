using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Batch aggregation with multiple functions in single query.
/// Calculates Sum, Avg, Min, Max, Count in ONE SQL request, over fields of the employee and over a field
/// two nested classes deep (HomeAddress.Building.Floor): for redb a nested field is just another structure.
/// </summary>
[ExampleMeta("E127", "AggregateAsync - Batch", "Analytics",
    ExampleTier.Free, 127, "Aggregate", "Batch", "Sum", "Avg", "Count", RelatedApis = ["IRedbQueryable.AggregateAsync", "Agg.Sum", "Agg.Average", "Agg.Count"])]
public class E127_AggregateAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Multiple aggregations in ONE query (Pro PVT-based)
        var stats = await redb.Query<EmployeeProps>()
            .AggregateAsync(x => new
            {
                TotalSalary = Agg.Sum(x.Props.Salary),
                AvgAge = Agg.Average(x.Props.Age),
                MinSalary = Agg.Min(x.Props.Salary),
                MaxSalary = Agg.Max(x.Props.Salary),
                MinFloor = Agg.Min(x.Props.HomeAddress!.Building!.Floor),
                MaxFloor = Agg.Max(x.Props.HomeAddress!.Building!.Floor),
                AvgFloor = Agg.Average(x.Props.HomeAddress!.Building!.Floor),
                Count = Agg.Count()
            });

        sw.Stop();

        return Ok("E127", "AggregateAsync - Batch", ExampleTier.Free, sw.ElapsedMilliseconds, (int)stats.Count,
            [$"8 aggregations in 1 query, 3 of them over HomeAddress.Building.Floor",
             $"Total: {stats.TotalSalary:N0}, Avg Age: {stats.AvgAge:F1}, Count: {stats.Count}",
             $"Home floor: {stats.MinFloor}..{stats.MaxFloor}, avg {stats.AvgFloor:F1}"]);
    }
}
