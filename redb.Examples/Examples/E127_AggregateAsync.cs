using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Batch aggregation with multiple functions in single query.
/// Calculates Sum, Avg, Min, Max, Count in ONE SQL request.
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
                Count = Agg.Count()
            });

        sw.Stop();

        return Ok("E127", "AggregateAsync - Batch", ExampleTier.Free, sw.ElapsedMilliseconds, (int)stats.Count,
            [$"5 aggregations in 1 query", $"Total: {stats.TotalSalary:N0}, Avg Age: {stats.AvgAge:F1}, Count: {stats.Count}"]);
    }
}
