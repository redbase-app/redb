using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// AggregateRedbAsync - batch aggregation of base IRedbObject fields in ONE query.
/// Combines Sum, Min, Max, Avg, Count on base fields WITHOUT JOIN.
/// </summary>
[ExampleMeta("E146", "AggregateRedbAsync - Batch Base", "Analytics",
    ExampleTier.Free, 146, "AggregateRedbAsync", "Batch", "NoJoin", RelatedApis = ["IRedbQueryable.AggregateRedbAsync", "Agg.Sum", "Agg.Min", "Agg.Max"])]
public class E146_AggregateRedbAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Multiple aggregations on base fields in ONE query (NO JOIN!)
        var stats = await redb.Query<EmployeeProps>()
            .AggregateRedbAsync(x => new
            {
                MaxId = Agg.Max(x.Id),
                MinDateCreate = Agg.Min(x.DateCreate),
                MaxDateModify = Agg.Max(x.DateModify),
                AvgId = Agg.Average(x.Id),
                Count = Agg.Count()
            });

        sw.Stop();

        return Ok("E146", "AggregateRedbAsync - Batch Base", ExampleTier.Free, sw.ElapsedMilliseconds, (int)stats.Count,
            [$"5 aggregations in 1 query (NO JOIN!)", 
             $"MaxId: {stats.MaxId}, Count: {stats.Count}",
             $"Dates: {stats.MinDateCreate:yyyy-MM-dd} - {stats.MaxDateModify:yyyy-MM-dd}"]);
    }
}
