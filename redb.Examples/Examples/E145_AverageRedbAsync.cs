using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// AverageRedbAsync - average of base IRedbObject field WITHOUT JOIN.
/// Useful for statistics on Id distribution.
/// </summary>
[ExampleMeta("E145", "AverageRedbAsync - Base Field Avg", "Analytics",
    ExampleTier.Free, 145, "AverageRedbAsync", "Aggregation", "NoJoin", RelatedApis = ["IRedbQueryable.AverageRedbAsync"])]
public class E145_AverageRedbAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Average of base field Id (from _objects table, NO JOIN!)
        var avgId = await redb.Query<EmployeeProps>().AverageRedbAsync(x => x.Id);

        // Get min/max for context
        var minId = await redb.Query<EmployeeProps>().MinRedbAsync(x => x.Id);
        var maxId = await redb.Query<EmployeeProps>().MaxRedbAsync(x => x.Id);

        sw.Stop();

        return Ok("E145", "AverageRedbAsync - Base Field Avg", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"AVG(o._id) = {avgId:F2}", 
             $"Range: {minId?.ToString() ?? "N/A"} - {maxId?.ToString() ?? "N/A"}", 
             "NO JOIN with _values!"]);
    }
}
