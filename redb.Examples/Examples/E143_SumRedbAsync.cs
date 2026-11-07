using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// SumRedbAsync - sum of base IRedbObject field WITHOUT JOIN to _values.
/// Much faster than SumAsync for base fields like Id.
/// </summary>
[ExampleMeta("E143", "SumRedbAsync - Base Field Sum", "Analytics",
    ExampleTier.Free, 143, "SumRedbAsync", "Aggregation", "NoJoin", RelatedApis = ["IRedbQueryable.SumRedbAsync"])]
public class E143_SumRedbAsync : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Sum of base field Id (from _objects table, NO JOIN with _values!)
        var sumId = await redb.Query<EmployeeProps>().SumRedbAsync(x => x.Id);

        // Compare with regular count
        var count = await redb.Query<EmployeeProps>().CountAsync();

        sw.Stop();

        return Ok("E143", "SumRedbAsync - Base Field Sum", ExampleTier.Free, sw.ElapsedMilliseconds, count,
            [$"SUM(o._id) = {sumId:N0}", $"NO JOIN with _values!", $"Count: {count}"]);
    }
}
