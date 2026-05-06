using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// GroupBy on base RedbObject fields without JOIN.
/// Groups by SchemeId or OwnerId directly from _objects table.
/// Much faster than GroupBy on Props fields.
/// </summary>
[ExampleMeta("E131", "GroupByRedb - Base Fields", "Analytics",
    ExampleTier.Free, 131, "GroupByRedb", "Base", "NoJoin", "Analytics", RelatedApis = ["IRedbQueryable.GroupByRedb"])]
public class E131_GroupByRedb : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Uncomment to see generated SQL:
        // var sql = await redb.Query<EmployeeProps>()
        //     .GroupByRedb(x => x.OwnerId)
        //     .ToSqlStringAsync(g => new { Owner = g.Key, Count = Agg.Count(g) });
        // Console.WriteLine(sql);

        // GroupBy on base field (no JOIN with _values!)
        var byOwner = await redb.Query<EmployeeProps>()
            .GroupByRedb(x => x.OwnerId)
            .SelectAsync(g => new
            {
                OwnerId = g.Key,
                TotalSalary = Agg.Sum(g, x => x.Salary),
                Count = Agg.Count(g)
            });

        sw.Stop();

        var first = byOwner.FirstOrDefault();
        return Ok("E131", "GroupByRedb - Base Fields", ExampleTier.Free, sw.ElapsedMilliseconds, byOwner.Count,
            [$"SQL: GROUP BY o._id_owner (NO JOIN!)", $"Groups: {byOwner.Count}, First owner: {first?.OwnerId ?? 0}"]);
    }
}
