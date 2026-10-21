using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E146_AggregateRedbAsync"/>: five aggregates over header fields in one query,
/// grouping the whole set by a constant; the dates come from the <c>objects</c> header.
/// </summary>
[ExampleMeta("E146_ef", "EF: AggregateRedbAsync - Batch Base", "Analytics", ExampleTier.Free, 1, "EF", "Aggregate")]
public class E146_AggregateRedbAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var stats = await db.EmployeeProps
            .Select(e => new { e._id, e.objects._date_create, e.objects._date_modify })
            .GroupBy(_ => 1)
            .Select(g => new
            {
                MaxId = g.Max(x => x._id),
                MinDateCreate = g.Min(x => x._date_create),
                MaxDateModify = g.Max(x => x._date_modify),
                AvgId = g.Average(x => x._id),
                Count = g.Count()
            })
            .FirstAsync();

        sw.Stop();

        return Ok("E146_ef", "EF: AggregateRedbAsync - Batch Base", ExampleTier.Free, sw.ElapsedMilliseconds, stats.Count,
            ["5 aggregations in 1 query (join to the header)",
             $"MaxId: {stats.MaxId}, Count: {stats.Count}",
             $"Dates: {stats.MinDateCreate:yyyy-MM-dd} - {stats.MaxDateModify:yyyy-MM-dd}"]);
    }
}
