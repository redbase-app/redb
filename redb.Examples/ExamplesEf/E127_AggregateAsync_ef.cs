using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E127_AggregateAsync"/>: eight aggregates in one query. EF has no batch aggregate
/// API; the usual way is to group the whole set by a constant. The nested floor is reached through the
/// navigations EmployeeProps -> HomeAddress -> Building, which EF turns into two joins (Include plays no part
/// in an aggregate).
/// </summary>
[ExampleMeta("E127_ef", "EF: AggregateAsync - Batch", "Analytics", ExampleTier.Free, 1, "EF")]
public class E127_AggregateAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var stats = await db.EmployeeProps
            .Select(e => new
            {
                e.Salary,
                e.Age,
                Floor = e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor
            })
            .GroupBy(_ => 1)
            .Select(g => new
            {
                TotalSalary = g.Sum(x => x.Salary),
                AvgAge = g.Average(x => x.Age),
                MinSalary = g.Min(x => x.Salary),
                MaxSalary = g.Max(x => x.Salary),
                MinFloor = g.Min(x => x.Floor),
                MaxFloor = g.Max(x => x.Floor),
                AvgFloor = g.Average(x => x.Floor),
                Count = g.Count()
            })
            .FirstAsync();

        sw.Stop();

        return Ok("E127_ef", "EF: AggregateAsync - Batch", ExampleTier.Free, sw.ElapsedMilliseconds, stats.Count,
            ["8 aggregations in 1 query, 3 of them over HomeAddress.Building.Floor",
             $"Total: {stats.TotalSalary:N0}, Avg Age: {stats.AvgAge:F1}, Count: {stats.Count}",
             $"Home floor: {stats.MinFloor}..{stats.MaxFloor}, avg {stats.AvgFloor:F1}"]);
    }
}
