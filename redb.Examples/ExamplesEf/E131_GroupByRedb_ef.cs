using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E131_GroupByRedb"/>: total salary and count per owner. The owner is a field of
/// the shared <c>objects</c> header, reached through the navigation from the employee.
/// </summary>
[ExampleMeta("E131_ef", "EF: GroupByRedb - Base Fields", "Analytics", ExampleTier.Free, 1, "EF", "GroupBy")]
public class E131_GroupByRedb_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var byOwner = await db.EmployeeProps
            .Select(e => new
            {
                e.objects._id_owner,
                e.Salary,
                Floor = e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor
            })
            .GroupBy(e => e._id_owner)
            .Select(g => new
            {
                OwnerId = g.Key,
                TotalSalary = g.Sum(e => e.Salary),
                AvgFloor = g.Average(e => e.Floor),
                Count = g.Count()
            })
            .ToListAsync();

        sw.Stop();

        var first = byOwner.FirstOrDefault();
        return Ok("E131_ef", "EF: GroupByRedb - Base Fields", ExampleTier.Free, sw.ElapsedMilliseconds, byOwner.Count,
            ["SQL: GROUP BY objects._id_owner (join to the header)", $"Groups: {byOwner.Count}, First owner: {first?.OwnerId ?? 0}",
             $"Avg home floor of the first owner: {first?.AvgFloor:F2}"]);
    }
}
