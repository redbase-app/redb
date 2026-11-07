using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E130_GroupByMultiKey"/>: grouped by department and position, with the average
/// HomeAddress.Building.Floor per group; the nested floor costs EF two joins.</summary>
[ExampleMeta("E130a_ef", "EF: GroupBy - Composite Key", "Analytics", ExampleTier.Free, 1, "EF")]
public class E130_GroupByMultiKey_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var byDeptPosition = await db.EmployeeProps
            .Select(e => new
            {
                e.Department,
                e.Position,
                e.Salary,
                Floor = e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor
            })
            .GroupBy(x => new { x.Department, x.Position })
            .Select(g => new
            {
                g.Key.Department,
                g.Key.Position,
                TotalSalary = g.Sum(x => x.Salary),
                AvgFloor = g.Average(x => x.Floor),
                Count = g.Count()
            })
            .ToListAsync();

        sw.Stop();

        var first = byDeptPosition.FirstOrDefault();
        return Ok("E130a_ef", "EF: GroupBy - Composite Key", ExampleTier.Free, sw.ElapsedMilliseconds, byDeptPosition.Count,
            [$"Groups: {byDeptPosition.Count}", $"First: {first?.Department ?? "N/A"}/{first?.Position ?? "N/A"}, Avg home floor: {first?.AvgFloor:F2}"]);
    }
}
