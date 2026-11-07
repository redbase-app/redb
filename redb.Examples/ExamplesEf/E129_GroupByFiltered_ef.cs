using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E129_GroupByFiltered"/>: Salary above 50000, grouped by department, with the
/// highest HomeAddress.Building.Floor per group; the nested floor costs EF two joins.</summary>
[ExampleMeta("E129_ef", "EF: GroupBy - With Filter", "Analytics", ExampleTier.Free, 1, "EF")]
public class E129_GroupByFiltered_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var byDeptFiltered = await db.EmployeeProps
            .Where(x => x.Salary > 50000m)
            .Select(e => new
            {
                e.Department,
                e.Salary,
                Floor = e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor
            })
            .GroupBy(x => x.Department)
            .Select(g => new
            {
                Department = g.Key,
                TotalSalary = g.Sum(x => x.Salary),
                MaxFloor = g.Max(x => x.Floor),
                Count = g.Count()
            })
            .ToListAsync();

        sw.Stop();

        var first = byDeptFiltered.FirstOrDefault();
        return Ok("E129_ef", "EF: GroupBy - With Filter", ExampleTier.Free, sw.ElapsedMilliseconds, byDeptFiltered.Count,
            ["Filter: Salary > 50k", $"Groups: {byDeptFiltered.Count}, First: {first?.Department ?? "N/A"}, Max home floor: {first?.MaxFloor}"]);
    }
}
