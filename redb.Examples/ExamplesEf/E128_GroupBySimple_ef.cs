using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E128_GroupBySimple"/>: per department total salary, average age, average
/// HomeAddress.Building.Floor and count; the nested floor costs EF two joins.</summary>
[ExampleMeta("E128_ef", "EF: GroupBy - Simple", "Analytics", ExampleTier.Free, 1, "EF", "GroupBy")]
public class E128_GroupBySimple_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var byDept = await db.EmployeeProps
            .Select(e => new
            {
                e.Department,
                e.Salary,
                e.Age,
                Floor = e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor
            })
            .GroupBy(x => x.Department)
            .Select(g => new
            {
                Department = g.Key,
                TotalSalary = g.Sum(x => x.Salary),
                AvgAge = g.Average(x => x.Age),
                AvgFloor = g.Average(x => x.Floor),
                Count = g.Count()
            })
            .ToListAsync();

        sw.Stop();

        var first = byDept.FirstOrDefault();
        return Ok("E128_ef", "EF: GroupBy - Simple", ExampleTier.Free, sw.ElapsedMilliseconds, byDept.Count,
            [$"Groups: {byDept.Count}", $"First: {first?.Department ?? "N/A"}, Count: {first?.Count ?? 0}, Avg home floor: {first?.AvgFloor:F2}"]);
    }
}
