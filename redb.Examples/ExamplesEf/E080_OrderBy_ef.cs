using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E080_OrderBy"/>: five lowest and five highest salaries, whole employees.</summary>
[ExampleMeta("E080_ef", "EF: OrderBy - Sort by Salary", "Query", ExampleTier.Free, 1, "EF")]
public class E080_OrderBy_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var lowest = await db.EmployeeProps.WithGraph().OrderBy(e => e.Salary).Take(5).ToBusinessListAsync();
        var highest = await db.EmployeeProps.WithGraph().OrderByDescending(e => e.Salary).Take(5).ToBusinessListAsync();

        sw.Stop();

        var lowestSalary = lowest.FirstOrDefault()?.Props?.Salary ?? 0;
        var highestSalary = highest.FirstOrDefault()?.Props?.Salary ?? 0;
        return Ok("E080_ef", "EF: OrderBy - Sort by Salary", ExampleTier.Free, sw.ElapsedMilliseconds, lowest.Count + highest.Count,
            [$"Lowest: ${lowestSalary:N0}, Highest: ${highestSalary:N0}"]);
    }
}
