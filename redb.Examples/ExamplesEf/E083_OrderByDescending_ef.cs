using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E083_OrderByDescending"/>: five top earners and five oldest, whole employees.</summary>
[ExampleMeta("E083_ef", "EF: OrderByDescending - Reverse Sort", "Query", ExampleTier.Free, 1, "EF")]
public class E083_OrderByDescending_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var topEarners = await db.EmployeeProps.WithGraph().OrderByDescending(e => e.Salary).Take(5).ToBusinessListAsync();
        var oldest = await db.EmployeeProps.WithGraph().OrderByDescending(e => e.Age).Take(5).ToBusinessListAsync();

        sw.Stop();

        var topSalary = topEarners.FirstOrDefault()?.Props?.Salary ?? 0;
        var oldestAge = oldest.FirstOrDefault()?.Props?.Age ?? 0;
        return Ok("E083_ef", "EF: OrderByDescending - Reverse Sort", ExampleTier.Free, sw.ElapsedMilliseconds,
            topEarners.Count + oldest.Count,
            [$"Top salary: ${topSalary:N0}", $"Oldest age: {oldestAge}"]);
    }
}
