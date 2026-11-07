using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E156_MathAbs"/>: |Age - 35| at most 5, up to 100 whole objects and the count.</summary>
[ExampleMeta("E156_ef", "EF: Where - Math.Abs", "Query", ExampleTier.Pro, 1, "EF", "Where", "Math")]
public class E156_MathAbs_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => Math.Abs(e.Age!.Value - 35) <= 5);
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var ages = results.Take(5).Select(e => e.Props.Age);
        var avgAge = results.Any() ? results.Average(e => e.Props.Age) : 0;
        return Ok("E156_ef", "EF: Where - Math.Abs", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            ["Filter: |Age - 35| <= 5 (ages 30-40)",
             $"Found: {totalCount} employees",
             $"Ages: {string.Join(", ", ages)}, Avg: {avgAge:F1}"]);
    }
}
