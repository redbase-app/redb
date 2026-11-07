using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E152_ArithmeticAddFields"/>: Age * 1000 + Salary above 120 000, up to 100 whole objects and the count.</summary>
[ExampleMeta("E152_ef", "EF: Where - Arithmetic Add Fields", "Query", ExampleTier.Pro, 1, "EF", "Where", "Arithmetic")]
public class E152_ArithmeticAddFields_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => e.Age * 1000 + e.Salary > 120_000m);
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var first = results.FirstOrDefault();
        var score = first != null ? first.Props.Age * 1000 + (int)first.Props.Salary : 0;
        return Ok("E152_ef", "EF: Where - Arithmetic Add Fields", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            ["Formula: Age * 1000 + Salary > 120,000",
             $"Found: {totalCount} employees",
             $"First: {first?.Props.FirstName}, Age {first?.Props.Age}, Score: {score:N0}"]);
    }
}
