using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E151_ArithmeticMultiply"/>: Salary * 12 above 1 000 000, up to 100 whole objects and the count.</summary>
[ExampleMeta("E151_ef", "EF: Where - Arithmetic Multiply", "Query", ExampleTier.Pro, 1, "EF", "Where", "Arithmetic")]
public class E151_ArithmeticMultiply_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => e.Salary * 12 > 1_000_000m);
        var highEarners = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var first = highEarners.FirstOrDefault();
        var annualSalary = first?.Props.Salary * 12 ?? 0;
        return Ok("E151_ef", "EF: Where - Arithmetic Multiply", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            ["Filter: Salary * 12 > 1,000,000",
             $"Found: {totalCount} high earners",
             $"First: {first?.Props.FirstName} {first?.Props.LastName}, Annual: ${annualSalary:N0}"]);
    }
}
