using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E158_Parentheses"/>: (Salary / 12) above 7000, up to 100 whole objects and the count.</summary>
[ExampleMeta("E158_ef", "EF: Where - Parentheses Priority", "Query", ExampleTier.Pro, 1, "EF", "Where", "Arithmetic")]
public class E158_Parentheses_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => (e.Salary / 12m) > 7000m);
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var first = results.FirstOrDefault();
        var monthly = first?.Props.Salary / 12m ?? 0;
        return Ok("E158_ef", "EF: Where - Parentheses Priority", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            ["Filter: (Salary / 12) > 7,000 (monthly)",
             $"Found: {totalCount} employees",
             $"First: {first?.Props.FirstName}, Monthly: ${monthly:N0}"]);
    }
}
