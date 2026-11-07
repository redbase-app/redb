using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E159_NestedMath"/>: |Salary - 80000| below 10000, up to 100 whole objects and the count.</summary>
[ExampleMeta("E159_ef", "EF: Where - Nested Math", "Query", ExampleTier.Pro, 1, "EF", "Where", "Math")]
public class E159_NestedMath_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => Math.Abs(e.Salary!.Value - 80000m) < 10000m);
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var salaries = results.Take(5).Select(e => $"${e.Props.Salary:N0}");
        return Ok("E159_ef", "EF: Where - Nested Math", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            ["Filter: |Salary - 80k| < 10k ($70k-$90k range)",
             $"Found: {totalCount} employees",
             $"Salaries: {string.Join(", ", salaries)}"]);
    }
}
