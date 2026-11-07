using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E016_WhereChain"/>: three chained Where calls, up to 100 whole employees.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E016_ef", "EF: Where - Chained Filters", "Query", ExampleTier.Free, 1, "EF")]
public class E016_WhereChain_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => e.Age >= 30)
            .Where(e => e.Salary > 60000m)
            .Where(e => e.Department != "Support")
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E016_ef", "EF: Where - Chained Filters", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: .Where(Age).Where(Salary).Where(Dept)"]);
    }
}