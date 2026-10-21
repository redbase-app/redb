using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E011_WhereAnd"/>: Age >= 30 AND Salary > 70000, up to 100 whole employees.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E011_ef", "EF: Where - AND Condition", "Query", ExampleTier.Free, 1, "EF")]
public class E011_WhereAnd_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => e.Age >= 30 && e.Salary > 70000m)
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E011_ef", "EF: Where - AND Condition", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: Age >= 30 AND Salary > 70000"]);
    }
}