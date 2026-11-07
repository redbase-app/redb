using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E014_WhereRange"/>: 60000 <= Salary < 90000, up to 100 whole employees.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E014_ef", "EF: Where - Range Condition", "Query", ExampleTier.Free, 1, "EF")]
public class E014_WhereRange_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => e.Salary >= 60000m && e.Salary < 90000m)
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E014_ef", "EF: Where - Range Condition", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: 60k <= Salary < 90k"]);
    }
}