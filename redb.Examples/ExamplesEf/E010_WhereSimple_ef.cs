using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E010_WhereSimple"/>: up to 100 employees with Salary above 75000, as whole objects.</summary>
[ExampleMeta("E010_ef", "EF: Where - Filter by Salary", "Query", ExampleTier.Free, 1, "EF", "Where", "Query", "Filter")]
public class E010_WhereSimple_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        // OrderBy is EF's own need, redb's query has none: with split queries and Take, every child query
        // must pick the same 100 employees, or some of them come back without their collections.
        var entities = await db.EmployeeProps.WithGraph()
            .Where(e => e.Salary > 75000m)
            .OrderBy(e => e._id)
            .Take(100)
            .ToListAsync();
        var result = entities.Select(EmployeeEfGraph.ToBusiness).ToList();

        sw.Stop();

        return Ok("E010_ef", "EF: Where - Filter by Salary", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: Salary > 75000"]);
    }
}
