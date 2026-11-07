using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E183_FirstOrDefaultAsync"/>: the top earner above 80000 as a whole object, and a miss.</summary>
[ExampleMeta("E183_ef", "EF: FirstOrDefaultAsync", "Query", ExampleTier.Free, 1, "EF")]
public class E183_FirstOrDefaultAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var highEarnerEntity = await db.EmployeeProps.WithGraph()
            .Where(e => e.Salary > 80000m)
            .OrderByDescending(e => e.Salary)
            .FirstOrDefaultAsync();
        var highEarner = highEarnerEntity == null ? null : EmployeeEfGraph.ToBusiness(highEarnerEntity);

        var notFoundEntity = await db.EmployeeProps.WithGraph()
            .Where(e => e.Salary > 10_000_000m)
            .OrderBy(e => e._id) // EF's own need with split queries, as in the other twins
            .FirstOrDefaultAsync();

        sw.Stop();

        if (highEarner == null)
        {
            return Fail("E183_ef", "EF: FirstOrDefaultAsync", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No employees with salary > 80000 found.");
        }

        return Ok("E183_ef", "EF: FirstOrDefaultAsync", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"First high earner: {highEarner.Props.FirstName} {highEarner.Props.LastName}",
             $"Salary: ${highEarner.Props.Salary:N0}",
             $"Not found (null): {notFoundEntity == null}"]);
    }
}
