using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E015_WhereTripleAnd"/>: three conditions, highest salary first, top 100 whole
/// employees. The redb example reports the time of its first list; so does this one. The planner phases
/// it prints after that are redb's own and have no EF counterpart.
/// </summary>
[ExampleMeta("E015_ef", "EF: Where - Triple AND", "Query", ExampleTier.Free, 1, "EF")]
public class E015_WhereTripleAnd_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => e.Position != "" && e.Age >= 30 && e.Salary > 70000m)
            .OrderByDescending(e => e.Salary)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E015_ef", "EF: Where - Triple AND", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: Position != '' AND Age >= 30 AND Salary > 70k, ORDER BY Salary DESC, top 100"]);
    }
}
