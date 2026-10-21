using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E149_SelectProjection"/>: three fields of 100 employees, then the same 100 as
/// whole objects. As in redb, the measured time covers both.
/// </summary>
[ExampleMeta("E149_ef", "EF: Select - Projection", "Query", ExampleTier.Free, 1, "EF")]
public class E149_SelectProjection_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var projected = await db.EmployeeProps
            .Take(100)
            .Select(x => new { x._id, x.FirstName, x.Salary })
            .ToListAsync();

        // OrderBy(_id) is EF's own need: with split queries and Take, every child query must pick the same rows.
        var swFull = Stopwatch.StartNew();
        var full = await db.EmployeeProps.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        swFull.Stop();

        sw.Stop();

        var first = projected.FirstOrDefault();
        return Ok("E149_ef", "EF: Select - Projection", ExampleTier.Free, sw.ElapsedMilliseconds, projected.Count,
            [$"Projected 3 fields: {sw.ElapsedMilliseconds - swFull.ElapsedMilliseconds}ms",
             $"Full load: {swFull.ElapsedMilliseconds}ms ({full.Count} objects)",
             first != null ? $"First: {first.FirstName} ${first.Salary:N0}" : "No data"]);
    }
}
