using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E052_ArrayNotContains"/>: Skills has C# and not intern, up to 100 whole employees.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E052_ef", "EF: Array NOT Contains", "Query", ExampleTier.Free, 1, "EF")]
public class E052_ArrayNotContains_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => e.EmployeeProps_Skills.Any(s => s._value == "C#") && !e.EmployeeProps_Skills.Any(s => s._value == "intern"))
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E052_ef", "EF: Array NOT Contains", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: Skills has 'C#' AND NOT 'intern'"]);
    }
}