using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E053_ArrayWithConditions"/>: Skills has C#, Age >= 30, Salary > 70000, up to 100 whole employees.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E053_ef", "EF: Array Contains + Conditions", "Query", ExampleTier.Free, 1, "EF")]
public class E053_ArrayWithConditions_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => e.EmployeeProps_Skills.Any(s => s._value == "C#") && e.Age >= 30 && e.Salary > 70000m)
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E053_ef", "EF: Array Contains + Conditions", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: Skills has 'C#' AND Age >= 30 AND Salary > 70k"]);
    }
}