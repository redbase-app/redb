using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E050_ArrayContains"/>: Skills contains C#, up to 100 whole employees.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E050_ef", "EF: Array Contains - Find by Skill", "Query", ExampleTier.Free, 1, "EF")]
public class E050_ArrayContains_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => e.EmployeeProps_Skills.Any(s => s._value == "C#"))
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E050_ef", "EF: Array Contains - Find by Skill", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: Skills.Contains('C#')"]);
    }
}