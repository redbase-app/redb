using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E041_DeepNested"/>: HomeAddress.Building.Floor above 10, up to 100 whole employees.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E041_ef", "EF: Where - Deep Nested Property", "Query", ExampleTier.Free, 1, "EF")]
public class E041_DeepNested_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor > 10)
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E041_ef", "EF: Where - Deep Nested Property", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: HomeAddress.Building.Floor > 10"]);
    }
}