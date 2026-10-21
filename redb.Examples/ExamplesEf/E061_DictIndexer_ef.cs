using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E061_DictIndexer"/>: BonusByYear[2023] above 6000, up to 100 whole employees.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E061_ef", "EF: Dict Indexer - Bonus by Year", "Query", ExampleTier.Free, 1, "EF")]
public class E061_DictIndexer_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => e.EmployeeProps_BonusByYear.Any(d => d._key == 2023 && d._value > 6000m))
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E061_ef", "EF: Dict Indexer - Bonus by Year", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: BonusByYear[2023] > 6000"]);
    }
}