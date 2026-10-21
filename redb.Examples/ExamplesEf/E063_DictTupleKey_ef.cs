using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E063_DictTupleKey"/>: PerformanceReviews[(2024, Q1)] is Excellent, up to 100 whole employees.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E063_ef", "EF: Dict - Tuple Key", "Query", ExampleTier.Free, 1, "EF")]
public class E063_DictTupleKey_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => e.EmployeeProps_PerformanceReviews.Any(r => r._key_Item1 == 2024 && r._key_Item2 == "Q1" && r._value == "Excellent"))
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E063_ef", "EF: Dict - Tuple Key", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: PerformanceReviews[(2024, 'Q1')] == 'Excellent'"]);
    }
}