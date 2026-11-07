using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E032_StringContainsIgnoreCase"/>: LastName contains smith in any case, up to 100 whole employees.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// <para>EF Core does not translate Contains with a StringComparison; the PostgreSQL provider's own EF.Functions.ILike is the EF way to say it.</para>
/// </summary>
[ExampleMeta("E032_ef", "EF: Where - Contains IgnoreCase", "Query", ExampleTier.Free, 1, "EF")]
public class E032_StringContainsIgnoreCase_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => EF.Functions.ILike(e.LastName!, "%smith%"))
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E032_ef", "EF: Where - Contains IgnoreCase", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: 'smith' (case-insensitive)"]);
    }
}