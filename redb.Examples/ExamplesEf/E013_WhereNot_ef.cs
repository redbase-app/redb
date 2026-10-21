using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E013_WhereNot"/>: Position other than Manager, up to 100 whole employees.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E013_ef", "EF: Where - NOT Equal", "Query", ExampleTier.Free, 1, "EF")]
public class E013_WhereNot_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .Where(e => e.Position != "Manager")
            .OrderBy(e => e._id)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E013_ef", "EF: Where - NOT Equal", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Filter: Position != 'Manager'"]);
    }
}