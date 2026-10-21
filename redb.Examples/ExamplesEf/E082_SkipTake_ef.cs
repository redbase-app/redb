using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E082_SkipTake"/>: page 2 of 10 by LastName, whole employees.
/// </summary>
[ExampleMeta("E082_ef", "EF: Skip/Take - Pagination", "Query", ExampleTier.Free, 1, "EF")]
public class E082_SkipTake_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .OrderBy(e => e.LastName)
            .Skip(10)
            .Take(10)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E082_ef", "EF: Skip/Take - Pagination", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Page 2, size 10"]);
    }
}