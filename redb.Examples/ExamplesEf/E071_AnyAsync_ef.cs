using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E071_AnyAsync"/>: does any employee exist.</summary>
[ExampleMeta("E071_ef", "EF: AnyAsync - Check Existence", "Query", ExampleTier.Free, 1, "EF")]
public class E071_AnyAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var anyExist = await db.EmployeeProps.AnyAsync();
        var anyOnTopFloor = await db.EmployeeProps
            .Where(e => e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor == 30)
            .AnyAsync();

        sw.Stop();

        return Ok("E071_ef", "EF: AnyAsync - Check Existence", ExampleTier.Free, sw.ElapsedMilliseconds, anyExist ? 1 : 0,
            [$"Any employees exist: {anyExist}", $"Any living on floor 30: {anyOnTopFloor}"]);
    }
}
