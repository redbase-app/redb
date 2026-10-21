using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E070_CountAsync"/>: total and filtered count.</summary>
[ExampleMeta("E070_ef", "EF: CountAsync - Total and Filtered", "Query", ExampleTier.Free, 1, "EF", "Count")]
public class E070_CountAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var totalCount = await db.EmployeeProps.CountAsync();
        var managersCount = await db.EmployeeProps.Where(e => e.Position == "Manager").CountAsync();
        var highFloorCount = await db.EmployeeProps
            .Where(e => e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor > 20)
            .CountAsync();

        sw.Stop();

        return Ok("E070_ef", "EF: CountAsync - Total and Filtered", ExampleTier.Free, sw.ElapsedMilliseconds, totalCount,
            [$"Total: {totalCount}, Managers: {managersCount}, Home floor above 20: {highFloorCount}"]);
    }
}
