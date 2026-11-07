using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E125_AverageAsync"/>: AVG(Age) and AVG(HomeAddress.Building.Floor) on the
/// server; the nested floor costs EF two joins.</summary>
[ExampleMeta("E125_ef", "EF: AverageAsync - Mean", "Analytics", ExampleTier.Free, 1, "EF")]
public class E125_AverageAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var avgAge = await db.EmployeeProps.AverageAsync(e => e.Age);
        var avgFloor = await db.EmployeeProps.AverageAsync(e => e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor);

        sw.Stop();

        return Ok("E125_ef", "EF: AverageAsync - Mean", ExampleTier.Free, sw.ElapsedMilliseconds, 2,
            [$"AVG(Age) = {avgAge:F2}", $"AVG(HomeAddress.Building.Floor) = {avgFloor:F2}", "Server-side aggregation"]);
    }
}
