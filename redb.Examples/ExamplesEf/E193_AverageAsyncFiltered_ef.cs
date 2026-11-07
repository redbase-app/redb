using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E193_AverageAsyncFiltered"/>: AVG(Salary) of ages 30 to 40 and of everyone, two queries.</summary>
[ExampleMeta("E193_ef", "EF: AverageAsync - With Filter", "Analytics", ExampleTier.Free, 1, "EF", "Average")]
public class E193_AverageAsyncFiltered_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var avgSalaryMiddle = await db.EmployeeProps.Where(x => x.Age >= 30 && x.Age <= 40).AverageAsync(x => x.Salary);
        var avgSalaryAll = await db.EmployeeProps.AverageAsync(x => x.Salary);
        var avgFloorLondon = await db.EmployeeProps
            .Where(x => x.EmployeeProps_HomeAddress!.City == "London")
            .AverageAsync(x => x.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor);

        sw.Stop();

        return Ok("E193_ef", "EF: AverageAsync - With Filter", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Age 30-40 AVG: {avgSalaryMiddle:N0}", $"All AVG: {avgSalaryAll:N0}",
             $"London AVG(HomeAddress.Building.Floor): {avgFloorLondon:F2}"]);
    }
}
