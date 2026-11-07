using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E190_SumAsyncFiltered"/>: SUM(Salary) of Engineering and of everyone, two queries.</summary>
[ExampleMeta("E190_ef", "EF: SumAsync - With Filter", "Analytics", ExampleTier.Free, 1, "EF", "Sum")]
public class E190_SumAsyncFiltered_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var engineeringSalary = await db.EmployeeProps.Where(x => x.Department == "Engineering").SumAsync(x => x.Salary) ?? 0m;
        var totalSalary = await db.EmployeeProps.SumAsync(x => x.Salary) ?? 0m;
        var berlinFloors = await db.EmployeeProps
            .Where(x => x.EmployeeProps_HomeAddress!.City == "Berlin")
            .SumAsync(x => x.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor) ?? 0;

        sw.Stop();

        return Ok("E190_ef", "EF: SumAsync - With Filter", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Engineering SUM: {engineeringSalary:N0}", $"Total SUM: {totalSalary:N0}",
             $"Ratio: {(totalSalary > 0 ? engineeringSalary / totalSalary * 100 : 0):F1}%",
             $"Berlin SUM(HomeAddress.Building.Floor): {berlinFloors:N0}"]);
    }
}
