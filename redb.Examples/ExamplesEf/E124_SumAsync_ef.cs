using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E124_SumAsync"/>: SUM(Salary) and SUM(HomeAddress.Building.Floor) on the
/// server; the nested floor costs EF two joins.</summary>
[ExampleMeta("E124_ef", "EF: SumAsync - Total", "Analytics", ExampleTier.Free, 1, "EF", "Sum")]
public class E124_SumAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var totalSalary = await db.EmployeeProps.SumAsync(e => e.Salary);
        var totalFloor = await db.EmployeeProps.SumAsync(e => e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor);

        sw.Stop();

        return Ok("E124_ef", "EF: SumAsync - Total", ExampleTier.Free, sw.ElapsedMilliseconds, 2,
            [$"SUM(Salary) = {totalSalary:N0}", $"SUM(HomeAddress.Building.Floor) = {totalFloor:N0}", "Server-side aggregation"]);
    }
}
