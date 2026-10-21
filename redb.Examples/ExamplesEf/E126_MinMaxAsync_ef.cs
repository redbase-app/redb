using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E126_MinMaxAsync"/>: MIN and MAX of Salary and of
/// HomeAddress.Building.Floor, four queries as in redb; the nested floor costs EF two joins.</summary>
[ExampleMeta("E126_ef", "EF: MinAsync/MaxAsync - Range", "Analytics", ExampleTier.Free, 1, "EF")]
public class E126_MinMaxAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var minSalary = await db.EmployeeProps.MinAsync(e => e.Salary);
        var maxSalary = await db.EmployeeProps.MaxAsync(e => e.Salary);
        var minFloor = await db.EmployeeProps.MinAsync(e => e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor);
        var maxFloor = await db.EmployeeProps.MaxAsync(e => e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor);

        sw.Stop();

        return Ok("E126_ef", "EF: MinAsync/MaxAsync - Range", ExampleTier.Free, sw.ElapsedMilliseconds, 4,
            [$"MIN(Salary) = {minSalary:N0}", $"MAX(Salary) = {maxSalary:N0}",
             $"MIN/MAX(HomeAddress.Building.Floor) = {minFloor}..{maxFloor}"]);
    }
}
