using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E073_AllAsync"/>: three AllAsync checks.</summary>
[ExampleMeta("E073_ef", "EF: AllAsync - Check All Match", "Query", ExampleTier.Free, 1, "EF")]
public class E073_AllAsync_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var allNonNegativeSalary = await db.EmployeeProps.AllAsync(e => e.Salary >= 0);
        var allHighEarnersOver25 = await db.EmployeeProps.Where(e => e.Salary > 100000m).AllAsync(e => e.Age > 25);
        var allMillionaires = await db.EmployeeProps.AllAsync(e => e.Salary > 1_000_000m);
        var allBerlinFloorsValid = await db.EmployeeProps
            .Where(e => e.EmployeeProps_HomeAddress!.City == "Berlin")
            .AllAsync(e => e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor >= 1);

        sw.Stop();

        var passedCount = new[] { allNonNegativeSalary, allHighEarnersOver25, allBerlinFloorsValid }.Count(c => c);
        return Ok("E073_ef", "EF: AllAsync - Check All Match", ExampleTier.Free, sw.ElapsedMilliseconds, passedCount,
            [$"All Salary >= 0: {allNonNegativeSalary}",
             $"All high earners (>100k) over 25: {allHighEarnersOver25}",
             $"All millionaires (>1M): {allMillionaires} (expected false)",
             $"All in Berlin on floor 1 or higher: {allBerlinFloorsValid}"]);
    }
}
