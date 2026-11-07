using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E185_WhereBool"/>: two counts and five whole objects under combined conditions.
/// <para>OrderBy(_id) is EF's own need, the redb query has none: with split queries and Take, every child query must pick the same employees.</para>
/// </summary>
[ExampleMeta("E185_ef", "EF: Where - Combined Conditions", "Query", ExampleTier.Free, 1, "EF")]
public class E185_WhereBool_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var expHighEarnersCount = await db.EmployeeProps.Where(e => e.Age > 30 && e.Salary > 70000m).CountAsync();
        var expHighEarners = await db.EmployeeProps.WithGraph()
            .Where(e => e.Age > 30 && e.Salary > 70000m)
            .OrderBy(e => e._id)
            .Take(5)
            .ToBusinessListAsync();
        var techDeptCount = await db.EmployeeProps.Where(e => e.Department == "IT" || e.Department == "Engineering").CountAsync();

        sw.Stop();

        var names = expHighEarners.Take(3).Select(e => $"{e.Props.FirstName} ({e.Props.Age}, ${e.Props.Salary:N0})");
        return Ok("E185_ef", "EF: Where - Combined Conditions", ExampleTier.Free, sw.ElapsedMilliseconds,
            expHighEarnersCount + techDeptCount,
            [$"Age > 30 AND Salary > 70000: {expHighEarnersCount}",
             $"IT OR Engineering: {techDeptCount}",
             $"Samples: {string.Join(", ", names)}"]);
    }
}
