using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E072_AnyWithPredicate"/>: is there anyone with Salary above 100000.</summary>
[ExampleMeta("E072_ef", "EF: AnyAsync - With Predicate", "Query", ExampleTier.Free, 1, "EF")]
public class E072_AnyWithPredicate_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var anyHighEarners = await db.EmployeeProps.AnyAsync(e => e.Salary > 100_000m);
        var anyBerlinHighFloor = await db.EmployeeProps.AnyAsync(e =>
            e.EmployeeProps_HomeAddress!.City == "Berlin"
            && e.EmployeeProps_HomeAddress!.EmployeeProps_HomeAddress_Building!.Floor > 25);

        sw.Stop();

        return Ok("E072_ef", "EF: AnyAsync - With Predicate", ExampleTier.Free, sw.ElapsedMilliseconds, anyHighEarners ? 1 : 0,
            [$"Any with Salary > 100k: {anyHighEarners}", $"Any in Berlin above floor 25: {anyBerlinHighFloor}"]);
    }
}
