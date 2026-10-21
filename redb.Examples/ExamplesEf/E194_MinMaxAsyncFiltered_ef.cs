using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E194_MinMaxAsyncFiltered"/>: MIN and MAX of Salary in Sales and overall, four queries as in redb.</summary>
[ExampleMeta("E194_ef", "EF: MinMaxAsync - With Filter", "Analytics", ExampleTier.Free, 1, "EF", "Min", "Max")]
public class E194_MinMaxAsyncFiltered_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var sales = db.EmployeeProps.Where(x => x.Department == "Sales");
        var minSalary = await sales.MinAsync(x => x.Salary);
        var maxSalary = await sales.MaxAsync(x => x.Salary);
        var minAll = await db.EmployeeProps.MinAsync(x => x.Salary);
        var maxAll = await db.EmployeeProps.MaxAsync(x => x.Salary);

        sw.Stop();

        return Ok("E194_ef", "EF: MinMaxAsync - With Filter", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
        [
            $"Sales: {minSalary:N0} - {maxSalary:N0}",
            $"All: {minAll:N0} - {maxAll:N0}"
        ]);
    }
}
