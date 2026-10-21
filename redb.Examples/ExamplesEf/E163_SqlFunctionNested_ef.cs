using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E163_SqlFunctionNested"/>: SQRT(Salary) above 250, up to 100 whole objects and the
/// count. EF writes sqrt for <c>Math.Sqrt</c>.
/// </summary>
[ExampleMeta("E163_ef", "EF: Sql.Function - Nested", "Query", ExampleTier.Pro, 1, "EF", "Where", "Function")]
public class E163_SqlFunctionNested_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => Math.Sqrt((double)e.Salary!.Value) > 250);
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var salaries = results.Take(5).Select(e => $"${e.Props.Salary:N0} (sqrt={Math.Sqrt((double)e.Props.Salary):F0})");
        return Ok("E163_ef", "EF: Sql.Function - Nested", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            ["SQL: SQRT(Salary) > 250 (Salary > 62,500)",
             $"Found: {totalCount} employees",
             $"Salaries: {string.Join(", ", salaries)}"]);
    }
}
