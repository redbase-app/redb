using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E081_OrderByThenBy"/>: Department ascending, Salary descending, first 100 whole employees.
/// </summary>
[ExampleMeta("E081_ef", "EF: OrderBy + ThenBy", "Query", ExampleTier.Free, 1, "EF")]
public class E081_OrderByThenBy_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var result = await db.EmployeeProps.WithGraph()
            .OrderBy(e => e.Department)
            .ThenByDescending(e => e.Salary)
            .Take(100)
            .ToBusinessListAsync();

        sw.Stop();

        return Ok("E081_ef", "EF: OrderBy + ThenBy", ExampleTier.Free, sw.ElapsedMilliseconds, result.Count,
            ["Sort: Department ASC, Salary DESC"]);
    }
}