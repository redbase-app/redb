using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E155_StringChain"/>: Department.ToUpper().Length above 5, up to 100 whole objects and the count.</summary>
[ExampleMeta("E155_ef", "EF: Where - String Chain", "Query", ExampleTier.Pro, 1, "EF", "Where", "String")]
public class E155_StringChain_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => e.Department!.ToUpper().Length > 5);
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var depts = results.Select(e => e.Props.Department).Distinct().Take(5);
        return Ok("E155_ef", "EF: Where - String Chain", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            ["Filter: Department.ToUpper().Length > 5",
             $"Found: {totalCount} employees",
             $"Departments: {string.Join(", ", depts)}"]);
    }
}
