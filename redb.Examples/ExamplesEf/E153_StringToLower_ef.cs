using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E153_StringToLower"/>: LastName.ToLower() contains "s", up to 100 whole objects and the count.</summary>
[ExampleMeta("E153_ef", "EF: Where - String ToLower", "Query", ExampleTier.Pro, 1, "EF", "Where", "String")]
public class E153_StringToLower_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => e.LastName!.ToLower().Contains("s"));
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var names = results.Take(3).Select(e => e.Props.LastName);
        return Ok("E153_ef", "EF: Where - String ToLower", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            ["Filter: LastName.ToLower().Contains(\"s\")",
             $"Found: {totalCount} employees",
             $"Examples: {string.Join(", ", names)}"]);
    }
}
