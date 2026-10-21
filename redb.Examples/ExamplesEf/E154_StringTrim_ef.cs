using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E154_StringTrim"/>: FirstName.Trim().Length above 3, up to 100 whole objects and the count.</summary>
[ExampleMeta("E154_ef", "EF: Where - String Trim", "Query", ExampleTier.Pro, 1, "EF", "Where", "String")]
public class E154_StringTrim_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => e.FirstName!.Trim().Length > 3);
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var names = results.Take(5).Select(e => $"{e.Props.FirstName} ({e.Props.FirstName.Length} chars)");
        return Ok("E154_ef", "EF: Where - String Trim", ExampleTier.Pro, sw.ElapsedMilliseconds, totalCount,
            ["Filter: FirstName.Trim().Length > 3",
             $"Found: {totalCount} employees",
             $"Examples: {string.Join(", ", names)}"]);
    }
}
