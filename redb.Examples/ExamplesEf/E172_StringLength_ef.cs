using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E172_StringLength"/>: LastName.Length above 6, up to 100 whole objects and the count.</summary>
[ExampleMeta("E172_ef", "EF: String.Length Filter", "Query", ExampleTier.Free, 1, "EF", "Where", "String")]
public class E172_StringLength_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var filtered = db.EmployeeProps.Where(e => e.LastName!.Length > 6);
        var results = await filtered.WithGraph().OrderBy(e => e._id).Take(100).ToBusinessListAsync();
        var totalCount = await filtered.CountAsync();

        sw.Stop();

        var names = results.Take(5).Select(e => $"{e.Props.LastName} ({e.Props.LastName.Length} chars)");
        return Ok("E172_ef", "EF: String.Length Filter", ExampleTier.Free, sw.ElapsedMilliseconds, totalCount,
            ["Filter: LastName.Length > 6",
             $"Found: {totalCount} employees",
             $"Samples: {string.Join(", ", names)}"]);
    }
}
