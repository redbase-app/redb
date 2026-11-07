using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E120_ListItemWhereByValue"/>: persons whose status item has the value "Active", whole.</summary>
[ExampleMeta("E120_ef", "EF: ListItem - Where by Value", "List", ExampleTier.Free, 1, "EF", "List", "Where")]
public class E120_ListItemWhereByValue_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var results = (await db.Persons()
                .Where(p => p.StatusNavigation!._value == "Active")
                .OrderBy(p => p._id)
                .Take(100)
                .ToListAsync())
            .Select(PersonEfMapping.ToBusiness).ToList();

        sw.Stop();

        if (results.Count == 0)
            return Ok("E120_ef", "EF: ListItem - Where by Value", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                ["No persons with Status='Active' found", "Run E118_ef first to create test data"]);

        var firstName = results.FirstOrDefault()?.Props?.Name ?? "N/A";
        return Ok("E120_ef", "EF: ListItem - Where by Value", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            ["Filter: Status.Value == 'Active'", $"Found: {results.Count}, First: {firstName}"]);
    }
}
