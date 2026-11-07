using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E123_ListItemArrayAny"/>: persons with any role item valued "Active", whole.</summary>
[ExampleMeta("E123_ef", "EF: ListItem Array - Any", "List", ExampleTier.Free, 1, "EF", "List", "Any")]
public class E123_ListItemArrayAny_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var results = (await db.Persons()
                .Where(p => p.PersonProps_Roles.Any(r => r._valueNavigation!._value == "Active"))
                .OrderBy(p => p._id)
                .Take(100)
                .ToListAsync())
            .Select(PersonEfMapping.ToBusiness).ToList();

        sw.Stop();

        if (results.Count == 0)
            return Ok("E123_ef", "EF: ListItem Array - Any", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                ["Filter: Roles.Any(r => r.Value == 'Active')", "No matching persons found. Run E119_ef first."]);

        var firstName = results.FirstOrDefault()?.Props?.Name ?? "N/A";
        var rolesCount = results.FirstOrDefault()?.Props?.Roles?.Count ?? 0;
        return Ok("E123_ef", "EF: ListItem Array - Any", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            ["Filter: Roles.Any(r => r.Value == 'Active')", $"Found: {results.Count}, First: {firstName} ({rolesCount} roles)"]);
    }
}
