using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E122_ListItemDirectCompare"/>: the "Active" item looked up, then persons whose Status is that item, whole.</summary>
[ExampleMeta("E122_ef", "EF: ListItem - Direct Compare", "List", ExampleTier.Free, 1, "EF", "List", "Where")]
public class E122_ListItemDirectCompare_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var list = await db.lists.FirstOrDefaultAsync(l => l._name == "ExampleStatuses");
        if (list == null)
        {
            sw.Stop();
            return Fail("E122_ef", "EF: ListItem - Direct Compare", ExampleTier.Free, sw.ElapsedMilliseconds,
                "List 'ExampleStatuses' not found. Run E114_ef first.");
        }

        var items = await db.list_items.Where(i => i._id_list == list._id).ToListAsync();
        var activeItem = items.FirstOrDefault(i => i._value == "Active");
        if (activeItem == null)
        {
            sw.Stop();
            return Fail("E122_ef", "EF: ListItem - Direct Compare", ExampleTier.Free, sw.ElapsedMilliseconds,
                "Item 'Active' not found. Run E115_ef first.");
        }

        var results = (await db.Persons()
                .Where(p => p.Status == activeItem._id)
                .OrderBy(p => p._id)
                .Take(100)
                .ToListAsync())
            .Select(PersonEfMapping.ToBusiness).ToList();

        sw.Stop();

        return Ok("E122_ef", "EF: ListItem - Direct Compare", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            [$"Filter: Status == ListItem(ID:{activeItem._id})", $"Found: {results.Count} persons"]);
    }
}
