using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E116_ListGetByName"/>: the list found by name, then its items, two queries as in redb.</summary>
[ExampleMeta("E116_ef", "EF: List - Get Items", "List", ExampleTier.Free, 1, "EF", "List")]
public class E116_ListGetByName_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var list = await db.lists.FirstOrDefaultAsync(l => l._name == "ExampleStatuses");
        if (list == null)
        {
            sw.Stop();
            return Fail("E116_ef", "EF: List - Get Items", ExampleTier.Free, sw.ElapsedMilliseconds,
                "List 'ExampleStatuses' not found. Run E114_ef first.");
        }

        var items = (await db.list_items.Where(i => i._id_list == list._id).OrderBy(i => i._id).ToListAsync())
            .Select(PersonEfMapping.ToItem).ToList();

        sw.Stop();

        var itemsPreview = string.Join(", ", items.Take(3).Select(i => i.Value));
        if (items.Count > 3) itemsPreview += "...";
        return Ok("E116_ef", "EF: List - Get Items", ExampleTier.Free, sw.ElapsedMilliseconds, items.Count,
            [$"List: {list._name} (ID: {list._id})", $"Items: {itemsPreview}"]);
    }
}
