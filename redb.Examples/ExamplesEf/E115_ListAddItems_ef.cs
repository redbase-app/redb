using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Ef.Entities;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E115_ListAddItems"/>: four items with aliases added to "ExampleStatuses" in one save.</summary>
[ExampleMeta("E115_ef", "EF: List - Add Items", "List", ExampleTier.Free, 1, "EF", "List")]
public class E115_ListAddItems_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var list = await db.lists.FirstOrDefaultAsync(l => l._name == "ExampleStatuses");
        if (list == null)
        {
            sw.Stop();
            return Fail("E115_ef", "EF: List - Add Items", ExampleTier.Free, sw.ElapsedMilliseconds,
                "List 'ExampleStatuses' not found. Run E114_ef first.");
        }

        var values = new List<string> { "Active", "Inactive", "Pending", "Blocked" };
        var aliases = new List<string> { "Active status", "Inactive status", "Pending review", "Blocked access" };
        var addedItems = values.Select((v, i) => new list_items { _id_list = list._id, _value = v, _alias = aliases[i] }).ToList();
        db.list_items.AddRange(addedItems);
        await db.SaveChangesAsync();

        sw.Stop();

        var itemsList = string.Join(", ", addedItems.Select(i => i._value));
        return Ok("E115_ef", "EF: List - Add Items", ExampleTier.Free, sw.ElapsedMilliseconds, addedItems.Count,
            [$"Added: {addedItems.Count} items", $"Values: {itemsList}"]);
    }
}
