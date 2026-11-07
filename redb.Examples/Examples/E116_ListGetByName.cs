using System.Diagnostics;
using redb.Core;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Gets a RedbList by name and retrieves its items.
/// Shows how to lookup lists and access their contents.
/// Requires E114+E115 to run first.
/// </summary>
[ExampleMeta("E116", "List - Get Items", "List",
    ExampleTier.Free, 116, "List", "GetByName", "GetItems", RelatedApis = ["IListProvider.GetListByNameAsync", "IListProvider.GetListItemsAsync"])]
public class E116_ListGetByName : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Get list by name
        var list = await redb.ListProvider.GetListByNameAsync("ExampleStatuses");
        if (list == null)
        {
            sw.Stop();
            return Fail("E116", "List - Get Items", ExampleTier.Free, sw.ElapsedMilliseconds,
                "List 'ExampleStatuses' not found. Run E114 first.");
        }

        // Uncomment to see generated SQL:
        // (Lists use direct ADO.NET, no ORM query)

        // Get all items from the list
        var items = await redb.ListProvider.GetListItemsAsync(list.Id);

        sw.Stop();

        var itemsPreview = string.Join(", ", items.Take(3).Select(i => $"{i.Value}"));
        if (items.Count > 3) itemsPreview += "...";

        return Ok("E116", "List - Get Items", ExampleTier.Free, sw.ElapsedMilliseconds, items.Count,
            [$"List: {list.Name} (ID: {list.Id})", $"Items: {itemsPreview}"]);
    }
}
