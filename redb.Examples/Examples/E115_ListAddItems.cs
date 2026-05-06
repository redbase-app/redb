using System.Diagnostics;
using redb.Core;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Adds multiple items to a RedbList using AddItemsAsync.
/// Batch operation for efficient bulk insert of list items.
/// Requires E114 to run first (creates the list).
/// </summary>
[ExampleMeta("E115", "List - Add Items", "List",
    ExampleTier.Free, 115, "List", "AddItems", "Batch", RelatedApis = ["IListProvider.AddItemsAsync"])]
public class E115_ListAddItems : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Get existing list (created in E114)
        var list = await redb.ListProvider.GetListByNameAsync("ExampleStatuses");
        if (list == null)
        {
            sw.Stop();
            return Fail("E115", "List - Add Items", ExampleTier.Free, sw.ElapsedMilliseconds,
                "List 'ExampleStatuses' not found. Run E114 first.");
        }

        // Prepare values and aliases
        var values = new List<string> { "Active", "Inactive", "Pending", "Blocked" };
        var aliases = new List<string> { "Active status", "Inactive status", "Pending review", "Blocked access" };

        // Uncomment to see generated SQL:
        // (Lists use direct ADO.NET, no ORM query)

        // Batch add items
        var addedItems = await redb.ListProvider.AddItemsAsync(list, values, aliases);

        sw.Stop();

        var itemsList = string.Join(", ", addedItems.Select(i => i.Value));
        return Ok("E115", "List - Add Items", ExampleTier.Free, sw.ElapsedMilliseconds, addedItems.Count,
            [$"Added: {addedItems.Count} items", $"Values: {itemsList}"]);
    }
}
