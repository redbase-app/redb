using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Creates a new RedbList (dictionary/lookup table).
/// RedbList is used to store reference data like statuses, categories, etc.
/// </summary>
[ExampleMeta("E114", "List - Create", "List",
    ExampleTier.Free, 114, "List", "Create", "Dictionary", RelatedApis = ["IListProvider.SaveListAsync", "RedbList.Create"])]
public class E114_ListCreate : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Check if list already exists and delete it (with FK cleanup)
        var existing = await redb.ListProvider.GetListByNameAsync("ExampleStatuses");
        if (existing != null)
        {
            // First delete all _values referencing list items (FK constraint)
            var items = await redb.ListProvider.GetListItemsAsync(existing.Id);
            var itemIds = items.Select(i => i.Id).ToList();
            if (itemIds.Count > 0)
            {
                await redb.Context.Bulk.BulkDeleteValuesByListItemIdsAsync(itemIds);
            }
            await redb.ListProvider.DeleteListAsync(existing.Id);
        }

        // Create a new list (dictionary)
        var statusList = RedbList.Create("ExampleStatuses", "Example Statuses");
        
        // Uncomment to see generated SQL:
        // (Lists use direct ADO.NET, no ORM query)
        
        var savedList = await redb.ListProvider.SaveListAsync(statusList);

        sw.Stop();

        return Ok("E114", "List - Create", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"List ID: {savedList.Id}", $"Name: {savedList.Name}", $"Alias: {savedList.Alias ?? "N/A"}"]);
    }
}
