using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Creates a ListItem with linked RedbObject.
/// ListItem can reference any object via IdObject property.
/// Linked object is loaded lazily via Object property.
/// Requires E114+E115 to run first.
/// </summary>
[ExampleMeta("E117", "ListItem - With Object", "List",
    ExampleTier.Free, 117, "ListItem", "Object", "Reference", RelatedApis = ["IListProvider.SaveListItemAsync", "RedbListItem.Object"])]
public class E117_ListItemWithObject : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Get existing list
        var list = await redb.ListProvider.GetListByNameAsync("ExampleStatuses");
        if (list == null)
        {
            sw.Stop();
            return Fail("E117", "ListItem - With Object", ExampleTier.Free, sw.ElapsedMilliseconds,
                "List 'ExampleStatuses' not found. Run E114 first.");
        }

        // Sync City scheme and create a city object
        await redb.SyncSchemeAsync<CityProps>();
        var cityObj = new RedbObject<CityProps>
        {
            name = "Moscow",
            Props = new CityProps
            {
                Name = "Moscow",
                Population = 12_655_000,
                Region = "Central Federal District",
                IsCapital = true,
                Coordinates = [55.7558, 37.6173]
            }
        };
        var cityId = await redb.SaveAsync(cityObj);

        // Create a new list item with object reference
        var item = new RedbListItem(list, "Capital", "Capital city status", cityObj);

        // Uncomment to see generated SQL:
        // (Lists use direct ADO.NET, no ORM query)

        var savedItem = await redb.ListProvider.SaveListItemAsync(item);

        sw.Stop();

        return Ok("E117", "ListItem - With Object", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Item: {savedItem.Value} (ID: {savedItem.Id})", $"Linked object ID: {savedItem.IdObject}"]);
    }
}
