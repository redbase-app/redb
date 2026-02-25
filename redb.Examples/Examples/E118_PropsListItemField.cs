using System.Diagnostics;
using redb.Core;
using redb.Core.Caching;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Object with ListItem field in Props.
/// Shows how to store reference to list item in object properties.
/// Requires E114+E115 to run first.
/// </summary>
[ExampleMeta("E118", "Props - ListItem Field", "List",
    ExampleTier.Free, 118, "Props", "ListItem", "Field", RelatedApis = ["RedbListItem"])]
public class E118_PropsListItemField : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Get existing list
        var list = await redb.ListProvider.GetListByNameAsync("ExampleStatuses");
        if (list == null)
        {
            sw.Stop();
            return Fail("E118", "Props - ListItem Field", ExampleTier.Free, sw.ElapsedMilliseconds,
                "List 'ExampleStatuses' not found. Run E114 first.");
        }

        // Get list items
        var items = await redb.ListProvider.GetListItemsAsync(list.Id);
        var activeStatus = items.FirstOrDefault(i => i.Value == "Active");
        if (activeStatus == null)
        {
            sw.Stop();
            return Fail("E118", "Props - ListItem Field", ExampleTier.Free, sw.ElapsedMilliseconds,
                "Item 'Active' not found. Run E115 first.");
        }

        // Sync Person scheme
        await redb.SyncSchemeAsync<PersonProps>();

        // Create person with Status field (ListItem)
        var person = new RedbObject<PersonProps>
        {
            name = "John Doe",
            Props = new PersonProps
            {
                Name = "John Doe",
                Age = 30,
                Email = "john@example.com",
                Status = activeStatus  // Single ListItem field
            }
        };

        // Uncomment to see generated SQL:
        // (Save uses COPY protocol for Pro)

        var personId = await redb.SaveAsync(person);

        // Load back to verify
        redb.PropsCache.Clear();
        var loaded = await redb.LoadAsync<PersonProps>(personId);

        sw.Stop();

        var statusValue = loaded?.Props?.Status?.Value ?? "N/A";
        return Ok("E118", "Props - ListItem Field", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Person ID: {personId}", $"Status: {statusValue}"]);
    }
}
