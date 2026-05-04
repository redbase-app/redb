using System.Diagnostics;
using redb.Core;
using redb.Core.Caching;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Object with List of ListItems field in Props.
/// Shows how to store multiple list item references (e.g. roles, tags).
/// Requires E114+E115 to run first.
/// </summary>
[ExampleMeta("E119", "Props - List<ListItem>", "List",
    ExampleTier.Free, 119, "Props", "ListItem", "Array", "Roles", RelatedApis = ["List<RedbListItem>"])]
public class E119_PropsListItemArray : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Get existing list
        var list = await redb.ListProvider.GetListByNameAsync("ExampleStatuses");
        if (list == null)
        {
            sw.Stop();
            return Fail("E119", "Props - List<ListItem>", ExampleTier.Free, sw.ElapsedMilliseconds,
                "List 'ExampleStatuses' not found. Run E114 first.");
        }

        // Get list items
        var items = await redb.ListProvider.GetListItemsAsync(list.Id);
        if (items.Count < 2)
        {
            sw.Stop();
            return Fail("E119", "Props - List<ListItem>", ExampleTier.Free, sw.ElapsedMilliseconds,
                "Need at least 2 items. Run E115 first.");
        }

        // Sync Person scheme
        await redb.SyncSchemeAsync<PersonProps>();

        // Create person with Roles field (List<ListItem>)
        var person = new RedbObject<PersonProps>
        {
            name = "Jane Smith",
            Props = new PersonProps
            {
                Name = "Jane Smith",
                Age = 28,
                Email = "jane@example.com",
                Roles = items.Take(3).ToList()  // Multiple ListItem references
            }
        };

        // Uncomment to see generated SQL:
        // (Save uses COPY protocol for Pro)

        var personId = await redb.SaveAsync(person);

        // Load back to verify
        redb.PropsCache.Clear();
        var loaded = await redb.LoadAsync<PersonProps>(personId);

        sw.Stop();

        var rolesCount = loaded?.Props?.Roles?.Count ?? 0;
        var rolesList = loaded?.Props?.Roles != null 
            ? string.Join(", ", loaded.Props.Roles.Select(r => r.Value))
            : "N/A";

        return Ok("E119", "Props - List<ListItem>", ExampleTier.Free, sw.ElapsedMilliseconds, rolesCount,
            [$"Person ID: {personId}", $"Roles ({rolesCount}): {rolesList}"]);
    }
}
