using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query filtering by direct ListItem comparison.
/// Shows how to compare ListItem field with a ListItem instance directly.
/// Requires E114-E118 to run first.
/// </summary>
[ExampleMeta("E122", "ListItem - Direct Compare", "List",
    ExampleTier.Free, 122, "ListItem", "Where", "Compare", "Query", RelatedApis = ["IRedbQueryable.Where", "RedbListItem"])]
public class E122_ListItemDirectCompare : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Get a ListItem to compare against
        var list = await redb.ListProvider.GetListByNameAsync("ExampleStatuses");
        if (list == null)
        {
            sw.Stop();
            return Fail("E122", "ListItem - Direct Compare", ExampleTier.Free, sw.ElapsedMilliseconds,
                "List 'ExampleStatuses' not found. Run E114 first.");
        }

        var items = await redb.ListProvider.GetListItemsAsync(list.Id);
        var activeItem = items.FirstOrDefault(i => i.Value == "Active");
        if (activeItem == null)
        {
            sw.Stop();
            return Fail("E122", "ListItem - Direct Compare", ExampleTier.Free, sw.ElapsedMilliseconds,
                "Item 'Active' not found. Run E115 first.");
        }

        // Query persons where Status == activeItem (by ID)
        var query = redb.Query<PersonProps>()
            .Where(p => p.Status == activeItem)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();

        sw.Stop();

        return Ok("E122", "ListItem - Direct Compare", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            [$"Filter: Status == ListItem(ID:{activeItem.Id})", $"Found: {results.Count} persons"]);
    }
}
