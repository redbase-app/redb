using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query filtering by ListItem.Value property.
/// Shows how to filter objects by the Value of a ListItem field.
/// Requires E114-E118 to run first.
/// </summary>
[ExampleMeta("E120", "ListItem - Where by Value", "List",
    ExampleTier.Free, 120, "ListItem", "Where", "Value", "Query", RelatedApis = ["IRedbQueryable.Where", "RedbListItem.Value"])]
public class E120_ListItemWhereByValue : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Query persons where Status.Value == "Active"
        var query = redb.Query<PersonProps>()
            .Where(p => p.Status!.Value == "Active")
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();

        sw.Stop();

        if (results.Count == 0)
        {
            return Ok("E120", "ListItem - Where by Value", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                ["No persons with Status='Active' found", "Run E118 first to create test data"]);
        }

        var firstName = results.FirstOrDefault()?.Props?.Name ?? "N/A";
        return Ok("E120", "ListItem - Where by Value", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            [$"Filter: Status.Value == 'Active'", $"Found: {results.Count}, First: {firstName}"]);
    }
}
