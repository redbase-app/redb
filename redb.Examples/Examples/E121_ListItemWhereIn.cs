using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query filtering by ListItem.Value using WhereIn.
/// Shows how to filter objects by multiple ListItem values at once.
/// Requires E114-E118 to run first.
/// </summary>
[ExampleMeta("E121", "ListItem - WhereIn by Value", "List",
    ExampleTier.Free, 121, "ListItem", "WhereIn", "Value", "Query", RelatedApis = ["IRedbQueryable.WhereIn", "RedbListItem.Value"])]
public class E121_ListItemWhereIn : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Query persons where Status.Value in ["Active", "Pending"]
        var targetValues = new[] { "Active", "Pending", "Blocked" };
        var query = redb.Query<PersonProps>()
            .WhereIn(p => p.Status!.Value, targetValues)
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();

        sw.Stop();

        if (results.Count == 0)
        {
            return Ok("E121", "ListItem - WhereIn by Value", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                [$"Filter: Status.Value IN [{string.Join(", ", targetValues)}]", "No matching persons found"]);
        }

        var statuses = results.Select(r => r.Props?.Status?.Value ?? "N/A").Distinct();
        return Ok("E121", "ListItem - WhereIn by Value", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            [$"Filter: Status.Value IN [{string.Join(", ", targetValues)}]", $"Found statuses: {string.Join(", ", statuses)}"]);
    }
}
