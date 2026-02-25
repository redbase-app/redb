using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query filtering by ListItem array using Any().
/// Shows how to filter objects where any element in ListItem array matches condition.
/// Requires E114-E119 to run first.
/// </summary>
[ExampleMeta("E123", "ListItem Array - Any", "List",
    ExampleTier.Free, 123, "ListItem", "Array", "Any", "Query", RelatedApis = ["IRedbQueryable.Where", "List<RedbListItem>.Any"])]
public class E123_ListItemArrayAny : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Query persons where any Role has Value == "Active"
        var query = redb.Query<PersonProps>()
            .Where(p => p.Roles!.Any(r => r.Value == "Active"))
            .Take(100);

        // Uncomment to see generated SQL:
        // var sql = await query.ToSqlStringAsync();
        // Console.WriteLine(sql);

        var results = await query.ToListAsync();

        sw.Stop();

        if (results.Count == 0)
        {
            return Ok("E123", "ListItem Array - Any", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                ["Filter: Roles.Any(r => r.Value == 'Active')", "No matching persons found. Run E119 first."]);
        }

        var firstName = results.FirstOrDefault()?.Props?.Name ?? "N/A";
        var rolesCount = results.FirstOrDefault()?.Props?.Roles?.Count ?? 0;
        return Ok("E123", "ListItem Array - Any", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            [$"Filter: Roles.Any(r => r.Value == 'Active')", $"Found: {results.Count}, First: {firstName} ({rolesCount} roles)"]);
    }
}
