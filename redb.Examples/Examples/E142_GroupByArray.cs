using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Groups objects by the elements of an array nested two levels deep using GroupByArray.
/// Path: EmployeeProps -> Contacts[] -> Contact.Metadata[] -> ContactDetail.Label, flattened with SelectMany.
/// The owner filter reads two nested classes (HomeAddress.Building.Floor), each group carries a total and a
/// conditional count, a window ranks the groups, and ToSqlStringAsync shows the statement that runs.
/// </summary>
[ExampleMeta("E142", "GroupByArray - Nested Array Elements", "Analytics",
    ExampleTier.Free, 142, "GroupByArray", "Array", "Expand", "SelectMany", "Window", "Rank",
    RelatedApis = ["IRedbQueryable.GroupByArray", "IRedbGroupedQueryable.WithWindow", "Agg.Count"])]
public class E142_GroupByArray : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Employees living above the 10th floor; the metadata entries of all their contacts are grouped by label.
        var byLabel = redb.Query<EmployeeProps>()
            .Where(e => e.Contacts != null && e.HomeAddress!.Building!.Floor > 10)
            .GroupByArray(e => e.Contacts!.SelectMany(c => c.Metadata!), m => m.Label)
            .WithWindow(w => w.OrderByDesc(g => Agg.Count(g)));

        var ranked = await byLabel.SelectAsync(g => new
        {
            Label = g.Key,
            Entries = Agg.Count(g),
            CorporateDomain = Agg.Count(g, m => m.Value == "company.com"),
            Rank = Win.Rank()
        });

        sw.Stop();

        var sql = await byLabel.ToSqlStringAsync(g => new
        {
            Label = g.Key,
            Entries = Agg.Count(g),
            CorporateDomain = Agg.Count(g, m => m.Value == "company.com"),
            Rank = Win.Rank()
        });

        var output = ranked.OrderBy(g => g.Rank).ThenBy(g => g.Label)
            .Select(g => $"#{g.Rank} {g.Label}: {g.Entries} entries, {g.CorporateDomain} with value company.com")
            .Prepend($"Groups by Contacts[].Metadata[].Label, home floor > 10: {ranked.Count}")
            .Append("SQL:")
            .Concat(sql.Split('\n').Select(l => l.TrimEnd('\r')))
            .ToArray();

        return Ok("E142", "GroupByArray - Nested Array Elements", ExampleTier.Free, sw.ElapsedMilliseconds, ranked.Count, output);
    }
}
