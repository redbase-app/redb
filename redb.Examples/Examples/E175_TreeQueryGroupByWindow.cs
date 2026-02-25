using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Core.Query.Window;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates GroupBy + Window Functions on TreeQuery.
/// First groups tree nodes, then applies window functions to rank the groups.
/// Pro feature: TreeQuery + GroupBy + Window for advanced hierarchical analytics.
/// </summary>
[ExampleMeta("E175", "TreeQuery - GroupBy + Window", "Tree",
    ExampleTier.Pro, 175, "Tree", "GroupBy", "Window", "Combined", RelatedApis = ["ITreeQueryable.GroupBy", "IGroupedQueryable.WithWindow", "Win.RowNumber"])]
public class E175_TreeQueryGroupByWindow : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Ensure tree data exists (from E089_TreeCreate)
        var root = await redb.TreeQuery<DepartmentProps>().WhereRoots().FirstOrDefaultAsync();
        if (root == null)
        {
            sw.Stop();
            return Fail("E175", "TreeQuery - GroupBy + Window", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No tree data found. Please run E089_TreeCreate first.");
        }

        // GroupBy + Window: group by IsActive, then rank groups by total budget
        var groupedWindowQuery = redb.TreeQuery<DepartmentProps>(root.Id)
            .Where(d => d.Budget > 0)
            .GroupBy(d => d.IsActive)
            .WithWindow(w => w
                .OrderByDesc(g => Agg.Sum(g, d => d.Budget)));

        // Uncomment to see generated SQL:
        // var sql = await groupedWindowQuery.ToSqlStringAsync(g => new {
        //     IsActive = g.Key,
        //     TotalBudget = Agg.Sum(g, d => d.Budget),
        //     DeptCount = Agg.Count(g),
        //     Rank = Win.RowNumber()
        // });
        // Console.WriteLine(sql);

        var results = await groupedWindowQuery.SelectAsync(g => new
        {
            IsActive = g.Key,
            TotalBudget = Agg.Sum(g, d => d.Budget),
            DeptCount = Agg.Count(g),
            Rank = Win.RowNumber()
        });

        sw.Stop();

        var output = results.Select(r => 
            $"#{r.Rank} Active={r.IsActive}: {r.DeptCount} depts, Total=${r.TotalBudget:N0}").ToArray();

        return Ok("E175", "TreeQuery - GroupBy + Window", ExampleTier.Pro, sw.ElapsedMilliseconds, results.Count,
            output.Prepend($"Groups ranked by budget: {results.Count}").ToArray());
    }
}
