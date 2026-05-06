using System.Diagnostics;
using redb.Core;
using redb.Core.Query.Aggregation;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates Window Functions on TreeQuery.
/// Calculates ROW_NUMBER and running totals within tree hierarchy.
/// Pro feature: TreeQuery + Window for hierarchical rankings and cumulative values.
/// </summary>
[ExampleMeta("E174", "TreeQuery - Window Functions", "Tree",
    ExampleTier.Free, 174, "Tree", "Window", "RowNumber", "RunningSum", RelatedApis = ["ITreeQueryable.WithWindow", "Win.RowNumber", "Win.Sum"])]
public class E174_TreeQueryWindow : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Ensure tree data exists (from E089_TreeCreate)
        var root = await redb.TreeQuery<DepartmentProps>().WhereRoots().FirstOrDefaultAsync();
        if (root == null)
        {
            sw.Stop();
            return Fail("E174", "TreeQuery - Window Functions", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No tree data found. Please run E089_TreeCreate first.");
        }

        // Window functions on tree: rank departments by budget within the hierarchy
        var windowQuery = redb.TreeQuery<DepartmentProps>(root.Id)
            .Where(d => d.Budget > 0)
            .Take(20)
            .WithWindow(w => w
                .PartitionBy(d => d.IsActive)
                .OrderByDesc(d => d.Budget));

        // Uncomment to see generated SQL:
        // var sql = await windowQuery.ToSqlStringAsync(d => new {
        //     d.Props.Name,
        //     d.Props.Budget,
        //     Rank = Win.RowNumber(),
        //     RunningBudget = Win.Sum(d.Props.Budget)
        // });
        // Console.WriteLine(sql);

        var results = await windowQuery.SelectAsync(d => new
        {
            Name = d.Props.Name,
            Budget = d.Props.Budget,
            IsActive = d.Props.IsActive,
            Rank = Win.RowNumber(),
            RunningBudget = Win.Sum(d.Props.Budget)
        });

        sw.Stop();

        var output = results.Take(5).Select(r => 
            $"#{r.Rank} {r.Name}: ${r.Budget:N0} (Running: ${r.RunningBudget:N0})").ToArray();

        return Ok("E174", "TreeQuery - Window Functions", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
            output.Prepend($"Total: {results.Count} departments ranked").ToArray());
    }
}
