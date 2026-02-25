using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Get all descendants of a node.
/// Returns all children, grandchildren, etc. recursively.
/// </summary>
[ExampleMeta("E094", "Tree Descendants - All", "Trees",
    ExampleTier.Free, 2, "Tree", "GetDescendantsAsync", "Recursive", "Pro")]
public class E094_TreeDescendants : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find TechCorp root - use TreeQuery!
        var roots = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "CORP")
            .Take(1)
            .ToListAsync();

        if (roots.Count == 0)
        {
            return Ok("E094", "Tree Descendants - All", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                ["No tree found. Run E090 first."]);
        }

        var root = (TreeRedbObject<DepartmentProps>)roots[0];

        // Get ALL descendants (children, grandchildren, etc.)
        var descendants = await redb.GetDescendantsAsync<DepartmentProps>(root);
        var list = descendants.ToList();
        sw.Stop();

        // Group by level
        var byLevel = list.GroupBy(d => d.Level).OrderBy(g => g.Key).ToList();
        var output = byLevel.Select(g => $"Level {g.Key}: {string.Join(", ", g.Select(d => d.Props.Code))}").ToList();
        output.Insert(0, $"Root: {root.Name}, Total descendants: {list.Count}");

        return Ok("E094", "Tree Descendants - All", ExampleTier.Free, sw.ElapsedMilliseconds, list.Count,
            output.ToArray());
    }
}
