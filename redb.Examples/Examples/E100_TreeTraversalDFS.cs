using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Utils;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Depth-First traversal of tree.
/// Visits nodes: Root -> Child1 -> Grandchild1 -> Grandchild2 -> Child2...
/// 
/// Key method:
/// - **tree.DepthFirstTraversal()** - iterate nodes depth-first
/// </summary>
[ExampleMeta("E100", "Tree Traversal - DFS", "Trees",
    ExampleTier.Free, 2, "Tree", "DepthFirstTraversal", "DFS", "Pro", Order = 100)]
public class E100_TreeTraversalDFS : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Find root
        var roots = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "CORP")
            .Take(1)
            .ToListAsync();

        if (roots.Count == 0)
            return Fail("E100", "Tree Traversal - DFS", ExampleTier.Free, 0, "No tree. Run E088 first.");

        var root = (TreeRedbObject<DepartmentProps>)roots[0];

        // Load full tree with Children populated
        var tree = await redb.LoadTreeAsync<DepartmentProps>(root, maxDepth: 5);

        // Measure DFS traversal (cast to interface for extension method)
        var sw = Stopwatch.StartNew();
        var visited = ((ITreeRedbObject<DepartmentProps>)tree).DepthFirstTraversal().ToList();
        sw.Stop();

        var sample = visited.Take(5).Select(n => n.Props.Code).ToArray();

        return Ok("E100", "Tree Traversal - DFS", ExampleTier.Free, sw.ElapsedMilliseconds, visited.Count,
            [$"DFS order (first 5): {string.Join(" -> ", sample)}", $"Total visited: {visited.Count}"]);
    }
}
