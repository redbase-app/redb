using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Utils;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Breadth-First traversal of tree.
/// Visits nodes level by level: Root -> All Level1 -> All Level2...
/// 
/// Key method:
/// - **tree.BreadthFirstTraversal()** - iterate nodes breadth-first
/// </summary>
[ExampleMeta("E101", "Tree Traversal - BFS", "Trees",
    ExampleTier.Free, 2, "Tree", "BreadthFirstTraversal", "BFS", "Pro", Order = 101)]
public class E101_TreeTraversalBFS : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Find root
        var roots = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "CORP")
            .Take(1)
            .ToListAsync();

        if (roots.Count == 0)
            return Fail("E101", "Tree Traversal - BFS", ExampleTier.Free, 0, "No tree. Run E088 first.");

        var root = (TreeRedbObject<DepartmentProps>)roots[0];

        // Load full tree with Children populated
        var tree = await redb.LoadTreeAsync<DepartmentProps>(root, maxDepth: 5);

        // Measure BFS traversal (cast to interface for extension method)
        var sw = Stopwatch.StartNew();
        var visited = ((ITreeRedbObject<DepartmentProps>)tree).BreadthFirstTraversal().ToList();
        sw.Stop();

        // Group by level
        var byLevel = visited.GroupBy(n => n.Level).OrderBy(g => g.Key);
        var levelCounts = byLevel.Select(g => $"L{g.Key}:{g.Count()}").ToArray();

        return Ok("E101", "Tree Traversal - BFS", ExampleTier.Free, sw.ElapsedMilliseconds, visited.Count,
            [$"BFS by level: {string.Join(", ", levelCounts)}", $"Total visited: {visited.Count}"]);
    }
}
