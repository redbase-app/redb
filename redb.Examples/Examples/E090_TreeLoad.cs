using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Load full tree with LoadTreeAsync.
/// 
/// **Requires E089 to run first (creates tree data).**
/// 
/// Loads entire hierarchy from root with specified depth.
/// </summary>
[ExampleMeta("E090", "Tree Load - Full Hierarchy", "Trees",
    ExampleTier.Free, 2, "Tree", "LoadTreeAsync", "Hierarchy", "Pro", Order = 90,
    RelatedApis = ["IRedbService.LoadTreeAsync", "TreeRedbObject"])]
public class E090_TreeLoad : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Find root
        var roots = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "CORP")
            .Take(1)
            .ToListAsync();

        if (roots.Count == 0)
            return Fail("E090", "Tree Load - Full Hierarchy", ExampleTier.Free, 0, "No tree. Run E089 first.");

        var root = (TreeRedbObject<DepartmentProps>)roots[0];

        // Measure LoadTreeAsync
        var sw = Stopwatch.StartNew();
        var tree = await redb.LoadTreeAsync<DepartmentProps>(root, maxDepth: 5);
        sw.Stop();

        var nodeCount = CountNodes(tree);

        return Ok("E090", "Tree Load - Full Hierarchy", ExampleTier.Free, sw.ElapsedMilliseconds, nodeCount,
            [$"Root: {tree.name}, Children: {tree.Children.Count}", $"Total nodes loaded: {nodeCount}"]);
    }

    private static int CountNodes(ITreeRedbObject<DepartmentProps> node)
    {
        var count = 1;
        foreach (var child in node.Children.OfType<ITreeRedbObject<DepartmentProps>>())
            count += CountNodes(child);
        return count;
    }
}
