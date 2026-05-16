using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// ToRootListAsync - get full tree structure.
/// Returns root nodes with Children populated recursively.
/// </summary>
[ExampleMeta("E109", "Tree Query - ToRootListAsync", "Trees",
    ExampleTier.Free, 3, "Tree", "ToRootListAsync", "FullTree", "Pro", Order = 109)]
public class E109_TreeToRootList : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Get full tree from roots
        var sw = Stopwatch.StartNew();
        var rootTrees = await redb.TreeQuery<DepartmentProps>()
            .WhereRoots()
            .ToRootListAsync();
        sw.Stop();

        if (rootTrees.Count == 0)
            return Fail("E109", "Tree Query - ToRootListAsync", ExampleTier.Free, sw.ElapsedMilliseconds, "No tree. Run E088 first.");

        // Count total nodes in tree
        var totalNodes = CountNodes(rootTrees[0]);

        return Ok("E109", "Tree Query - ToRootListAsync", ExampleTier.Free, sw.ElapsedMilliseconds, totalNodes,
        [
            $"Root trees: {rootTrees.Count}",
            $"First tree total nodes: {totalNodes}"
        ]);
    }

    private static int CountNodes(ITreeRedbObject node)
    {
        var count = 1;
        foreach (var child in node.Children)
            count += CountNodes(child);
        return count;
    }
}
