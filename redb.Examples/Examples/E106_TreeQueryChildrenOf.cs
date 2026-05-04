using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Query direct children of a node.
/// WhereChildrenOf(parentId) - returns only immediate children.
/// </summary>
[ExampleMeta("E106", "Tree Query - WhereChildrenOf", "Trees",
    ExampleTier.Free, 2, "Tree", "TreeQuery", "WhereChildrenOf", "Pro", Order = 106)]
public class E106_TreeQueryChildrenOf : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Find root
        var roots = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "CORP")
            .Take(1)
            .ToListAsync();

        if (roots.Count == 0)
            return Fail("E106", "Tree Query - WhereChildrenOf", ExampleTier.Free, 0, "No tree. Run E088 first.");

        var rootId = roots[0].Id;

        // Get direct children only
        var sw = Stopwatch.StartNew();
        var children = await redb.TreeQuery<DepartmentProps>()
            .WhereChildrenOf(rootId)
            .ToListAsync();
        sw.Stop();

        var names = children.Take(5).Select(c => c.Props.Code).ToArray();

        return Ok("E106", "Tree Query - WhereChildrenOf", ExampleTier.Free, sw.ElapsedMilliseconds, children.Count,
        [
            $"Direct children of CORP: {children.Count}",
            $"Codes: {string.Join(", ", names)}"
        ]);
    }
}
