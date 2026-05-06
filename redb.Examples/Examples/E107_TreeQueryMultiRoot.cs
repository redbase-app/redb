using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// TreeQuery with multiple root parents.
/// TreeQuery(parents[], maxDepth) - queries across multiple subtrees.
/// </summary>
[ExampleMeta("E107", "Tree Query - Multiple Roots", "Trees",
    ExampleTier.Free, 3, "Tree", "TreeQuery", "MultiRoot", "Pro", Order = 107)]
public class E107_TreeQueryMultiRoot : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Find two offices to use as roots
        var office1 = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "OFF-01")
            .Take(1)
            .ToListAsync();

        var office2 = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "OFF-02")
            .Take(1)
            .ToListAsync();

        if (office1.Count == 0 || office2.Count == 0)
            return Fail("E107", "Tree Query - Multiple Roots", ExampleTier.Free, 0, "No tree. Run E088 first.");

        var parents = new IRedbObject[] { office1[0], office2[0] };

        // Query across both subtrees
        var sw = Stopwatch.StartNew();
        var results = await redb.TreeQuery<DepartmentProps>(parents, maxDepth: 3)
            .ToListAsync();
        sw.Stop();

        var byParent = results.GroupBy(r => r.ParentId).Select(g => $"{g.Key}:{g.Count()}").ToArray();

        return Ok("E107", "Tree Query - Multiple Roots", ExampleTier.Free, sw.ElapsedMilliseconds, results.Count,
        [
            $"Nodes in OFF-01 + OFF-02 subtrees: {results.Count}",
            $"By parent: {string.Join(", ", byParent.Take(3))}"
        ]);
    }
}
