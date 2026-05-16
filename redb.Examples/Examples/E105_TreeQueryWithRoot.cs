using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// TreeQuery scoped to specific root.
/// TreeQuery(rootId, maxDepth) - queries only within subtree.
/// </summary>
[ExampleMeta("E105", "Tree Query - Scoped to Root", "Trees",
    ExampleTier.Free, 2, "Tree", "TreeQuery", "RootId", "Pro", Order = 105)]
public class E105_TreeQueryWithRoot : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Find an office to use as root
        var offices = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "OFF-01")
            .Take(1)
            .ToListAsync();

        if (offices.Count == 0)
            return Fail("E105", "Tree Query - Scoped to Root", ExampleTier.Free, 0, "No tree. Run E088 first.");

        var officeId = offices[0].Id;

        // Query only within this office's subtree
        var sw = Stopwatch.StartNew();
        var subtreeNodes = await redb.TreeQuery<DepartmentProps>(officeId, maxDepth: 3)
            .ToListAsync();
        sw.Stop();

        var codes = subtreeNodes.Take(5).Select(n => n.Props.Code).ToArray();

        return Ok("E105", "Tree Query - Scoped to Root", ExampleTier.Free, sw.ElapsedMilliseconds, subtreeNodes.Count,
        [
            $"Subtree of OFF-01: {subtreeNodes.Count} nodes",
            $"Sample: {string.Join(", ", codes)}"
        ]);
    }
}
