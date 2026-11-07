using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Find root nodes in tree.
/// WhereRoots returns only top-level nodes (parent_id = null).
/// </summary>
[ExampleMeta("E097", "Tree Query - Roots Only", "Trees",
    ExampleTier.Free, 2, "Tree", "TreeQuery", "WhereRoots", "Pro",
    RelatedApis = ["ITreeQueryable.WhereRoots", "IRedbService.TreeQuery"])]
public class E097_TreeQueryRoots : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find all root nodes
        var roots = await redb.TreeQuery<DepartmentProps>()
            .WhereRoots()
            .ToListAsync();

        sw.Stop();

        var names = roots.Select(r => $"{r.Name} ({r.Props.Code})").ToArray();

        return Ok("E097", "Tree Query - Roots Only", ExampleTier.Free, sw.ElapsedMilliseconds, roots.Count,
        [
            "Filter: WhereRoots() - parent_id IS NULL",
            $"Found: {string.Join(", ", names)}"
        ]);
    }
}
