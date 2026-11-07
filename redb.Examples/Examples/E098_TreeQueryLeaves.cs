using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Find leaf nodes in tree.
/// WhereLeaves returns nodes with no children.
/// </summary>
[ExampleMeta("E098", "Tree Query - Leaves Only", "Trees",
    ExampleTier.Free, 2, "Tree", "TreeQuery", "WhereLeaves", "Pro")]
public class E098_TreeQueryLeaves : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find all leaf nodes (no children)
        var leaves = await redb.TreeQuery<DepartmentProps>()
            .WhereLeaves()
            .ToListAsync();

        sw.Stop();

        var names = leaves.Select(l => $"{l.Name} ({l.Props.Code})").ToArray();

        return Ok("E098", "Tree Query - Leaves Only", ExampleTier.Free, sw.ElapsedMilliseconds, leaves.Count,
        [
            "Filter: WhereLeaves() - no children",
            $"Found: {string.Join(", ", names)}"
        ]);
    }
}
