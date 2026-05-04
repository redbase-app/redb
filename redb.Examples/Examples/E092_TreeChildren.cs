using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Get direct children of a node.
/// Uses GetChildrenAsync - returns only immediate children, not descendants.
/// </summary>
[ExampleMeta("E092", "Tree Children - Direct", "Trees",
    ExampleTier.Free, 2, "Tree", "GetChildrenAsync", "Pro")]
public class E092_TreeChildren : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find TechCorp root - use TreeQuery for tree objects!
        var roots = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "CORP")
            .Take(1)
            .ToListAsync();

        if (roots.Count == 0)
        {
            return Ok("E092", "Tree Children - Direct", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                ["No tree found. Run E090 first."]);
        }

        var root = (TreeRedbObject<DepartmentProps>)roots[0];

        // Get direct children only
        var children = await redb.GetChildrenAsync<DepartmentProps>(root);
        var childList = children.ToList();
        sw.Stop();

        var names = childList.Select(c => c.Name).ToArray();

        return Ok("E092", "Tree Children - Direct", ExampleTier.Free, sw.ElapsedMilliseconds, childList.Count,
        [
            $"Parent: {root.Name} ({root.Props.Code})",
            $"Children: {string.Join(", ", names)}"
        ]);
    }
}
