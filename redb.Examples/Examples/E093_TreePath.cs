using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Get path from node to root.
/// Builds breadcrumbs: Team > Department > Office > TechCorp.
/// </summary>
[ExampleMeta("E093", "Tree Path - Breadcrumbs", "Trees",
    ExampleTier.Free, 2, "Tree", "GetPathToRootAsync", "Breadcrumbs", "Pro")]
public class E093_TreePath : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find deepest node (any team at level 3) - use TreeQuery!
        var nodes = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code.StartsWith("TEAM-"))
            .Take(1)
            .ToListAsync();

        if (nodes.Count == 0)
        {
            return Ok("E093", "Tree Path - Breadcrumbs", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                ["No tree found. Run E088 or E089 first."]);
        }

        var node = (TreeRedbObject<DepartmentProps>)nodes[0];

        // Get path to root (returns: DevAlpha, IT-MSK, Moscow, TechCorp)
        var path = await redb.GetPathToRootAsync<DepartmentProps>(node);
        var pathList = path.ToList();
        sw.Stop();

        var breadcrumbs = string.Join(" > ", pathList.Select(n => n.Name));

        return Ok("E093", "Tree Path - Breadcrumbs", ExampleTier.Free, sw.ElapsedMilliseconds, pathList.Count,
        [
            $"From: {node.name}",
            $"Path: {breadcrumbs}",
            $"Levels: {pathList.Count}"
        ]);
    }
}
