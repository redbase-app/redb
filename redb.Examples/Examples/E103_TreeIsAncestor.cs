using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Extensions;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Check if node is ancestor of another.
/// IsAncestorOfAsync - checks if this node is parent/grandparent of target.
/// </summary>
[ExampleMeta("E103", "Tree - IsAncestorOf", "Trees",
    ExampleTier.Free, 2, "Tree", "IsAncestorOfAsync", "Pro", Order = 103)]
public class E103_TreeIsAncestor : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Find root and a deep node
        var roots = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "CORP")
            .Take(1)
            .ToListAsync();

        var teams = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code.StartsWith("TEAM-"))
            .Take(1)
            .ToListAsync();

        if (roots.Count == 0 || teams.Count == 0)
            return Fail("E103", "Tree - IsAncestorOf", ExampleTier.Free, 0, "No tree. Run E088 first.");

        var root = roots[0];
        var team = teams[0];

        // Check if root is ancestor of team
        var sw = Stopwatch.StartNew();
        var isAncestor = await root.IsAncestorOfAsync<DepartmentProps>(team, redb);
        var isNotAncestor = await team.IsAncestorOfAsync<DepartmentProps>(root, redb);
        sw.Stop();

        return Ok("E103", "Tree - IsAncestorOf", ExampleTier.Free, sw.ElapsedMilliseconds, 2,
        [
            $"{root.Props.Code} ancestor of {team.Props.Code}: {isAncestor}",
            $"{team.Props.Code} ancestor of {root.Props.Code}: {isNotAncestor}"
        ]);
    }
}
