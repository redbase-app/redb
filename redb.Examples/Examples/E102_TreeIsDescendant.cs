using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Extensions;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Check if node is descendant of another.
/// IsDescendantOfAsync - optimized check without loading full tree.
/// </summary>
[ExampleMeta("E102", "Tree - IsDescendantOf", "Trees",
    ExampleTier.Free, 2, "Tree", "IsDescendantOfAsync", "Pro", Order = 102)]
public class E102_TreeIsDescendant : ExampleBase
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
            return Fail("E102", "Tree - IsDescendantOf", ExampleTier.Free, 0, "No tree. Run E088 first.");

        var root = roots[0];
        var team = teams[0];

        // Check if team is descendant of root
        var sw = Stopwatch.StartNew();
        var isDescendant = await team.IsDescendantOfAsync<DepartmentProps>(root, redb);
        var isNotDescendant = await root.IsDescendantOfAsync<DepartmentProps>(team, redb);
        sw.Stop();

        return Ok("E102", "Tree - IsDescendantOf", ExampleTier.Free, sw.ElapsedMilliseconds, 2,
        [
            $"{team.Props.Code} descendant of {root.Props.Code}: {isDescendant}",
            $"{root.Props.Code} descendant of {team.Props.Code}: {isNotDescendant}"
        ]);
    }
}
