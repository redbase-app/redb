using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// ToTreeListAsync with Parent chain.
/// Returns nodes with Parent property populated up to root.
/// </summary>
[ExampleMeta("E108", "Tree Query - ToTreeListAsync", "Trees",
    ExampleTier.Free, 3, "Tree", "ToTreeListAsync", "ParentChain", "Pro", Order = 108)]
public class E108_TreeToTreeList : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Query teams (deep nodes)
        var sw = Stopwatch.StartNew();
        var teams = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code.StartsWith("TEAM-"))
            .Take(5)
            .ToTreeListAsync();
        sw.Stop();

        if (teams.Count == 0)
            return Fail("E108", "Tree Query - ToTreeListAsync", ExampleTier.Free, sw.ElapsedMilliseconds, "No tree. Run E088 first.");

        // Check parent chain
        var first = teams[0];
        var parentDepth = 0;
        var current = first.Parent;
        while (current != null && parentDepth < 10)
        {
            parentDepth++;
            current = (current as TreeRedbObject<DepartmentProps>)?.Parent;
        }

        return Ok("E108", "Tree Query - ToTreeListAsync", ExampleTier.Free, sw.ElapsedMilliseconds, teams.Count,
        [
            $"Loaded: {teams.Count} teams with Parent chain",
            $"First team parent depth: {parentDepth} levels to root"
        ]);
    }
}
