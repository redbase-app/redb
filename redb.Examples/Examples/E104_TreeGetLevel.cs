using System.Diagnostics;
using redb.Core;
using redb.Core.Extensions;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Get tree level of a node.
/// GetTreeLevelAsync - counts levels from root (root = 0).
/// </summary>
[ExampleMeta("E104", "Tree - GetLevel", "Trees",
    ExampleTier.Free, 2, "Tree", "GetTreeLevelAsync", "Pro", Order = 104)]
public class E104_TreeGetLevel : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Find nodes at different levels
        var roots = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "CORP")
            .Take(1)
            .ToListAsync();

        var offices = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "OFF-01")
            .Take(1)
            .ToListAsync();

        var teams = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code.StartsWith("TEAM-"))
            .Take(1)
            .ToListAsync();

        if (roots.Count == 0)
            return Fail("E104", "Tree - GetLevel", ExampleTier.Free, 0, "No tree. Run E088 first.");

        var sw = Stopwatch.StartNew();
        var rootLevel = await roots[0].GetTreeLevelAsync<DepartmentProps>(redb);
        var officeLevel = offices.Count > 0 ? await offices[0].GetTreeLevelAsync<DepartmentProps>(redb) : -1;
        var teamLevel = teams.Count > 0 ? await teams[0].GetTreeLevelAsync<DepartmentProps>(redb) : -1;
        sw.Stop();

        return Ok("E104", "Tree - GetLevel", ExampleTier.Free, sw.ElapsedMilliseconds, 3,
        [
            $"Root (CORP): level {rootLevel}",
            $"Office (OFF-01): level {officeLevel}",
            $"Team: level {teamLevel}"
        ]);
    }
}
