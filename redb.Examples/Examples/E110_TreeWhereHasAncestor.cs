using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// WhereHasAncestor - polymorphic ancestor filter.
/// Find nodes that have a specific type of ancestor matching condition.
/// </summary>
[ExampleMeta("E110", "Tree Query - WhereHasAncestor", "Trees",
    ExampleTier.Free, 3, "Tree", "WhereHasAncestor", "Polymorphic", "Pro", Order = 110,
    RelatedApis = ["ITreeQueryable.WhereHasAncestor"])]
public class E110_TreeWhereHasAncestor : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Find teams that have an ancestor with Budget > 500000 (decimal test!)
        var sw = Stopwatch.StartNew();
        var teams = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code.StartsWith("TEAM-"))
            .WhereHasAncestor<DepartmentProps>(anc => anc.Budget > 500000m)
            .ToListAsync();
        sw.Stop();

        var codes = teams.Take(5).Select(t => t.Props.Code).ToArray();

        return Ok("E110", "Tree Query - WhereHasAncestor", ExampleTier.Free, sw.ElapsedMilliseconds, teams.Count,
        [
            $"Teams with rich ancestors (budget > 500k): {teams.Count}",
            $"Sample: {string.Join(", ", codes)}"
        ]);
    }
}
