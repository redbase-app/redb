using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// WhereHasDescendant - find parents by child criteria.
/// Find offices that have active departments beneath them.
/// </summary>
[ExampleMeta("E111", "Tree Query - WhereHasDescendant", "Trees",
    ExampleTier.Free, 3, "Tree", "WhereHasDescendant", "Polymorphic", "Pro", Order = 111)]
public class E111_TreeWhereHasDescendant : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Find offices that have descendants with Budget > 100000 (decimal test!)
        var sw = Stopwatch.StartNew();
        var offices = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code.StartsWith("OFF-"))
            .WhereHasDescendant<DepartmentProps>(desc => desc.Budget > 100000m)
            .ToListAsync();
        sw.Stop();

        var codes = offices.Take(5).Select(o => o.Props.Code).ToArray();

        return Ok("E111", "Tree Query - WhereHasDescendant", ExampleTier.Free, sw.ElapsedMilliseconds, offices.Count,
        [
            $"Offices with rich descendants (budget > 100k): {offices.Count}",
            $"Sample: {string.Join(", ", codes)}"
        ]);
    }
}
