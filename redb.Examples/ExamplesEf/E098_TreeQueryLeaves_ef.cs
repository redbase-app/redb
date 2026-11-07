using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E098_TreeQueryLeaves"/>: department nodes that have no children.</summary>
[ExampleMeta("E098_ef", "EF: Tree Query - Leaves Only", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E098_TreeQueryLeaves_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var leaves = (await db.Nodes().Where(d => !d.objects.Inverse_id_parentNavigation.Any()).ToListAsync())
            .Select(DepartmentEfMapping.ToBusiness).ToList();

        sw.Stop();

        var names = leaves.Select(l => $"{l.Props.Name} ({l.Props.Code})").ToArray();
        return Ok("E098_ef", "EF: Tree Query - Leaves Only", ExampleTier.Free, sw.ElapsedMilliseconds, leaves.Count,
        [
            "Filter: no children (NOT EXISTS child)",
            $"Found: {string.Join(", ", names)}"
        ]);
    }
}
