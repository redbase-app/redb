using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E097_TreeQueryRoots"/>: department nodes without a parent.</summary>
[ExampleMeta("E097_ef", "EF: Tree Query - Roots Only", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E097_TreeQueryRoots_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var roots = (await db.Nodes().Where(d => d.objects._id_parent == null).ToListAsync())
            .Select(DepartmentEfMapping.ToBusiness).ToList();

        sw.Stop();

        var names = roots.Select(r => $"{r.Props.Name} ({r.Props.Code})").ToArray();
        return Ok("E097_ef", "EF: Tree Query - Roots Only", ExampleTier.Free, sw.ElapsedMilliseconds, roots.Count,
        [
            "Filter: _id_parent IS NULL",
            $"Found: {string.Join(", ", names)}"
        ]);
    }
}
