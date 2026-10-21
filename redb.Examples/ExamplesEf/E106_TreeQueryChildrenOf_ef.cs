using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E106_TreeQueryChildrenOf"/>: the direct children of the CORP root. The root lookup is outside the measurement.</summary>
[ExampleMeta("E106_ef", "EF: Tree Query - WhereChildrenOf", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E106_TreeQueryChildrenOf_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        await using var db = await contexts.CreateDbContextAsync();

        var rootId = await db.Nodes().Where(d => d.Code == "CORP").OrderBy(d => d._id).Select(d => (long?)d._id).FirstOrDefaultAsync();
        if (rootId == null)
            return Fail("E106_ef", "EF: Tree Query - WhereChildrenOf", ExampleTier.Free, 0, "No tree. Run E088_ef first.");

        var sw = Stopwatch.StartNew();
        var children = (await db.Nodes().Where(d => d.objects._id_parent == rootId).ToListAsync())
            .Select(DepartmentEfMapping.ToBusiness).ToList();
        sw.Stop();

        var names = children.Take(5).Select(c => c.Props.Code).ToArray();
        return Ok("E106_ef", "EF: Tree Query - WhereChildrenOf", ExampleTier.Free, sw.ElapsedMilliseconds, children.Count,
        [
            $"Direct children of CORP: {children.Count}",
            $"Codes: {string.Join(", ", names)}"
        ]);
    }
}
