using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E092_TreeChildren"/>: the CORP root and its direct children (_id_parent equal to the root).</summary>
[ExampleMeta("E092_ef", "EF: Tree Children - Direct", "Trees", ExampleTier.Free, 1, "EF", "Tree")]
public class E092_TreeChildren_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var rootEntity = await db.Nodes().Where(d => d.Code == "CORP").OrderBy(d => d._id).FirstOrDefaultAsync();
        if (rootEntity == null)
            return Ok("E092_ef", "EF: Tree Children - Direct", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                ["No tree found. Run E088_ef first."]);

        var root = DepartmentEfMapping.ToBusiness(rootEntity);
        var childList = (await db.Nodes().Where(d => d.objects._id_parent == root.id).ToListAsync())
            .Select(DepartmentEfMapping.ToBusiness).ToList();

        sw.Stop();

        var names = childList.Select(c => c.name).ToArray();
        return Ok("E092_ef", "EF: Tree Children - Direct", ExampleTier.Free, sw.ElapsedMilliseconds, childList.Count,
        [
            $"Parent: {root.name} ({root.Props.Code})",
            $"Children: {string.Join(", ", names)}"
        ]);
    }
}
