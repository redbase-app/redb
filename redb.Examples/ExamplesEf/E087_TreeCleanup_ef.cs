using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E087_TreeCleanup"/>: every department node loaded, then removed. EF has no trash,
/// so the nodes are deleted for good (the cascade on _id_parent and on the Props rows does the rest).
/// </summary>
[ExampleMeta("E087_ef", "EF: Tree Cleanup - Remove All", "Trees", ExampleTier.Free, 1, "EF", "Tree", "Delete")]
public class E087_TreeCleanup_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var existing = (await db.Nodes().ToListAsync()).Select(DepartmentEfMapping.ToBusiness).ToList();
        var count = existing.Count;
        if (count > 0)
            await db.objects.Where(o => o._id_scheme == EfSchemeIds.Department).ExecuteDeleteAsync();

        sw.Stop();

        return Ok("E087_ef", "EF: Tree Cleanup - Remove All", ExampleTier.Free, sw.ElapsedMilliseconds, count,
            [count > 0 ? $"Deleted: {count} tree nodes" : "No tree data to delete"]);
    }
}
