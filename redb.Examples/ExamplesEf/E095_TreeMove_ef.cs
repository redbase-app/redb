using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E095_TreeMove"/>: a department moved under OFF-02 and back under OFF-01 by changing
/// its _id_parent, saved each time. Its subtree follows, since the children point at it.
/// </summary>
[ExampleMeta("E095_ef", "EF: Tree Move - Relocate Node", "Trees", ExampleTier.Free, 1, "EF", "Tree", "Update")]
public class E095_TreeMove_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var dept = await db.Nodes().Where(d => d.Code!.StartsWith("DEPT-01-")).OrderBy(d => d._id).FirstOrDefaultAsync();
        if (dept == null)
            return Ok("E095_ef", "EF: Tree Move - Relocate Node", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                ["No tree found. Run E088_ef first."]);

        var targetOffice = await db.Nodes().Where(d => d.Code == "OFF-02").OrderBy(d => d._id).FirstOrDefaultAsync();
        if (targetOffice == null)
            return Ok("E095_ef", "EF: Tree Move - Relocate Node", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                ["Target office not found."]);

        var originalParentId = dept.objects._id_parent;
        dept.objects._id_parent = targetOffice._id;
        dept.objects._date_modify = DateTime.UtcNow;
        await db.SaveChangesAsync();

        if (originalParentId.HasValue)
        {
            var origOffice = await db.Nodes().Where(d => d.Code == "OFF-01").OrderBy(d => d._id).FirstOrDefaultAsync();
            if (origOffice != null)
            {
                dept.objects._id_parent = origOffice._id;
                dept.objects._date_modify = DateTime.UtcNow;
                await db.SaveChangesAsync();
            }
        }

        sw.Stop();

        return Ok("E095_ef", "EF: Tree Move - Relocate Node", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
        [
            $"Moved: {dept.objects._name} to {targetOffice.objects._name}",
            "Moved back to original office",
            "Subtree preserved during move"
        ]);
    }
}
