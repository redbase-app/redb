using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E091_TreeCount"/>: the count of department nodes.</summary>
[ExampleMeta("E091_ef", "EF: Tree Count - CountAsync", "Trees", ExampleTier.Free, 1, "EF", "Tree", "Count")]
public class E091_TreeCount_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var count = await db.DepartmentProps.CountAsync();

        sw.Stop();

        if (count == 0)
            return Fail("E091_ef", "EF: Tree Count - CountAsync", ExampleTier.Free, sw.ElapsedMilliseconds, "No tree. Run E088_ef first.");

        return Ok("E091_ef", "EF: Tree Count - CountAsync", ExampleTier.Free, sw.ElapsedMilliseconds, count,
            ["DepartmentProps.CountAsync()", $"Total: {count} nodes"]);
    }
}
