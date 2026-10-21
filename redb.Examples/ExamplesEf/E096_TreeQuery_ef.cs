using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E096_TreeQuery"/>: department nodes whose name contains "IT", ordered by name.</summary>
[ExampleMeta("E096_ef", "EF: Tree Query - LINQ on Trees", "Trees", ExampleTier.Free, 1, "EF", "Tree", "Where")]
public class E096_TreeQuery_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var itDepts = (await db.Nodes().Where(d => d.Name!.Contains("IT")).OrderBy(d => d.Name).ToListAsync())
            .Select(DepartmentEfMapping.ToBusiness).ToList();

        sw.Stop();

        var names = itDepts.Select(d => $"{d.Props.Name} ({d.Props.Code})").ToArray();
        return Ok("E096_ef", "EF: Tree Query - LINQ on Trees", ExampleTier.Free, sw.ElapsedMilliseconds, itDepts.Count,
        [
            "Filter: Name.Contains('IT')",
            $"Found: {string.Join(", ", names)}"
        ]);
    }
}
