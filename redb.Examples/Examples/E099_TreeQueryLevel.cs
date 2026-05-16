using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Filter tree nodes by level.
/// WhereLevel finds nodes at specific depth from root.
/// </summary>
[ExampleMeta("E099", "Tree Query - By Level", "Trees",
    ExampleTier.Free, 2, "Tree", "TreeQuery", "WhereLevel", "Pro")]
public class E099_TreeQueryLevel : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Find root first - use TreeQuery!
        var roots = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "CORP")
            .Take(1)
            .ToListAsync();

        if (roots.Count == 0)
        {
            return Ok("E099", "Tree Query - By Level", ExampleTier.Free, sw.ElapsedMilliseconds, 0,
                ["No tree found. Run E090 first."]);
        }

        var rootId = roots[0].Id;

        var query = redb.TreeQuery<DepartmentProps>(rootId, maxDepth: 5)
            .WhereLevel(2);

        //var sql = query.ToSqlString();
        //Console.WriteLine(sql);

        // Find nodes at level 2 (departments under offices)
        var level2 = await query
            .WhereLevel(2)
            .ToListAsync();

        sw.Stop();

        var names = level2.Select(n => $"{n.Name} ({n.Props.Code})").ToArray();

        return Ok("E099", "Tree Query - By Level", ExampleTier.Free, sw.ElapsedMilliseconds, level2.Count,
        [
            "Filter: WhereLevel(2) - departments under offices",
            $"Found: {string.Join(", ", names)}"
        ]);
    }
}
