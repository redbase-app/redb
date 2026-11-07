using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Load tree nodes as flat list without Parent/Children links.
/// ToFlatListAsync is faster than ToTreeListAsync - skips relationship building.
/// Requires E089 to run first (creates tree data).
/// </summary>
[ExampleMeta("E113", "Tree Query - ToFlatListAsync", "Trees",
    ExampleTier.Free, 113, "ToFlatListAsync", "Tree", "Flat", "Performance", 
    RelatedApis = ["ITreeQueryable.ToFlatListAsync"])]
public class E113_TreeToFlatList : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Load root department
        var root = await redb.TreeQuery<DepartmentProps>()
            .WhereRoots()
            .Take(1)
            .ToListAsync();

        if (root.Count == 0)
        {
            sw.Stop();
            return Fail("E113", "Tree Query - ToFlatListAsync", ExampleTier.Free, sw.ElapsedMilliseconds,
                "No tree found. Run E089 first.");
        }

        // Load tree as flat list (no Parent/Children links)
        var query = redb.TreeQuery<DepartmentProps>(root[0].Id, maxDepth: 3)
            .Where(d => d.IsActive);

        // Uncomment to see generated SQL:
        //var sql = await query.ToSqlStringAsync();
        //Console.WriteLine(sql);

        var flatList = await query.ToFlatListAsync();

        sw.Stop();

        var sample = flatList.FirstOrDefault()?.Props.Name ?? "N/A";

        return Ok("E113", "Tree Query - ToFlatListAsync", ExampleTier.Free, sw.ElapsedMilliseconds, flatList.Count,
            [$"Flat list: {flatList.Count} nodes", $"First: {sample}"]);
    }
}
