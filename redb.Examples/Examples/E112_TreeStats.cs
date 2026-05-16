using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Core.Models.Collections;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Tree statistics via TreeCollection.
/// TreeCollection.GetStats() - node count, depth, leaf count, max width.
/// 
/// Key method:
/// - **collection.GetStats()** - get tree metrics
/// </summary>
[ExampleMeta("E112", "Tree - GetStats", "Trees",
    ExampleTier.Free, 2, "Tree", "TreeCollection", "GetStats", "Statistics", "Pro", Order = 112)]
public class E112_TreeStats : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        // Find root
        var roots = await redb.TreeQuery<DepartmentProps>()
            .Where(d => d.Code == "CORP")
            .Take(1)
            .ToListAsync();

        if (roots.Count == 0)
            return Fail("E112", "Tree - GetStats", ExampleTier.Free, 0, "No tree. Run E088 first.");

        var root = (TreeRedbObject<DepartmentProps>)roots[0];

        // Load tree and build collection
        var sw = Stopwatch.StartNew();
        var tree = await redb.LoadTreeAsync<DepartmentProps>(root, maxDepth: 5);

        // Build TreeCollection for stats
        var collection = new TreeCollection<DepartmentProps>();
        AddToCollection(collection, tree);

        var stats = collection.GetStats();
        sw.Stop();

        return Ok("E112", "Tree - GetStats", ExampleTier.Free, sw.ElapsedMilliseconds, collection.Count,
            [$"Stats: {stats}", $"Collection count: {collection.Count}"]);
    }

    private static void AddToCollection(TreeCollection<DepartmentProps> collection, ITreeRedbObject<DepartmentProps> node)
    {
        collection.Add(node);
        foreach (var child in node.Children.OfType<ITreeRedbObject<DepartmentProps>>())
            AddToCollection(collection, child);
    }
}
