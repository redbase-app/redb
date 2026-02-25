using System.Diagnostics;
using redb.Core;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Cleanup existing tree data before E088/E089 tests.
/// 
/// **Run this before E088 or E089 to ensure clean state.**
/// 
/// Uses DeleteWithPurgeAsync for permanent removal.
/// </summary>
[ExampleMeta("E087", "Tree Cleanup - Remove All", "Trees",
    ExampleTier.Free, 1, "Tree", "Cleanup", "DeleteWithPurgeAsync", "Pro", Order = 87)]
public class E087_TreeCleanup : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        await redb.SyncSchemeAsync<DepartmentProps>();

        var sw = Stopwatch.StartNew();

        var existing = await redb.TreeQuery<DepartmentProps>().ToListAsync();
        var count = existing.Count;

        if (count > 0)
        {
            await redb.DeleteWithPurgeAsync(existing.Select(e => e.Id).ToList(), batchSize: 50);
        }

        sw.Stop();

        return Ok("E087", "Tree Cleanup - Remove All", ExampleTier.Free, sw.ElapsedMilliseconds, count,
            [count > 0 ? $"Deleted: {count} tree nodes" : "No tree data to delete"]);
    }
}
