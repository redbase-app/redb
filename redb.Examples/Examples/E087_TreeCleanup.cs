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
/// Uses SoftDeleteAsync: the objects are re-parented under the trash scheme synchronously,
/// so they vanish from queries (and release their unique keys) immediately - clean state
/// without waiting. The physical purge of the _values cascade is queued in the database
/// and drained by the built-in BackgroundDeletionService in any host that runs it.
/// </summary>
[ExampleMeta("E087", "Tree Cleanup - Remove All", "Trees",
    ExampleTier.Free, 1, "Tree", "Cleanup", "SoftDeleteAsync", "Pro", Order = 87)]
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
            Console.WriteLine($"[E087] Soft-deleting {count} tree nodes...");

            // One fast, atomic mark - no waiting for the physical purge. The next examples
            // see a clean tree right away; the deferred purge work stays queued in the
            // database (the DB is the queue), invisible to queries either way.
            var mark = await redb.SoftDeleteAsync(existing.Select(e => e.Id).ToList());

            Console.WriteLine(
                $"[E087] Done: {mark.MarkedCount} nodes moved to trash container {mark.TrashId}; " +
                "physical purge is queued for the background deletion service.");
        }

        sw.Stop();

        return Ok("E087", "Tree Cleanup - Remove All", ExampleTier.Free, sw.ElapsedMilliseconds, count,
            [count > 0 ? $"Soft-deleted: {count} tree nodes (physical purge deferred)" : "No tree data to delete"]);
    }
}
