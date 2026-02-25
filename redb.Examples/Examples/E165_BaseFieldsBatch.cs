using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Models.Contracts;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates batch save/load of non-generic RedbObject with base fields.
/// Efficient for bulk operations on simple objects without Props.
/// All data stored in _objects table - no EAV overhead.
/// </summary>
[ExampleMeta("E165", "Base Fields - Batch Operations", "Core",
    ExampleTier.Free, 165, "BaseFields", "Batch", "NonGeneric", RelatedApis = ["RedbObject", "IRedbService.SaveAsync"])]
public class E165_BaseFieldsBatch : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Create batch of non-generic objects
        var objects = new List<RedbObject>();
        var batchId = DateTime.UtcNow.Ticks;
        
        for (int i = 0; i < 10; i++)
        {
            objects.Add(new RedbObject
            {
                name = $"Batch_BaseFields_{batchId}_{i}",
                value_string = $"Item_{i}",
                value_long = i * 100,
                value_bool = i % 2 == 0
            });
        }

        // Batch save (cast to IRedbObject for batch API)
        var ids = await redb.SaveAsync(objects.Cast<IRedbObject>());

        // Batch load
        var loaded = await redb.LoadAsync(ids);

        sw.Stop();

        if (loaded.Count != 10)
        {
            return Fail("E165", "Base Fields - Batch Operations", ExampleTier.Free, sw.ElapsedMilliseconds,
                $"Expected 10 objects, got {loaded.Count}.");
        }

        // Verify data integrity
        var verified = loaded.All(x => x.ValueString != null && x.ValueString.StartsWith("Item_"));

        return Ok("E165", "Base Fields - Batch Operations", ExampleTier.Free, sw.ElapsedMilliseconds, loaded.Count,
            [$"Saved and loaded: {loaded.Count} objects",
             $"IDs: [{string.Join(", ", ids.Take(3))}...]",
             $"Data verified: {verified}"]);
    }
}
