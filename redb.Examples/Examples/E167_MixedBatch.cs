using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Models.Contracts;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates mixed batch save with both non-generic RedbObject and generic RedbObject&lt;T&gt; with Props=null.
/// Shows flexibility of REDB to handle different object types in single batch operation.
/// </summary>
[ExampleMeta("E167", "Mixed Batch - Generic and Non-Generic", "Core",
    ExampleTier.Free, 167, "Mixed", "Batch", "NonGeneric", "Generic", RelatedApis = ["RedbObject", "RedbObject<TProps>", "IRedbService.SaveAsync"])]
public class E167_MixedBatch : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        var batchId = DateTime.UtcNow.Ticks;
        var objects = new List<IRedbObject>();

        // Add non-generic objects
        objects.Add(new RedbObject
        {
            name = $"Mixed_NonGeneric_1_{batchId}",
            value_string = "NonGeneric",
            value_long = 1
        });

        objects.Add(new RedbObject
        {
            name = $"Mixed_NonGeneric_2_{batchId}",
            value_string = "NonGeneric",
            value_long = 2
        });

        // Add generic objects with Props=null
        var scheme = await redb.EnsureSchemeFromTypeAsync<EmployeeProps>();
        
        var gen1 = new RedbObject<EmployeeProps>
        {
            name = $"Mixed_Generic_1_{batchId}",
            scheme_id = scheme.Id,
            value_string = "Generic",
            value_long = 3
        };
        gen1.Props = null!;
        objects.Add(gen1);

        var gen2 = new RedbObject<EmployeeProps>
        {
            name = $"Mixed_Generic_2_{batchId}",
            scheme_id = scheme.Id,
            value_string = "Generic",
            value_long = 4
        };
        gen2.Props = null!;
        objects.Add(gen2);

        // Batch save mixed types
        var ids = await redb.SaveAsync(objects);

        // Batch load
        var loaded = await redb.LoadAsync(ids);

        sw.Stop();

        if (loaded.Count != 4)
        {
            return Fail("E167", "Mixed Batch - Generic and Non-Generic", ExampleTier.Free, sw.ElapsedMilliseconds,
                $"Expected 4 objects, got {loaded.Count}.");
        }

        var nonGenCount = loaded.Count(x => x.ValueString == "NonGeneric");
        var genCount = loaded.Count(x => x.ValueString == "Generic");

        return Ok("E167", "Mixed Batch - Generic and Non-Generic", ExampleTier.Free, sw.ElapsedMilliseconds, loaded.Count,
            [$"Total: {loaded.Count} objects",
             $"Non-Generic: {nonGenCount}, Generic: {genCount}",
             $"IDs: [{string.Join(", ", ids)}]"]);
    }
}
