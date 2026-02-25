using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates using RedbObject base fields without Props (EAV values).
/// Base fields are stored directly in _objects table for fast access:
/// value_string, value_long, value_bool, value_double, value_datetime, value_guid, value_numeric, value_bytes.
/// Useful for simple objects, lookups, metadata without dynamic properties.
/// </summary>
[ExampleMeta("E164", "Base Fields - Single Object", "Core",
    ExampleTier.Free, 164, "BaseFields", "RedbObject", "NoProps", RelatedApis = ["RedbObject", "IRedbService.SaveAsync"])]
public class E164_BaseFieldsSingle : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Create non-generic RedbObject with base fields only (no Props/EAV)
        var obj = new RedbObject
        {
            name = $"BaseFields_Example_{DateTime.UtcNow.Ticks}",
            value_string = "Example String Value",
            value_long = 12345,
            value_bool = true,
            value_double = 3.14159,
            value_datetime = DateTime.UtcNow,
            value_numeric = 99.99m
        };

        // Save - no _values table involved, only _objects
        var id = await redb.SaveAsync(obj);

        // Load back
        var loaded = (await redb.LoadAsync(new[] { id })).FirstOrDefault();

        sw.Stop();

        if (loaded == null)
        {
            return Fail("E164", "Base Fields - Single Object", ExampleTier.Free, sw.ElapsedMilliseconds,
                "Failed to load saved object.");
        }

        return Ok("E164", "Base Fields - Single Object", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"ID: {id}, Name: {loaded.Name}",
             $"value_string: {loaded.ValueString}",
             $"value_long: {loaded.ValueLong}, value_bool: {loaded.ValueBool}"]);
    }
}
