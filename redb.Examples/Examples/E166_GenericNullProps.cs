using System.Diagnostics;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Models.Contracts;
using redb.Examples.Models;
using redb.Examples.Output;

namespace redb.Examples.Examples;

/// <summary>
/// Demonstrates using generic RedbObject&lt;TProps&gt; with Props=null.
/// Useful when you need scheme association but don't need EAV values.
/// Only base fields are stored - Props is explicitly null.
/// </summary>
[ExampleMeta("E166", "Generic with Props=null", "Core",
    ExampleTier.Free, 166, "Generic", "NullProps", "BaseFields", RelatedApis = ["RedbObject<TProps>", "IRedbService.SaveAsync"])]
public class E166_GenericNullProps : ExampleBase
{
    public override async Task<ExampleResult> RunAsync(IRedbService redb)
    {
        var sw = Stopwatch.StartNew();

        // Ensure scheme exists
        var scheme = await redb.EnsureSchemeFromTypeAsync<EmployeeProps>();

        // Create generic object but with Props=null (only base fields)
        var obj = new RedbObject<EmployeeProps>
        {
            name = $"GenericNullProps_{DateTime.UtcNow.Ticks}",
            scheme_id = scheme.Id,
            value_string = "MetadataOnly",
            value_long = 999,
            value_bool = true
        };
        obj.Props = null!; // Explicitly null - no EAV values

        // Save via interface (triggers non-Props save path)
        var id = await redb.SaveAsync((IRedbObject)obj);

        // Load as base IRedbObject (not generic)
        var loaded = (await redb.LoadAsync(new[] { id })).FirstOrDefault();

        sw.Stop();

        if (loaded == null)
        {
            return Fail("E166", "Generic with Props=null", ExampleTier.Free, sw.ElapsedMilliseconds,
                "Failed to load saved object.");
        }

        return Ok("E166", "Generic with Props=null", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"ID: {id}, SchemeId: {loaded.SchemeId}",
             $"value_string: {loaded.ValueString}",
             $"value_long: {loaded.ValueLong} (no EAV values)"]);
    }
}
