using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Ef.Entities;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E167_MixedBatch"/>: two plain header rows and two marked as employees without Props,
/// saved and loaded as one batch.
/// </summary>
[ExampleMeta("E167_ef", "EF: Mixed Batch - Generic and Non-Generic", "Core", ExampleTier.Free, 1, "EF", "Base", "Batch")]
public class E167_MixedBatch_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();

        var now = DateTime.UtcNow;
        objects Header(long schemeId, string name, string value, long number) => new()
        {
            _id_scheme = schemeId,
            _date_create = now,
            _date_modify = now,
            _name = $"{name}_{now.Ticks}",
            _value_string = value,
            _value_long = number
        };

        var batch = new List<objects>
        {
            Header(EfSchemeIds.None, "Mixed_NonGeneric_1", "NonGeneric", 1),
            Header(EfSchemeIds.None, "Mixed_NonGeneric_2", "NonGeneric", 2),
            Header(EfSchemeIds.Employee, "Mixed_Generic_1", "Generic", 3),
            Header(EfSchemeIds.Employee, "Mixed_Generic_2", "Generic", 4)
        };
        await using (var db = await contexts.CreateDbContextAsync())
        {
            db.objects.AddRange(batch);
            await db.SaveChangesAsync();
        }
        var ids = batch.Select(o => o._id).ToList();

        List<objects> loaded;
        await using (var db = await contexts.CreateDbContextAsync())
            loaded = await db.objects.Where(o => ids.Contains(o._id)).ToListAsync();

        sw.Stop();

        if (loaded.Count != 4)
            return Fail("E167_ef", "EF: Mixed Batch - Generic and Non-Generic", ExampleTier.Free, sw.ElapsedMilliseconds,
                $"Expected 4 objects, got {loaded.Count}.");

        var nonGenCount = loaded.Count(x => x._value_string == "NonGeneric");
        var genCount = loaded.Count(x => x._value_string == "Generic");
        var result = Ok("E167_ef", "EF: Mixed Batch - Generic and Non-Generic", ExampleTier.Free, sw.ElapsedMilliseconds, loaded.Count,
            [$"Total: {loaded.Count} objects",
             $"Non-Generic: {nonGenCount}, Generic: {genCount}",
             $"IDs: [{string.Join(", ", ids)}]"]);

        await using (var db = await contexts.CreateDbContextAsync())
            await db.objects.Where(o => ids.Contains(o._id)).ExecuteDeleteAsync();

        return result;
    }
}
