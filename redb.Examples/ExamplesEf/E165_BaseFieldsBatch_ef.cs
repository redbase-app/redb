using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Ef.Entities;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E165_BaseFieldsBatch"/>: ten header rows without Props, saved and loaded as a batch.</summary>
[ExampleMeta("E165_ef", "EF: Base Fields - Batch Operations", "Core", ExampleTier.Free, 1, "EF", "Base", "Batch")]
public class E165_BaseFieldsBatch_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();

        var now = DateTime.UtcNow;
        var batch = Enumerable.Range(0, 1000).Select(i => new objects
        {
            _id_scheme = EfSchemeIds.None,
            _date_create = now,
            _date_modify = now,
            _name = $"Batch_BaseFields_{now.Ticks}_{i}",
            _value_string = $"Item_{i}",
            _value_long = i * 100,
            _value_bool = i % 2 == 0
        }).ToList();
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

        if (loaded.Count != 1000)
            return Fail("E165_ef", "EF: Base Fields - Batch Operations", ExampleTier.Free, sw.ElapsedMilliseconds,
                $"Expected 10 objects, got {loaded.Count}.");

        var verified = loaded.All(x => x._value_string != null && x._value_string.StartsWith("Item_"));
        var result = Ok("E165_ef", "EF: Base Fields - Batch Operations", ExampleTier.Free, sw.ElapsedMilliseconds, loaded.Count,
            [$"Saved and loaded: {loaded.Count} objects",
             $"IDs: [{string.Join(", ", ids.Take(3))}...]",
             $"Data verified: {verified}"]);

        await using (var db = await contexts.CreateDbContextAsync())
            await db.objects.Where(o => ids.Contains(o._id)).ExecuteDeleteAsync();

        return result;
    }
}
