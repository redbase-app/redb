using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Ef.Entities;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>EF Core twin of <see cref="E164_BaseFieldsSingle"/>: one header row with value fields and no Props, saved and loaded back.</summary>
[ExampleMeta("E164_ef", "EF: Base Fields - Single Object", "Core", ExampleTier.Free, 1, "EF", "Base")]
public class E164_BaseFieldsSingle_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();

        var now = DateTime.UtcNow;
        var obj = new objects
        {
            _id_scheme = EfSchemeIds.None,
            _date_create = now,
            _date_modify = now,
            _name = $"BaseFields_Example_{now.Ticks}",
            _value_string = "Example String Value",
            _value_long = 12345,
            _value_bool = true,
            _value_double = 3.14159,
            _value_datetime = now,
            _value_numeric = 99.99m
        };
        await using (var db = await contexts.CreateDbContextAsync())
        {
            db.objects.Add(obj);
            await db.SaveChangesAsync();
        }
        var id = obj._id;

        objects? loaded;
        await using (var db = await contexts.CreateDbContextAsync())
            loaded = await db.objects.FirstOrDefaultAsync(o => o._id == id);

        sw.Stop();

        if (loaded == null)
            return Fail("E164_ef", "EF: Base Fields - Single Object", ExampleTier.Free, sw.ElapsedMilliseconds,
                "Failed to load saved object.");

        var result = Ok("E164_ef", "EF: Base Fields - Single Object", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"ID: {id}, Name: {loaded._name}",
             $"value_string: {loaded._value_string}",
             $"value_long: {loaded._value_long}, value_bool: {loaded._value_bool}"]);

        await using (var db = await contexts.CreateDbContextAsync())
            await db.objects.Where(o => o._id == id).ExecuteDeleteAsync();

        return result;
    }
}
