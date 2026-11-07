using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Ef.Entities;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E166_GenericNullProps"/>: a header row marked as an employee but without an
/// EmployeeProps row, saved and loaded back.
/// </summary>
[ExampleMeta("E166_ef", "EF: Generic with Props=null", "Core", ExampleTier.Free, 1, "EF", "Base")]
public class E166_GenericNullProps_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();

        var now = DateTime.UtcNow;
        var obj = new objects
        {
            _id_scheme = EfSchemeIds.Employee,
            _date_create = now,
            _date_modify = now,
            _name = $"GenericNullProps_{now.Ticks}",
            _value_string = "MetadataOnly",
            _value_long = 999,
            _value_bool = true
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
            return Fail("E166_ef", "EF: Generic with Props=null", ExampleTier.Free, sw.ElapsedMilliseconds,
                "Failed to load saved object.");

        var result = Ok("E166_ef", "EF: Generic with Props=null", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"ID: {id}, SchemeId: {loaded._id_scheme}",
             $"value_string: {loaded._value_string}",
             $"value_long: {loaded._value_long} (no Props row)"]);

        await using (var db = await contexts.CreateDbContextAsync())
            await db.objects.Where(o => o._id == id).ExecuteDeleteAsync();

        return result;
    }
}
