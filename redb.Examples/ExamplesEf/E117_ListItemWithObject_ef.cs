using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Ef.Entities;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E117_ListItemWithObject"/>: a city saved with its coordinates, then a list item
/// "Capital" that points at it, saved together.
/// </summary>
[ExampleMeta("E117_ef", "EF: ListItem - With Object", "List", ExampleTier.Free, 1, "EF", "List")]
public class E117_ListItemWithObject_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var list = await db.lists.FirstOrDefaultAsync(l => l._name == "ExampleStatuses");
        if (list == null)
        {
            sw.Stop();
            return Fail("E117_ef", "EF: ListItem - With Object", ExampleTier.Free, sw.ElapsedMilliseconds,
                "List 'ExampleStatuses' not found. Run E114_ef first.");
        }

        var now = DateTime.UtcNow;
        var city = new objects
        {
            _id_scheme = EfSchemeIds.City,
            _date_create = now,
            _date_modify = now,
            _name = "Moscow",
            CityProps = new CityProps
            {
                Name = "Moscow",
                Population = 12_655_000,
                Region = "Central Federal District",
                IsCapital = true,
                CityProps_Coordinates =
                [
                    new CityProps_Coordinates { _idx = 0, _value = 55.7558 },
                    new CityProps_Coordinates { _idx = 1, _value = 37.6173 }
                ]
            }
        };
        var item = new list_items { _id_list = list._id, _value = "Capital", _alias = "Capital city status", _id_objectNavigation = city };
        db.list_items.Add(item);
        await db.SaveChangesAsync();

        sw.Stop();

        return Ok("E117_ef", "EF: ListItem - With Object", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Item: {item._value} (ID: {item._id})", $"Linked object ID: {item._id_object}"]);
    }
}
