using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Core.Models.Entities;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;
using Biz = redb.Examples.Models;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E118_PropsListItemField"/>: a person whose Status points at the item "Active",
/// saved and loaded back whole with the item included.
/// </summary>
[ExampleMeta("E118_ef", "EF: Props - ListItem Field", "List", ExampleTier.Free, 1, "EF", "List")]
public class E118_PropsListItemField_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();

        long personId;
        await using (var db = await contexts.CreateDbContextAsync())
        {
            var list = await db.lists.FirstOrDefaultAsync(l => l._name == "ExampleStatuses");
            if (list == null)
            {
                sw.Stop();
                return Fail("E118_ef", "EF: Props - ListItem Field", ExampleTier.Free, sw.ElapsedMilliseconds,
                    "List 'ExampleStatuses' not found. Run E114_ef first.");
            }

            var items = await db.list_items.Where(i => i._id_list == list._id).ToListAsync();
            var activeStatus = items.FirstOrDefault(i => i._value == "Active");
            if (activeStatus == null)
            {
                sw.Stop();
                return Fail("E118_ef", "EF: Props - ListItem Field", ExampleTier.Free, sw.ElapsedMilliseconds,
                    "Item 'Active' not found. Run E115_ef first.");
            }

            var person = PersonEfMapping.ToEntity("John Doe", 30, "john@example.com", activeStatus._id, []);
            db.objects.Add(person);
            await db.SaveChangesAsync();
            personId = person._id;
        }

        RedbObject<Biz.PersonProps>? loaded;
        await using (var db = await contexts.CreateDbContextAsync())
        {
            var entity = await db.Persons().FirstOrDefaultAsync(p => p._id == personId);
            loaded = entity == null ? null : PersonEfMapping.ToBusiness(entity);
        }

        sw.Stop();

        var statusValue = loaded?.Props?.Status?.Value ?? "N/A";
        return Ok("E118_ef", "EF: Props - ListItem Field", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"Person ID: {personId}", $"Status: {statusValue}"]);
    }
}
