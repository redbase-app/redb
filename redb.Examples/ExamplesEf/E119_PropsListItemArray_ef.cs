using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Core.Models.Entities;
using redb.Examples.Ef;
using redb.Examples.Examples;
using redb.Examples.Output;
using Biz = redb.Examples.Models;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E119_PropsListItemArray"/>: a person whose Roles point at the first three items,
/// saved and loaded back whole with the items included.
/// </summary>
[ExampleMeta("E119_ef", "EF: Props - List<ListItem>", "List", ExampleTier.Free, 1, "EF", "List")]
public class E119_PropsListItemArray_ef : EfExampleBase
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
                return Fail("E119_ef", "EF: Props - List<ListItem>", ExampleTier.Free, sw.ElapsedMilliseconds,
                    "List 'ExampleStatuses' not found. Run E114_ef first.");
            }

            var items = await db.list_items.Where(i => i._id_list == list._id).OrderBy(i => i._id).ToListAsync();
            if (items.Count < 2)
            {
                sw.Stop();
                return Fail("E119_ef", "EF: Props - List<ListItem>", ExampleTier.Free, sw.ElapsedMilliseconds,
                    "Need at least 2 items. Run E115_ef first.");
            }

            var person = PersonEfMapping.ToEntity("Jane Smith", 28, "jane@example.com", null,
                items.Take(3).Select(i => i._id).ToList());
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

        var rolesCount = loaded?.Props?.Roles?.Count ?? 0;
        var rolesList = loaded?.Props?.Roles != null
            ? string.Join(", ", loaded.Props.Roles.Select(r => r.Value))
            : "N/A";
        return Ok("E119_ef", "EF: Props - List<ListItem>", ExampleTier.Free, sw.ElapsedMilliseconds, rolesCount,
            [$"Person ID: {personId}", $"Roles ({rolesCount}): {rolesList}"]);
    }
}
