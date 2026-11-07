using System.Diagnostics;
using Microsoft.EntityFrameworkCore;
using redb.Examples.Ef;
using redb.Examples.Ef.Entities;
using redb.Examples.Examples;
using redb.Examples.Output;

namespace redb.Examples.ExamplesEf;

/// <summary>
/// EF Core twin of <see cref="E114_ListCreate"/>: the list "ExampleStatuses" created anew. An earlier one is
/// removed first, with the person fields that point at its items, as redb removes those values.
/// </summary>
[ExampleMeta("E114_ef", "EF: List - Create", "List", ExampleTier.Free, 1, "EF", "List")]
public class E114_ListCreate_ef : EfExampleBase
{
    public override async Task<ExampleResult> RunEfAsync(IDbContextFactory<FlatContext> contexts)
    {
        var sw = Stopwatch.StartNew();
        await using var db = await contexts.CreateDbContextAsync();

        var existing = await db.lists.FirstOrDefaultAsync(l => l._name == "ExampleStatuses");
        if (existing != null)
        {
            var itemIds = await db.list_items.Where(i => i._id_list == existing._id).Select(i => i._id).ToListAsync();
            if (itemIds.Count > 0)
            {
                await db.PersonProps.Where(p => p.Status != null && itemIds.Contains(p.Status.Value))
                    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Status, (long?)null));
                await db.PersonProps_Roles.Where(r => r._value != null && itemIds.Contains(r._value.Value))
                    .ExecuteDeleteAsync();
            }
            // The items go with the list through the cascade on _id_list.
            await db.lists.Where(l => l._id == existing._id).ExecuteDeleteAsync();
        }

        var savedList = new lists { _name = "ExampleStatuses", _alias = "Example Statuses" };
        db.lists.Add(savedList);
        await db.SaveChangesAsync();

        sw.Stop();

        return Ok("E114_ef", "EF: List - Create", ExampleTier.Free, sw.ElapsedMilliseconds, 1,
            [$"List ID: {savedList._id}", $"Name: {savedList._name}", $"Alias: {savedList._alias ?? "N/A"}"]);
    }
}
