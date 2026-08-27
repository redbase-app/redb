using redb.Core;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Differential tests for the PVT prefilter over ListItem fields.
///
/// Kept apart from <see cref="PvtPrefilterEquivalenceTestsBase"/> because these need a list and its
/// items, which the Employee fixture does not have.
///
/// A ListItem field is the one place where a single structure carries several accessors, and they do
/// not behave alike. <c>Status.Id</c> lives in the row's own <c>_listitem</c> column, so it is a plain
/// row predicate. <c>Status.Value</c> and <c>Status.Alias</c> live in <c>_list_items</c> and need a
/// join, which no row predicate can express. The planner has to take the first and refuse the other
/// two, and it has to do so while all three share a structure id.
///
/// This was found in production: a filter over `ShippingPoint.Id` got no prefilter at all, first
/// because every ListItem accessor was refused wholesale, then because membership in a set was not
/// an expressible leaf.
/// </summary>
public abstract class PvtPrefilterListItemTestsBase
{
    protected readonly IRedbService Redb;

    protected PvtPrefilterListItemTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>Six persons over three statuses, so every subset below is non-trivial.</summary>
    private async Task<IReadOnlyList<RedbListItem>> SeedAsync(string listName)
    {
        var existing = await Redb.ListProvider.GetListByNameAsync(listName);
        if (existing != null)
        {
            var oldItems = await Redb.ListProvider.GetListItemsAsync(existing.Id);
            if (oldItems.Count > 0)
                await Redb.Context.Bulk.BulkDeleteValuesByListItemIdsAsync(oldItems.Select(i => i.Id).ToList());
            await Redb.ListProvider.DeleteListAsync(existing.Id);
        }

        var list = await Redb.ListProvider.SaveListAsync(RedbList.Create(listName, listName));
        var items = await Redb.ListProvider.AddItemsAsync(list, ["Active", "Blocked", "Archived"]);

        var persons = Enumerable.Range(0, 9).Select(i => new RedbObject<PersonProps>
        {
            name = $"{listName}_{i}",
            Props = new PersonProps
            {
                Name = $"Person{i}",
                Age = 20 + i,
                Email = $"p{i}@prefilter.test",
                Status = items[i % items.Count]
            }
        }).ToList();

        await Redb.SaveAsync(persons);
        return items;
    }

    private async Task<(T Off, T On)> BothAsync<T>(Func<Task<T>> run)
    {
        var restore = Redb.Configuration.EnablePvtPrefilter;
        try
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = false);
            var off = await run();

            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = true);
            var on = await run();

            return (off, on);
        }
        finally
        {
            Redb.UpdateConfiguration(c => c.EnablePvtPrefilter = restore);
        }
    }

    private async Task AssertSameAsync(Func<IRedbQueryable<PersonProps>> build, string because)
    {
        var (off, on) = await BothAsync(async () =>
        {
            var rows = await build().ToListAsync();
            return rows.Select(r => r.id).ToList();
        });

        on.Should().Equal(off, because);
    }

    [Fact]
    public async Task ListItemId_Equality_SameResults()
    {
        var items = await SeedAsync("PrefilterStatuses_Equality");
        var active = items.First(i => i.Value == "Active");

        await AssertSameAsync(
            () => Redb.Query<PersonProps>()
                .Where(p => p.Status!.Id == active.Id)
                .OrderByRedb(o => o.Id).Take(100),
            "Status.Id is stored in the row's own _listitem column and is a plain row predicate");
    }

    [Fact]
    public async Task ListItemId_In_SameResults()
    {
        var items = await SeedAsync("PrefilterStatuses_In");
        var wanted = items.Where(i => i.Value != "Archived").Select(i => i.Id).ToArray();

        await AssertSameAsync(
            () => Redb.Query<PersonProps>()
                .Where(p => wanted.Contains(p.Status!.Id))
                .OrderByRedb(o => o.Id).Take(100),
            "membership over a ListItem id is the production shape that had no prefilter at all");
    }

    /// <summary>
    /// Both accessors sit on one structure, and only one of them may become a branch. If the refused
    /// one ever leaked in, its predicate would be spliced onto the wrong column: a string pattern
    /// against a bigint.
    /// </summary>
    [Fact]
    public async Task ListItemId_WithValueAccessor_SameResults()
    {
        var items = await SeedAsync("PrefilterStatuses_Mixed");
        var active = items.First(i => i.Value == "Active");

        await AssertSameAsync(
            () => Redb.Query<PersonProps>()
                .Where(p => p.Status!.Id == active.Id && p.Status!.Value == "Active")
                .OrderByRedb(o => o.Id).Take(100),
            "Status.Value needs a join to _list_items, so it must stay out of the row predicate " +
            "while Status.Id goes in, and the answer must not change either way");
    }

    [Fact]
    public async Task ListItemValue_SameResults()
    {
        await SeedAsync("PrefilterStatuses_Value");

        await AssertSameAsync(
            () => Redb.Query<PersonProps>()
                .Where(p => p.Status!.Value == "Active")
                .OrderByRedb(o => o.Id).Take(100),
            "the display text lives in _list_items, which no row predicate can reach");
    }

    /// <summary>
    /// The production query in full: membership over a ListItem id next to an ordinary field. Both
    /// halves are expressible, so both become branches; before that, the planner kept only the more
    /// selective one and then refused the plan because the other column was left uncovered.
    /// </summary>
    [Fact]
    public async Task ListItemId_In_WithSecondField_SameResults()
    {
        var items = await SeedAsync("PrefilterStatuses_TwoFields");
        var wanted = items.Where(i => i.Value != "Archived").Select(i => i.Id).ToArray();

        await AssertSameAsync(
            () => Redb.Query<PersonProps>()
                .Where(p => wanted.Contains(p.Status!.Id) && p.Age > 21)
                .OrderByRedb(o => o.Id).Take(100),
            "every conjunct becomes a branch, so the ListItem column is covered and the plan stands");
    }
}
