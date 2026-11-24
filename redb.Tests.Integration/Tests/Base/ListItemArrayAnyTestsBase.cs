using System.Linq.Expressions;
using redb.Core;
using redb.Core.Models.Contracts;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// <c>p.Roles.Any(r =&gt; r.Value == x)</c> over a list of list items asks whether some item has that value, on every
/// provider. The parser used to read any comparison as ==, so <c>r.Value != x</c> or <c>r.Id &gt; n</c> answered the
/// question for == without a word; such an operator is now refused before any SQL.
/// </summary>
public abstract class ListItemArrayAnyTestsBase
{
    protected readonly IRedbService Redb;

    protected ListItemArrayAnyTestsBase(IRedbService redb) => Redb = redb;

    private async Task<(string Tag, RedbListItem Admin, RedbListItem User, RedbList List)> SeedAsync()
    {
        await Redb.SyncSchemeAsync<PersonProps>();
        var tag = "lia-" + Guid.NewGuid().ToString("N")[..8];
        var list = await Redb.ListProvider.SaveListAsync(RedbList.Create(tag, tag));
        var items = await Redb.ListProvider.AddItemsAsync(list, ["Admin", "User"]);
        var admin = items.First(i => i.Value == "Admin");
        var user = items.First(i => i.Value == "User");

        var people = new List<PersonProps>
        {
            new() { Name = "1", Age = 40, Email = tag, Roles = [admin, user] },
            new() { Name = "2", Age = 20, Email = tag, Roles = [user] },
            new() { Name = "3", Age = 50, Email = tag, Roles = [] },
            new() { Name = "4", Age = 60, Email = tag },
        };
        await Redb.SaveAsync(people.Select(p => (IRedbObject)new RedbObject<PersonProps> { name = $"{tag}-{p.Name}", Props = p }).ToList());
        return (tag, admin, user, list);
    }

    private async Task CleanupAsync(RedbList list)
    {
        var items = await Redb.ListProvider.GetListItemsAsync(list.Id);
        await Redb.Context.Bulk.BulkDeleteValuesByListItemIdsAsync(items.Select(i => i.Id).ToList());
        await Redb.ListProvider.DeleteListAsync(list.Id);
    }

    private async Task<string[]> NamesAsync(string tag, Expression<Func<PersonProps, bool>> filter)
        => (await Redb.Query<PersonProps>().Where(p => p.Email == tag).Where(filter).ToListAsync())
            .Select(o => o.Props.Name).OrderBy(n => n).ToArray();

    [Fact]
    public async Task AnyOverListItems_AsksForAnItemWithThatValue()
    {
        var (tag, admin, user, list) = await SeedAsync();
        try
        {
            (await NamesAsync(tag, p => p.Roles!.Any(r => r.Value == "Admin"))).Should().Equal("1");
            (await NamesAsync(tag, p => p.Roles!.Any(r => "User" == r.Value))).Should().Equal("1", "2");
            var userId = user.Id;
            (await NamesAsync(tag, p => p.Roles!.Any(r => r.Id == userId))).Should().Equal("1", "2");

            // No item with that value: an empty or a missing list included.
            (await NamesAsync(tag, p => !p.Roles!.Any(r => r.Value == "Admin"))).Should().Equal("2", "3", "4");
            (await NamesAsync(tag, p => p.Age > 30 && p.Roles!.Any(r => r.Value == "User"))).Should().Equal("1");
            (await NamesAsync(tag, p => p.Age > 30 && !p.Roles!.Any(r => r.Value == "User"))).Should().Equal("3", "4");
        }
        finally
        {
            await CleanupAsync(list);
        }
    }

    [Fact]
    public async Task AnyOverListItems_RefusesAnOperatorOtherThanEquality()
    {
        var (tag, admin, _, list) = await SeedAsync();
        try
        {
            var adminId = admin.Id;
            var names = new List<string> { "Admin" };
            var refused = new Expression<Func<PersonProps, bool>>[]
            {
                p => p.Roles!.Any(r => r.Value != "Admin"),
                p => p.Roles!.Any(r => r.Id > adminId),
                p => p.Roles!.Any(r => adminId <= r.Id),
                p => !p.Roles!.Any(r => r.Id != adminId),
            };
            foreach (var filter in refused)
            {
                var run = async () => await NamesAsync(tag, filter);
                (await run.Should().ThrowAsync<NotSupportedException>()).Which.Message.Should().Contain("only with ==");
            }

            var contains = async () => await NamesAsync(tag, p => p.Roles!.Any(r => names.Contains(r.Value)));
            await contains.Should().ThrowAsync<NotSupportedException>();
        }
        finally
        {
            await CleanupAsync(list);
        }
    }
}
