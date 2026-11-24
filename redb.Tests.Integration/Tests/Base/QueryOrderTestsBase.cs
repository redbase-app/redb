using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A query returns its objects in the order asked for, in a list and in a tree. The objects are saved so that the order
/// asked for is the reverse of the id order, which is the order a plain scan returns. Free on SQL Server read a tree
/// page by joining the objects to the ordered ids without carrying the order.
/// </summary>
public abstract class QueryOrderTestsBase
{
    protected readonly IRedbService Redb;

    protected QueryOrderTestsBase(IRedbService redb) => Redb = redb;

    private async Task<(string Tag, long Root)> SeedAsync()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        var tag = "ord-" + Guid.NewGuid().ToString("N")[..8];
        var root = new TreeRedbObject<SimpleProps> { name = $"{tag}-root", Props = new SimpleProps { Title = $"{tag}-root", Price = 1000 } };
        root.id = await Redb.SaveAsync(root);
        for (var i = 1; i <= 8; i++)
        {
            // Saved with a falling price: the later the id, the lower the price.
            var child = new TreeRedbObject<SimpleProps> { name = $"{tag}-{i}", Props = new SimpleProps { Title = tag, Count = i, Price = 100 - i } };
            await Redb.CreateChildAsync(child, root);
        }
        return (tag, root.id);
    }

    [Fact]
    public async Task Tree_OrderByWithTake_KeepsTheOrder()
    {
        var (tag, root) = await SeedAsync();

        var nodes = await Redb.TreeQuery<SimpleProps>(root).Where(p => p.Title == tag).OrderBy(p => p.Price).Take(6).ToListAsync();

        nodes.Select(o => o.Props.Count).Should().Equal(8, 7, 6, 5, 4, 3);
    }

    /// <summary>
    /// Without Take there is no paging. Free on SQL Server dropped ORDER BY then (T-SQL keeps an ORDER BY inside a
    /// derived table only with OFFSET), and the objects came back in the order of the plan.
    /// </summary>
    [Fact]
    public async Task List_OrderByWithoutTake_KeepsTheOrder()
    {
        var (tag, _) = await SeedAsync();

        var asc = await Redb.Query<SimpleProps>().Where(p => p.Title == tag).OrderBy(p => p.Price).ToListAsync();
        var desc = await Redb.Query<SimpleProps>().Where(p => p.Title == tag).OrderByDescending(p => p.Count).ToListAsync();

        asc.Select(o => o.Props.Count).Should().Equal(8, 7, 6, 5, 4, 3, 2, 1);
        desc.Select(o => o.Props.Count).Should().Equal(8, 7, 6, 5, 4, 3, 2, 1);
    }

    [Fact]
    public async Task Tree_OrderByWithoutTake_KeepsTheOrder()
    {
        var (tag, root) = await SeedAsync();

        var nodes = await Redb.TreeQuery<SimpleProps>(root).Where(p => p.Title == tag).OrderBy(p => p.Price).ToListAsync();

        nodes.Select(o => o.Props.Count).Should().Equal(8, 7, 6, 5, 4, 3, 2, 1);
    }
}
