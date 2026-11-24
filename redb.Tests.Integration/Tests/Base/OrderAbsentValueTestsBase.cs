using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Sorting by a field returns every object the filter selects, also those without a value for that field (they sort
/// first ascending and last descending, as LINQ orders null), and OrderBy then ThenBy sorts by both keys.
/// </summary>
public abstract class OrderAbsentValueTestsBase
{
    protected readonly IRedbService Redb;

    protected OrderAbsentValueTestsBase(IRedbService redb) => Redb = redb;

    private async Task<string> SeedAsync()
    {
        await Redb.SyncSchemeAsync<OrderAbsentProps>();
        var tag = "oa-" + Guid.NewGuid().ToString("N")[..8];
        var rows = new (string? Label, int? Rank, int Group, string Name)[]
        {
            ("b", 2, 1, "d"),
            (null, null, 2, "a"),
            ("a", 1, 1, "b"),
            ("c", 3, 2, "c"),
            (null, null, 1, "a"),
        };
        foreach (var r in rows)
        {
            await Redb.SaveAsync(new RedbObject<OrderAbsentProps>
            {
                name = $"{tag}-{r.Name}-{r.Group}",
                Props = new OrderAbsentProps { Tag = tag, Label = r.Label, Rank = r.Rank, Group = r.Group, Name = r.Name }
            });
        }
        return tag;
    }

    [Fact]
    public async Task OrderBy_AStringFieldSomeObjectsLack_ReturnsEveryObject_AbsentFirst()
    {
        var tag = await SeedAsync();

        var rows = await Redb.Query<OrderAbsentProps>().Where(p => p.Tag == tag).OrderBy(p => p.Label).ToListAsync();

        rows.Select(r => r.Props.Label).Should().Equal(new string?[] { null, null, "a", "b", "c" });
    }

    [Fact]
    public async Task OrderByDescending_AStringFieldSomeObjectsLack_ReturnsEveryObject_AbsentLast()
    {
        var tag = await SeedAsync();

        var rows = await Redb.Query<OrderAbsentProps>().Where(p => p.Tag == tag).OrderByDescending(p => p.Label).ToListAsync();

        rows.Select(r => r.Props.Label).Should().Equal(new string?[] { "c", "b", "a", null, null });
    }

    [Fact]
    public async Task OrderBy_ANumberFieldSomeObjectsLack_ReturnsEveryObject_AbsentFirst()
    {
        var tag = await SeedAsync();

        var rows = await Redb.Query<OrderAbsentProps>().Where(p => p.Tag == tag).OrderBy(p => p.Rank).ToListAsync();

        rows.Select(r => r.Props.Rank).Should().Equal(new int?[] { null, null, 1, 2, 3 });
    }

    [Fact]
    public async Task OrderBy_ThenByAString_SortsByBothKeys()
    {
        var tag = await SeedAsync();

        var rows = await Redb.Query<OrderAbsentProps>().Where(p => p.Tag == tag)
            .OrderBy(p => p.Group).ThenBy(p => p.Name).ToListAsync();

        rows.Select(r => $"{r.Props.Group}{r.Props.Name}").Should().Equal("1a", "1b", "1d", "2a", "2c");
    }

    [Fact]
    public async Task OrderBy_ThenByDescendingAString_SortsByBothKeys()
    {
        var tag = await SeedAsync();

        var rows = await Redb.Query<OrderAbsentProps>().Where(p => p.Tag == tag)
            .OrderBy(p => p.Group).ThenByDescending(p => p.Name).ToListAsync();

        rows.Select(r => $"{r.Props.Group}{r.Props.Name}").Should().Equal("1d", "1b", "1a", "2c", "2a");
    }

    [Fact]
    public async Task ThenBy_AFieldSomeObjectsLack_PutsThemFirstInTheirGroup()
    {
        var tag = await SeedAsync();

        var asc = await Redb.Query<OrderAbsentProps>().Where(p => p.Tag == tag).OrderBy(p => p.Group).ThenBy(p => p.Label).ToListAsync();
        asc.Select(r => $"{r.Props.Group}{r.Props.Label ?? "-"}").Should().Equal("1-", "1a", "1b", "2-", "2c");

        var desc = await Redb.Query<OrderAbsentProps>().Where(p => p.Tag == tag).OrderBy(p => p.Group).ThenByDescending(p => p.Label).ToListAsync();
        desc.Select(r => $"{r.Props.Group}{r.Props.Label ?? "-"}").Should().Equal("1b", "1a", "1-", "2c", "2-");
    }

    [Fact]
    public async Task Page_OverAFieldSomeObjectsLack_StartsWithThem()
    {
        var tag = await SeedAsync();

        var page = await Redb.Query<OrderAbsentProps>().Where(p => p.Tag == tag).OrderBy(p => p.Rank).ThenByRedb(o => o.Id).Skip(1).Take(2).ToListAsync();
        page.Select(r => r.Props.Rank).Should().Equal(new int?[] { null, 1 });

        var last = await Redb.Query<OrderAbsentProps>().Where(p => p.Tag == tag).OrderByDescending(p => p.Rank).ThenByRedb(o => o.Id).Skip(3).Take(2).ToListAsync();
        last.Select(r => r.Props.Rank).Should().Equal(new int?[] { null, null });
    }

    [Fact]
    public async Task DistinctBy_OrderedByAFieldSomeObjectsLack_KeepsTheEmptyKeyFirst()
    {
        var tag = await SeedAsync();

        var rows = await Redb.Query<OrderAbsentProps>().Where(p => p.Tag == tag).OrderBy(p => p.Label).DistinctBy(p => p.Label).ToListAsync();
        rows.Select(r => r.Props.Label).Should().Equal(new string?[] { null, "a", "b", "c" });
    }

    [Fact]
    public async Task TreeQuery_OrderedByAFieldSomeChildrenLack_PutsThemFirst()
    {
        await Redb.SyncSchemeAsync<OrderAbsentProps>();
        var tag = "oat-" + Guid.NewGuid().ToString("N")[..8];
        var root = new TreeRedbObject<OrderAbsentProps> { name = tag + "-root", Props = new OrderAbsentProps { Tag = tag + "-root", Rank = 100 } };
        root.id = await Redb.SaveAsync(root);
        foreach (var rank in new int?[] { 2, null, 1, null })
            await Redb.CreateChildAsync(new TreeRedbObject<OrderAbsentProps> { name = $"{tag}-{rank}", Props = new OrderAbsentProps { Tag = tag, Rank = rank } }, root);

        var asc = await Redb.TreeQuery<OrderAbsentProps>(root.id).Where(p => p.Tag == tag).OrderBy(p => p.Rank).ToListAsync();
        asc.Select(r => r.Props.Rank).Should().Equal(new int?[] { null, null, 1, 2 });

        var desc = await Redb.TreeQuery<OrderAbsentProps>(root.id).Where(p => p.Tag == tag).OrderByDescending(p => p.Rank).ToListAsync();
        desc.Select(r => r.Props.Rank).Should().Equal(new int?[] { 2, 1, null, null });
    }

    [Fact]
    public async Task Window_OrderedByAFieldSomeObjectsLack_NumbersThemFirst()
    {
        var tag = await SeedAsync();

        var rows = await Redb.Query<OrderAbsentProps>().Where(p => p.Tag == tag)
            .WithWindow(w => w.OrderBy(p => p.Rank))
            .SelectAsync(x => new { x.Props.Rank, Row = redb.Core.Query.Aggregation.Win.RowNumber() });
        rows.Where(r => r.Rank == null).Select(r => r.Row).OrderBy(r => r).Should().Equal(1L, 2L);

        var desc = await Redb.Query<OrderAbsentProps>().Where(p => p.Tag == tag)
            .WithWindow(w => w.OrderByDesc(p => p.Rank))
            .SelectAsync(x => new { x.Props.Rank, Row = redb.Core.Query.Aggregation.Win.RowNumber() });
        desc.Where(r => r.Rank == null).Select(r => r.Row).OrderBy(r => r).Should().Equal(4L, 5L);
    }
}
