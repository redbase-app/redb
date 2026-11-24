using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// DeleteAsync over a batch counts every requested object it deleted, a parent and its child both. On SQLite the
/// _id_parent cascade removed the child with its parent before the statement reached it, and the child was not counted.
/// A descendant that was not requested is deleted with its parent and not counted, as on every provider.
/// </summary>
public abstract class DeleteBatchCountTestsBase
{
    protected readonly IRedbService Redb;

    protected DeleteBatchCountTestsBase(IRedbService redb) => Redb = redb;

    private async Task<(long Parent, long Child, long Grandchild)> SeedAsync()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        var tag = "dbc-" + Guid.NewGuid().ToString("N")[..8];
        var parent = new TreeRedbObject<SimpleProps> { name = $"{tag}-p", Props = new SimpleProps { Title = tag } };
        parent.id = await Redb.SaveAsync(parent);
        var child = new TreeRedbObject<SimpleProps> { name = $"{tag}-c", Props = new SimpleProps { Title = tag, Count = 1 } };
        child.id = await Redb.CreateChildAsync(child, parent);
        var grandchild = new TreeRedbObject<SimpleProps> { name = $"{tag}-g", Props = new SimpleProps { Title = tag, Count = 2 } };
        grandchild.id = await Redb.CreateChildAsync(grandchild, child);
        return (parent.id, child.id, grandchild.id);
    }

    [Fact]
    public async Task DeleteBatch_HoldingAParentAndItsChild_CountsBoth()
    {
        var (parent, child, grandchild) = await SeedAsync();

        var deleted = await Redb.DeleteAsync(new[] { parent, child });

        deleted.Should().Be(2, "both requested objects were deleted by this call");
        (await Redb.LoadAsync<SimpleProps>(grandchild)).Should().BeNull("the descendant goes with its parent");
    }

    [Fact]
    public async Task DeleteBatch_ChildListedBeforeItsParent_CountsBoth()
    {
        var (parent, child, _) = await SeedAsync();

        (await Redb.DeleteAsync(new[] { child, parent })).Should().Be(2);
    }

    private async Task<(long Parent, long Child, long Grandchild, string Tag)> SeedTaggedAsync()
    {
        var (parent, child, grandchild) = await SeedAsync();
        var tag = (await Redb.LoadAsync<SimpleProps>(parent))!.Props.Title;
        return (parent, child, grandchild, tag);
    }

    // A query delete whose filter matches a parent and its child counts both. Pro on SQLite builds its own DELETE, and
    // changes() missed the child the cascade had removed with the parent; a filter on base fields only failed there
    // ("no such column: o._id"). Free threw NotSupportedException: it asked the removed search_objects_simple for the ids.

    [Fact]
    public async Task QueryDelete_MatchingAParentAndItsChild_CountsBoth()
    {
        var (_, child, grandchild, tag) = await SeedTaggedAsync();

        // Count 0 is the parent, 1 the child: a Props filter, so Pro reads it through the pivot.
        var deleted = await Redb.Query<SimpleProps>().Where(p => p.Title == tag && p.Count < 2).DeleteAsync();

        deleted.Should().Be(2, "the filter matched the parent and the child");
        (await Redb.LoadAsync<SimpleProps>(child)).Should().BeNull();
        (await Redb.LoadAsync<SimpleProps>(grandchild)).Should().BeNull("the descendant goes with its parent");
    }

    [Fact]
    public async Task QueryDelete_OnBaseFieldsOnly_MatchingAParentAndItsChild_CountsBoth()
    {
        var (parent, child, _, _) = await SeedTaggedAsync();

        var deleted = await Redb.Query<SimpleProps>().WhereRedb(o => o.Id == parent || o.Id == child).DeleteAsync();

        deleted.Should().Be(2);
    }

    [Fact]
    public async Task QueryDelete_MatchingNothing_CountsZero()
    {
        var (_, _, _, tag) = await SeedTaggedAsync();

        (await Redb.Query<SimpleProps>().Where(p => p.Title == tag && p.Count > 100).DeleteAsync()).Should().Be(0);
        (await Redb.Query<SimpleProps>().Where(p => p.Title == tag).CountAsync()).Should().Be(3, "nothing was deleted");
    }

    /// <summary>A tree query delete removes the nodes the tree query reads: here the child and the grandchild.</summary>
    [Fact]
    public async Task TreeQueryDelete_BelowTheParent_CountsTheNodesItReads()
    {
        var (parent, child, grandchild, tag) = await SeedTaggedAsync();

        var deleted = await Redb.TreeQuery<SimpleProps>(parent).Where(p => p.Title == tag && p.Count > 0).DeleteAsync();

        deleted.Should().Be(2);
        (await Redb.LoadAsync<SimpleProps>(parent)).Should().NotBeNull("the filter leaves the parent out (Count 0)");
        (await Redb.LoadAsync<SimpleProps>(child)).Should().BeNull();
        (await Redb.LoadAsync<SimpleProps>(grandchild)).Should().BeNull();
    }
}
