using FluentAssertions;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Fixtures;
using redb.Tests.Integration.Models;
using Xunit;

namespace redb.Tests.Integration.Tests.Sqlite;

/// <summary>
/// DeleteSubtree ran its two DELETEs as separate statements, so a live FK reference into the
/// subtree made the second one fail **after** the first had committed: the objects survived with
/// every value already gone (DataLoss p.2). Both statements now share one transaction, so the
/// failure leaves the subtree untouched.
/// </summary>
[Collection("Sqlite")]
public class SqliteDeleteSubtreeAtomicityTests(SqliteFixture fixture)
{
    private async Task<(long RootId, long ChildId)> SeedReferencedSubtreeAsync()
    {
        var redb = fixture.Redb;
        await redb.SyncSchemeAsync<GraphNodeProps>();

        var root = new RedbObject<GraphNodeProps> { name = "root", Props = new GraphNodeProps { Title = "root" } };
        await redb.SaveAsync(root);

        var child = new RedbObject<GraphNodeProps> { name = "child", Props = new GraphNodeProps { Title = "child" } };
        child.parent_id = root.id;
        await redb.SaveAsync(child);

        // A live object outside the subtree holds a reference to the child, so deleting the child
        // row would violate _values._Object.
        var referer = new RedbObject<GraphNodeProps>
        {
            name = "referer",
            Props = new GraphNodeProps { Title = "referer", Left = child }
        };
        await redb.SaveAsync(referer);

        return (root.id, child.id);
    }

    [Fact]
    public async Task DeleteSubtree_WithLiveReferenceIntoSubtree_LeavesEverythingInPlace()
    {
        var redb = fixture.Redb;
        var (rootId, childId) = await SeedReferencedSubtreeAsync();
        var root = await redb.LoadAsync<GraphNodeProps>(rootId);

        var act = async () => await redb.DeleteSubtreeAsync(root!);

        // The reference blocks the object delete, and because both statements now share a
        // transaction, the failure also undoes the values delete.
        await act.Should().ThrowAsync<Exception>();

        var reloadedChild = await redb.LoadAsync<GraphNodeProps>(childId);
        reloadedChild.Should().NotBeNull();
        reloadedChild!.Props.Title.Should().Be("child", "the rolled-back delete must not strip the subtree of its values");

        var reloadedRoot = await redb.LoadAsync<GraphNodeProps>(rootId);
        reloadedRoot.Should().NotBeNull("the root itself must still be there after the failed delete");
    }
}
