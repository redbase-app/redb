using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A save of a graph that is not a tree (review after 4.0.0, finding 3). The parent's hash carries "id:hash" of every
/// reference, so a reference must be hashed before its parents. The collector is pre-order and collects a target shared
/// by two parents once, under the first; hashing in reverse collection order put the second parent before the target,
/// and its stored hash carried the target's stale hash for ever - a props-cache miss on every load. The same collector
/// never deduplicated a new object: one instance referenced twice was collected, given an id and inserted twice.
/// </summary>
public abstract class SaveGraphHashTestsBase
{
    protected readonly IRedbService Redb;

    protected SaveGraphHashTestsBase(IRedbService redb) => Redb = redb;

    private static string NewTag() => Guid.NewGuid().ToString("N")[..8];

    private static RedbObject<GraphNodeProps> Node(string name, string title, RedbObject<GraphNodeProps>? left = null, RedbObject<GraphNodeProps>? right = null)
        => new() { name = name, Props = new GraphNodeProps { Title = title, Left = left, Right = right } };

    [Fact]
    public async Task TwoNewParents_SharingAnEditedTarget_StoreTheHashTheLoadRecomputes()
    {
        await Redb.SyncSchemeAsync<GraphNodeProps>();
        var tag = NewTag();

        var targetId = await Redb.SaveAsync(Node($"graph-target-{tag}", "v1"));
        var target = (await Redb.LoadAsync<GraphNodeProps>(targetId))!;
        // Edited: its stored hash is stale, the save recomputes it - and every parent must hash it after that.
        target.Props.Title = "v2";

        var left = Node($"graph-left-{tag}", "left", left: target);
        var right = Node($"graph-right-{tag}", "right", left: target);
        await Redb.SaveAsync(Node($"graph-root-{tag}", "root", left: left, right: right));

        foreach (var id in new[] { left.id, right.id })
        {
            var loaded = (await Redb.LoadAsync<GraphNodeProps>(id))!;
            loaded.hash.Should().Be(loaded.ComputeHash(),
                $"the stored hash of '{loaded.name}' must be the hash the loaded object recomputes - the props cache and " +
                "the ChangeTracking shortcut compare exactly these two");
        }
    }

    [Fact]
    public async Task ANewObject_ReferencedByTwoParents_IsSavedOnce()
    {
        await Redb.SyncSchemeAsync<GraphNodeProps>();
        var tag = NewTag();

        var shared = Node($"graph-shared-{tag}", "shared");
        var a = Node($"graph-a-{tag}", "a", left: shared);
        var b = Node($"graph-b-{tag}", "b", left: shared);
        await Redb.SaveAsync(Node($"graph-root2-{tag}", "root", left: a, right: b));

        var rows = await Redb.Context.ExecuteScalarAsync<long>(
            $"SELECT COUNT(*) FROM _objects WHERE _name = 'graph-shared-{tag}'");
        rows.Should().Be(1, "one instance is one object, however many parents reference it");

        var loadedA = (await Redb.LoadAsync<GraphNodeProps>(a.id))!;
        var loadedB = (await Redb.LoadAsync<GraphNodeProps>(b.id))!;
        loadedA.Props.Left!.id.Should().Be(loadedB.Props.Left!.id, "both parents reference the one saved object");
    }
}
