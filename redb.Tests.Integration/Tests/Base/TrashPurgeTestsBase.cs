using System.Diagnostics;
using System.Transactions;
using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;
using redb.Core.Services;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Purging a trash container whose objects are referenced (trash review, 2026-09-14). A
/// <c>RedbObject&lt;T&gt;</c> reference is a <c>_values._Object</c> row with a foreign key and no
/// ON DELETE action, so the physical delete of a referenced object fails on every provider. Before the
/// fix one such object failed the whole batch, the container stayed 'running' and the background
/// worker retried it every 30 minutes forever; on SQLite the failed batch also left its transaction and
/// the database write lock on the pooled handle.
/// <list type="bullet">
///   <item>A reference held by an object that is itself in the trash dies with it - it never blocks.</item>
///   <item>A reference held by a live object blocks: the purge removes everything else, marks the container
///   'failed' and throws <see cref="RedbObjectReferencedException"/> naming both sides. redb does not null
///   the reference - that would hide the caller's bug inside another object's data.</item>
/// </list>
/// </summary>
public abstract class TrashPurgeTestsBase
{
    private readonly IServiceProvider _services;

    protected TrashPurgeTestsBase(IServiceProvider services) => _services = services;

    private static string NewTag() => Guid.NewGuid().ToString("N")[..12];

    private static async Task SyncAsync(IRedbService redb)
    {
        await redb.SyncSchemeAsync<TrashTargetProps>();
        await redb.SyncSchemeAsync<TrashHolderProps>();
    }

    private static RedbObject<TrashTargetProps> Target(string tag, string role) =>
        new() { name = $"trash-{role}-{tag}", Props = new TrashTargetProps { Title = role } };

    private static Task<long> CountAsync(IRedbService redb, string where) =>
        redb.Context.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM _objects WHERE {where}");

    /// <summary>
    /// Whether a second connection may write while a transaction holds rows. SQLite has one writer: a purge waiting for
    /// the lock a transaction holds is the classical SQLite deadlock (the transaction cannot commit past the purge's open
    /// read), so there the race of <see cref="Purge_BatchDeletedByAConcurrentPurger_IsRetried_NotFailed"/> is played
    /// sequentially - the same rows gone and moved before the purge, no lock held meanwhile.
    /// </summary>
    protected virtual bool ConcurrentWriters => true;

    [Fact]
    public async Task Purge_ObjectReferencedOnlyFromTheTrash_Completes()
    {
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await SyncAsync(redb);
        var tag = NewTag();

        var targetId = await redb.SaveAsync(Target(tag, "target"));
        var holderId = await redb.SaveAsync(new RedbObject<TrashHolderProps>
        {
            name = $"trash-holder-{tag}",
            Props = new TrashHolderProps { Title = tag, Target = new RedbObject<TrashTargetProps> { id = targetId } }
        });

        var holderTrash = await redb.SoftDeleteAsync(new[] { holderId });
        var targetTrash = await redb.SoftDeleteAsync(new[] { targetId });

        // The target's container first: its only reference is held by an object already in the trash.
        await redb.PurgeTrashAsync(targetTrash.TrashId, targetTrash.MarkedCount);
        await redb.PurgeTrashAsync(holderTrash.TrashId, holderTrash.MarkedCount);

        (await CountAsync(redb, $"_id IN ({targetId}, {holderId})")).Should().Be(0,
            "a reference held by a trashed object dies with it and never blocks the purge");
    }

    [Fact]
    public async Task Purge_ObjectReferencedByALiveObject_FailsTheContainer_AndPurgesTheRest()
    {
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await SyncAsync(redb);
        var tag = NewTag();

        var keptId = await redb.SaveAsync(Target(tag, "kept"));
        var freeId = await redb.SaveAsync(Target(tag, "free"));
        var mark = await redb.SoftDeleteAsync(new[] { keptId, freeId });

        // A live reference to an object already in the trash (a concurrent writer, or code that saved a
        // reference it should not have).
        var holderId = await redb.SaveAsync(new RedbObject<TrashHolderProps>
        {
            name = $"trash-holder-{tag}",
            Props = new TrashHolderProps { Title = tag, Target = new RedbObject<TrashTargetProps> { id = keptId } }
        });

        var act = async () => await redb.PurgeTrashAsync(mark.TrashId, mark.MarkedCount);
        var thrown = await act.Should().ThrowAsync<RedbObjectReferencedException>(
            "a live reference must surface loudly, with both sides named");
        thrown.Which.TrashId.Should().Be(mark.TrashId);
        thrown.Which.RemainingCount.Should().Be(1);
        thrown.Which.ReferencedObjectIds.Should().Contain(keptId);
        thrown.Which.ReferencingObjectIds.Should().Contain(holderId);

        (await CountAsync(redb, $"_id = {freeId}")).Should().Be(0, "everything nobody references is purged");
        (await CountAsync(redb, $"_id = {keptId} AND _id_scheme = -10")).Should().Be(1,
            "the referenced object stays in the trash - the reference is not nulled behind the caller's back");
        (await CountAsync(redb, $"_id = {holderId} AND _id_scheme <> -10")).Should().Be(1, "the holder is untouched");
        (await redb.GetDeletionProgressAsync(mark.TrashId))!.Status.Should().Be(PurgeStatus.Failed);
        (await redb.GetOrphanedDeletionTasksAsync(30)).Should().NotContain(t => t.TrashId == mark.TrashId,
            "a failed container is not retried by the background worker");

        // Nothing of the failed purge is left on any connection.
        await redb.SaveAsync(Target(tag, "same-scope-after"));
        await using var other = _services.CreateAsyncScope();
        var clock = Stopwatch.StartNew();
        await other.ServiceProvider.GetRequiredService<IRedbService>().SaveAsync(Target(tag, "other-scope-after"));
        clock.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(4), "no connection keeps a lock after the purge");
        (await CountAsync(redb, $"_name LIKE 'trash-%-scope-after-{tag}'")).Should().Be(2);
    }

    [Fact]
    public async Task Purge_CollectionElementReferenceFromALiveObject_BlocksLikeASingleReference()
    {
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await SyncAsync(redb);
        var tag = NewTag();

        var targetId = await redb.SaveAsync(Target(tag, "element"));
        var mark = await redb.SoftDeleteAsync(new[] { targetId });
        var holderId = await redb.SaveAsync(new RedbObject<TrashHolderProps>
        {
            name = $"trash-holder-{tag}",
            Props = new TrashHolderProps { Title = tag, Targets = [new RedbObject<TrashTargetProps> { id = targetId }] }
        });

        var act = async () => await redb.PurgeTrashAsync(mark.TrashId, mark.MarkedCount);
        var thrown = await act.Should().ThrowAsync<RedbObjectReferencedException>();
        thrown.Which.ReferencingObjectIds.Should().Contain(holderId);
        (await CountAsync(redb, $"_id = {targetId}")).Should().Be(1);
    }

    /// <summary>
    /// Two purgers on one container - the background worker beside a caller's PurgeTrashAsync, two cluster nodes: a
    /// batch chosen by one and deleted by the other meanwhile deleted nothing here, and "nothing deleted while objects
    /// remain" was read as "every remaining object is referenced by a live object". The container was marked 'failed'
    /// with nothing blocking it, the worker stopped claiming it, and the caller got a
    /// <see cref="RedbObjectReferencedException"/> naming no referrer. The race is played by a transaction that deletes
    /// the container's objects, moves one more in, and commits only once the purge waits on the rows it holds. On
    /// PostgreSQL the purge chooses its batch from the snapshot and then waits on the delete; SQL Server and SQLite
    /// wait while choosing and never see the race - there the fact holds as a plain concurrent purge.
    /// </summary>
    [Fact]
    public async Task Purge_BatchDeletedByAConcurrentPurger_IsRetried_NotFailed()
    {
        await using var scope = _services.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await SyncAsync(redb);
        var tag = NewTag();

        var ids = new List<long>();
        for (var i = 0; i < 6; i++)
            ids.Add(await redb.SaveAsync(Target(tag, $"race-{i}")));
        var mark = await redb.SoftDeleteAsync(ids);
        var lateId = await redb.SaveAsync(Target(tag, "race-late"));

        await using var otherScope = _services.CreateAsyncScope();
        var other = otherScope.ServiceProvider.GetRequiredService<IRedbService>();
        var purge = Task.CompletedTask;
        await other.Context.ExecuteAtomicAsync(async () =>
        {
            var batch = string.Join(", ", ids);
            await other.Context.ExecuteAsync($"DELETE FROM _values WHERE _id_object IN ({batch})");
            await other.Context.ExecuteAsync($"DELETE FROM _objects WHERE _id IN ({batch})");
            await other.Context.ExecuteAsync($"UPDATE _objects SET _id_parent = {mark.TrashId} WHERE _id = {lateId}");
            if (!ConcurrentWriters)
                return;

            // The purge runs on its own scope, outside this transaction's flow, and blocks on the rows held here.
            using (new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled))
                purge = redb.PurgeTrashAsync(mark.TrashId, mark.MarkedCount, batchSize: ids.Count);
            await Task.WhenAny(purge, Task.Delay(TimeSpan.FromSeconds(2)));
        });
        if (!ConcurrentWriters)
            purge = redb.PurgeTrashAsync(mark.TrashId, mark.MarkedCount, batchSize: ids.Count);

        await purge;

        (await CountAsync(redb, $"_id_parent = {mark.TrashId}")).Should().Be(0, "the purge goes on with the next batch");
        (await CountAsync(redb, $"_id = {mark.TrashId}")).Should().Be(0, "a completed container is removed");
    }
}
