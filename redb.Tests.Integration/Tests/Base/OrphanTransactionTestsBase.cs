using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A command whose SQL text opens a transaction and fails before ending it must not leave that
/// transaction on the connection (trash review, 2026-09-14). The connection wrapper does not own such a
/// transaction, so nothing would ever end it:
/// <list type="bullet">
///   <item>MSSQL - every later statement of the scope ran inside it and vanished with it when the pooled
///   session was reset: a save reported success and its row was gone;</item>
///   <item>SQLite - the handle went back to the pool holding the database write lock, and every writer
///   of the process failed with "database is locked" for as long as that handle sat in the pool;</item>
///   <item>PostgreSQL - the scope was stuck in an aborted transaction block and refused its next
///   transaction.</item>
/// </list>
/// The connection layer is tier-independent, so the Free collections cover it.
/// </summary>
public abstract class OrphanTransactionTestsBase
{
    private readonly IServiceProvider _services;

    protected OrphanTransactionTestsBase(IServiceProvider services) => _services = services;

    /// <summary>A batch that opens a transaction in its own text and then fails.</summary>
    protected abstract string FailingBatchThatOpensATransaction { get; }

    [Fact]
    public async Task FailedCommand_LeavesNoTransactionBehind()
    {
        var tag = Guid.NewGuid().ToString("N")[..12];

        // A second scope with a live connection BEFORE the failure: a pooled handle that keeps a write
        // lock stays invisible to a scope that happens to receive that same handle again.
        await using var other = _services.CreateAsyncScope();
        var otherRedb = other.ServiceProvider.GetRequiredService<IRedbService>();
        await otherRedb.SyncSchemeAsync<TrashTargetProps>();

        await using (var scope = _services.CreateAsyncScope())
        {
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            var act = async () => await redb.Context.ExecuteAsync(FailingBatchThatOpensATransaction);
            await act.Should().ThrowAsync<Exception>("the batch is built to fail");

            // The caller handles the failure and keeps working in the same scope.
            await redb.SaveAsync(new RedbObject<TrashTargetProps>
                { name = $"orphan-same-{tag}", Props = new TrashTargetProps { Title = "same" } });
        }

        var clock = Stopwatch.StartNew();
        await otherRedb.SaveAsync(new RedbObject<TrashTargetProps>
            { name = $"orphan-other-{tag}", Props = new TrashTargetProps { Title = "other" } });
        clock.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(4),
            "no connection may keep holding a lock after its command failed");

        (await otherRedb.Context.ExecuteScalarAsync<long>($"SELECT COUNT(*) FROM _objects WHERE _name = 'orphan-same-{tag}'"))
            .Should().Be(1, "the save made after the failure is committed, not held in a transaction the failed batch left open");
    }
}
