using redb.Core;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The cancellation contract (CANCELLATION_PLAN, wave B5): cancellation surfaces as
/// <see cref="OperationCanceledException"/> and nothing else; a cancelled save leaves the
/// database untouched (the rollback always completes, s3.2/s3.4); after a successful commit
/// the token is never consulted again (s3.3). A pre-cancelled token is the deterministic
/// probe - it must stop every verb before the first write. The in-flight tests are
/// provider-specific (PG pg_sleep / MSSQL WAITFOR) and hang off <see cref="SleepSql"/>.
/// </summary>
public abstract class CancellationTestsBase : IAsyncLifetime
{
    protected readonly IRedbService Redb;

    protected CancellationTestsBase(IRedbService redb) => Redb = redb;

    /// <summary>A statement that sleeps ~30s server-side, or null when the engine cannot sleep (SQLite).</summary>
    protected virtual string? SleepSql => null;

    public async Task InitializeAsync() => await ResetAsync();
    public async Task DisposeAsync() => await ResetAsync();

    private async Task ResetAsync()
        => await Redb.Context.ExecuteAsync("DELETE FROM _objects WHERE _name LIKE 'ct-probe-%'");

    private static CancellationToken Cancelled()
    {
        var cts = new CancellationTokenSource();
        cts.Cancel();
        return cts.Token;
    }

    private Task<long> CountByNameAsync(string name)
        => Redb.Context.ExecuteScalarAsync<long>(
            $"SELECT COUNT(*) FROM _objects WHERE _name = '{name}'");

    [Fact]
    public async Task PreCancelledSave_ThrowsOce_AndWritesNothing()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        var obj = TestDataFactory.CreateSimple("ct-probe-save", 5m);

        var act = async () => await Redb.SaveAsync(obj, Cancelled());
        await act.Should().ThrowAsync<OperationCanceledException>(
            "cancellation has exactly one shape - OperationCanceledException");

        (await CountByNameAsync("ct-probe-save")).Should().Be(0,
            "a cancelled save must leave no trace in the database");
    }

    [Fact]
    public async Task PreCancelledBatchSave_ThrowsOce_AndWritesNothing()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        var batch = Enumerable.Range(0, 20)
            .Select(i => (Core.Models.Contracts.IRedbObject)TestDataFactory.CreateSimple($"ct-probe-batch-{i}", i))
            .ToList();

        var act = async () => await Redb.SaveAsync(batch, Cancelled());
        await act.Should().ThrowAsync<OperationCanceledException>();

        var count = await Redb.Context.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM _objects WHERE _name LIKE 'ct-probe-batch-%'");
        count.Should().Be(0, "the batch is atomic: cancelled means nothing landed");
    }

    [Fact]
    public async Task PreCancelledLoad_ThrowsOce()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        var id = await Redb.SaveAsync(TestDataFactory.CreateSimple("ct-probe-load", 1m));

        var act = async () => await Redb.LoadAsync<SimpleProps>(id, depth: 1, cancellationToken: Cancelled());
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task PreCancelledToList_ThrowsOce()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        await Redb.SaveAsync(TestDataFactory.CreateSimple("ct-probe-query", 2m));

        var act = async () => await Redb.Query<SimpleProps>().ToListAsync(Cancelled());
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task PreCancelledDelete_ThrowsOce_AndTheRowSurvives()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        var id = await Redb.SaveAsync(TestDataFactory.CreateSimple("ct-probe-delete", 3m));

        var act = async () => await Redb.DeleteAsync(id, Cancelled());
        await act.Should().ThrowAsync<OperationCanceledException>();

        (await CountByNameAsync("ct-probe-delete")).Should().Be(1,
            "the DELETE never reached the database - the row must survive");
    }

    [Fact]
    public async Task PreCancelledPurgeTrash_ThrowsOce()
    {
        // Unified cancellation semantics (owner decision, 2026-09-08): PurgeTrashAsync gave up
        // its historical graceful return (a Cancelled progress report and a soft exit) - like
        // every other verb it now surfaces OCE. The farewell progress report survives.
        var act = async () => await Redb.PurgeTrashAsync(trashId: -1, totalCount: 0, cancellationToken: Cancelled());
        await act.Should().ThrowAsync<OperationCanceledException>(
            "a pre-cancelled purge must throw like every other cancelled verb, not return quietly");
    }

    [Fact]
    public async Task PreCancelledDeleteWithPurge_ThrowsOce_AndNothingIsMarked()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();
        var id = await Redb.SaveAsync(TestDataFactory.CreateSimple("ct-probe-purge", 4m));

        var act = async () => await Redb.DeleteWithPurgeAsync(new[] { id }, cancellationToken: Cancelled());
        await act.Should().ThrowAsync<OperationCanceledException>();

        (await CountByNameAsync("ct-probe-purge")).Should().Be(1,
            "the cancelled call must not even mark the object for deletion");
    }

    [Fact]
    public async Task InFlightCancel_BreaksALongStatement_Quickly()
    {
        if (SleepSql is null)
            return; // the engine has no server-side sleep (SQLite) - covered by the pre-cancelled probes

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var sw = System.Diagnostics.Stopwatch.StartNew();

        var act = async () => await Redb.Context.ExecuteAsync(SleepSql, System.Array.Empty<object>(), cts.Token);
        await act.Should().ThrowAsync<OperationCanceledException>(
            "an in-flight command must be torn down by the token, not awaited to completion");
        sw.Stop();

        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(15),
            "cancellation must interrupt the ~30s sleep long before it finishes");
    }

    [Fact]
    public async Task InFlightCancelledBatch_LeavesNoTrace_OrCommitsWhole()
    {
        // A racing cancel: either the save finished first (every object is in), or the OCE
        // won and the transaction rolled back (nothing is in). Any in-between state is the
        // failure this test exists to catch (s3.2: atomicity survives cancellation).
        await Redb.SyncSchemeAsync<SimpleProps>();
        const int n = 60;
        var batch = Enumerable.Range(0, n)
            .Select(i => (Core.Models.Contracts.IRedbObject)TestDataFactory.CreateSimple($"ct-probe-race-{i}", i))
            .ToList();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(20));
        var cancelled = false;
        try
        {
            await Redb.SaveAsync(batch, cts.Token);
        }
        catch (OperationCanceledException)
        {
            cancelled = true;
        }

        var count = await Redb.Context.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM _objects WHERE _name LIKE 'ct-probe-race-%'");
        if (cancelled)
            count.Should().Be(0, "a cancelled batch save must roll back completely");
        else
            count.Should().Be(n, "an uncancelled save must have committed every object");
    }
}
