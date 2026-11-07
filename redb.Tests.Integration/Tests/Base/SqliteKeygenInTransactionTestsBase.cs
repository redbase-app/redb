using System.Diagnostics;
using redb.Core;
using redb.Core.Data;
using redb.Tests.Integration.Helpers;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The SQLite single-writer keygen trap (live worker storm, 2026-09-08): the id cache is
/// process memory and starts EMPTY, so the first save after startup must fetch a block from
/// the database - a write. When that save runs inside the caller's own BEGIN IMMEDIATE
/// (ExecuteAtomicAsync around SaveAsync, the Tsak heartbeat shape), the old refill went
/// through a SEPARATE pooled connection and waited for the file's only write lock - held by
/// the very transaction waiting for the refill. Self-deadlock, unwound only by busy_timeout
/// x retries (~15-20s), taking Quartz and every other writer down with it. The fix: inside
/// an active scope transaction keys come through that same connection (the lock is already
/// ours), bypassing the shared cache - a rollback takes the sequence bump back and those ids
/// were never visible outside, so no duplicates on either outcome.
/// </summary>
public abstract class SqliteKeygenInTransactionTestsBase : IAsyncLifetime
{
    protected readonly IRedbService Redb;

    protected SqliteKeygenInTransactionTestsBase(IRedbService redb) => Redb = redb;

    public async Task InitializeAsync() => await ResetAsync();
    public async Task DisposeAsync() => await ResetAsync();

    private async Task ResetAsync()
        => await Redb.Context.ExecuteAsync("DELETE FROM _objects WHERE _name LIKE 'keygen-tx-%'");

    [Fact]
    public async Task SaveInsideUserTransaction_SurvivesColdKeyCache()
    {
        await Redb.SyncSchemeAsync<SimpleProps>();

        // Cold start: the shared id cache is empty, exactly like a freshly launched worker.
        RedbKeyGeneratorBase.ClearAllCaches();

        var sw = Stopwatch.StartNew();
        await Redb.Context.ExecuteAtomicAsync(async () =>
        {
            var obj = TestDataFactory.CreateSimple("keygen-tx-cold", 1m);
            await Redb.SaveAsync(obj);
        });
        sw.Stop();

        // The old path self-deadlocked here and either threw 'database is locked' or crawled
        // through the busy_timeout ladder; the ambient bypass must make this instant.
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(4),
            "keys inside the scope's own transaction must come through it, not wait on its lock");

        var count = await Redb.Context.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM _objects WHERE _name = 'keygen-tx-cold'");
        count.Should().Be(1);
    }

    [Fact]
    public async Task AmbientKeysAfterRollback_DoNotCollide()
    {
        // A rollback takes the sequence bump back; the next writer may receive the same ids
        // again - which is correct, nothing outside the transaction ever saw them. The pin:
        // subsequent saves succeed with no primary-key collision and no shared-cache poison.
        await Redb.SyncSchemeAsync<SimpleProps>();
        RedbKeyGeneratorBase.ClearAllCaches();

        var act = async () => await Redb.Context.ExecuteAtomicAsync(async () =>
        {
            await Redb.SaveAsync(TestDataFactory.CreateSimple("keygen-tx-rollback", 2m));
            throw new InvalidOperationException("keygen-rollback-marker");
        });
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*keygen-rollback-marker*");

        (await Redb.Context.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM _objects WHERE _name = 'keygen-tx-rollback'"))
            .Should().Be(0, "the marker exception rolled the save back");

        // Cold cache again so the next ids are re-read from the rolled-back sequence.
        RedbKeyGeneratorBase.ClearAllCaches();
        var id = await Redb.SaveAsync(TestDataFactory.CreateSimple("keygen-tx-after", 3m));
        id.Should().BeGreaterThan(0, "re-issued ids are free - the rollback returned them unused");
    }
}
