using System.Data.Common;
using System.Transactions;
using Microsoft.Data.Sqlite;
using redb.Core.Data;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// The registry opens one connection per ambient transaction and database (review after 4.0.0, finding 5). The open ran
/// under one process-wide lock, and on SQLite the open includes <c>BEGIN IMMEDIATE</c> - a wait for the database write
/// lock up to the busy timeout - so one transaction waiting for one SQLite file held the first open of every other
/// transaction on every database. The lock is per transaction and database now.
/// </summary>
public class AmbientConnectionRegistryTests
{
    [Fact]
    public async Task OpeningForOneTransaction_DoesNotWaitBehindAnotherTransactionsOpen()
    {
        using var slowTx = new CommittableTransaction();
        using var fastTx = new CommittableTransaction();
        var tag = Guid.NewGuid().ToString("N");
        var slowStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var slow = AmbientConnectionRegistry.GetOrOpenAsync(slowTx, $"unit-slow-{tag}", "sig",
            async _ =>
            {
                slowStarted.SetResult();
                await release.Task;
                return (DbConnection)new SqliteConnection("Data Source=:memory:");
            },
            beginOwnTransaction: null);
        await slowStarted.Task;

        var fast = AmbientConnectionRegistry.GetOrOpenAsync(fastTx, $"unit-fast-{tag}", "sig",
            _ => Task.FromResult((DbConnection)new SqliteConnection("Data Source=:memory:")),
            beginOwnTransaction: null);
        var first = await Task.WhenAny(fast, Task.Delay(TimeSpan.FromSeconds(2)));

        release.SetResult();
        await slow;
        first.Should().BeSameAs(fast,
            "the open of one transaction's connection (SQLite waiting for its write lock) must not hold the first open " +
            "of every other transaction");
        await fast;

        slowTx.Rollback();
        fastTx.Rollback();
    }
}
