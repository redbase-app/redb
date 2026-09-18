using System.Transactions;
using Npgsql;
using redb.Core.Data;

namespace redb.Tests.Integration.Tests.Unit;

/// <summary>
/// Route request to core (docs/V4/ROUTE_REQUEST_TO_CORE_AMBIENT_TX.md, request 2; owner agreed, 2026-09-15): inside an
/// ambient transaction the deadlock victim's transaction is already rolled back by the server (SQL Server 1205,
/// PostgreSQL 40P01). Retrying the command inside it fails with a second error (25P02, aborted transaction) that
/// replaces the deadlock the unit of work needs to see. The helper retries only outside an ambient transaction.
/// </summary>
public class DeadlockRetryHelperAmbientTests
{
    private static PostgresException Deadlock() => new("deadlock detected", "ERROR", "ERROR", "40P01");

    [Fact]
    public async Task InsideAmbientTransaction_DeadlockIsNotRetried_TheOriginalLeaves()
    {
        var calls = 0;
        var deadlock = Deadlock();

        using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
        var act = () => DeadlockRetryHelper.ExecuteWithRetryAsync(() =>
        {
            calls++;
            return Task.FromException(deadlock);
        }, maxRetries: 3, baseDelayMs: 1);

        (await act.Should().ThrowAsync<PostgresException>()).Which.Should().BeSameAs(deadlock);
        calls.Should().Be(1, "the transaction is already rolled back on the server; a retry inside it cannot succeed");
    }

    [Fact]
    public async Task InsideAmbientTransaction_TypedOverload_DeadlockIsNotRetried()
    {
        var calls = 0;
        var deadlock = Deadlock();

        using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
        var act = () => DeadlockRetryHelper.ExecuteWithRetryAsync<int>(() =>
        {
            calls++;
            return Task.FromException<int>(deadlock);
        }, maxRetries: 3, baseDelayMs: 1);

        (await act.Should().ThrowAsync<PostgresException>()).Which.Should().BeSameAs(deadlock);
        calls.Should().Be(1);
    }

    [Fact]
    public async Task WithoutAmbientTransaction_DeadlockIsRetried()
    {
        var calls = 0;

        var result = await DeadlockRetryHelper.ExecuteWithRetryAsync(() =>
        {
            calls++;
            return calls == 1 ? Task.FromException<int>(Deadlock()) : Task.FromResult(42);
        }, maxRetries: 3, baseDelayMs: 1);

        result.Should().Be(42);
        calls.Should().Be(2, "a top-level operation owns its transaction and may run it again");
    }

    [Fact]
    public async Task UnderSuppressedScope_DeadlockIsRetried()
    {
        var calls = 0;

        using var outer = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
        using var suppressed = new TransactionScope(TransactionScopeOption.Suppress, TransactionScopeAsyncFlowOption.Enabled);
        await DeadlockRetryHelper.ExecuteWithRetryAsync(() =>
        {
            calls++;
            return calls == 1 ? Task.FromException(Deadlock()) : Task.CompletedTask;
        }, maxRetries: 3, baseDelayMs: 1);

        calls.Should().Be(2, "a suppressed scope has no ambient transaction: the operation owns its own");
    }
}
