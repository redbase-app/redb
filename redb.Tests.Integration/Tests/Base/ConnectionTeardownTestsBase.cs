using Microsoft.Extensions.Configuration;
using redb.Core.Data;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Scope-teardown contract of <see cref="IRedbConnection"/> (tsum garage report, 2026-09-11):
/// disposing a connection must not race an in-flight command. The reported shape: an HTTP
/// exchange's ReleaseScopes() hit <c>NpgsqlOperationInProgressException</c> because a
/// SEDA-side lazy loader was still reading through the dying scope's connection - the
/// command guard existed, but DisposeAsync was exempt from it.
///
/// Pinned contract: (1) DisposeAsync WAITS for the running command, both complete cleanly;
/// (2) a command entered after teardown is refused with <see cref="ObjectDisposedException"/> -
/// the lazy loader's cue to fall back to a detached scope.
/// </summary>
public abstract class ConnectionTeardownTestsBase
{
    /// <summary>A fresh, self-owned connection - never the fixture's shared one.</summary>
    protected abstract IRedbConnection CreateConnection();

    /// <summary>A scalar query that runs for roughly the given time on this provider.</summary>
    protected abstract string SleepSql(double seconds);

    protected virtual string ProbeSql => "SELECT 1";

    protected static string ConnString(string name) =>
        new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .Build()
            .GetConnectionString(name)!;

    [Fact]
    public async Task Dispose_WaitsForTheInFlightCommand()
    {
        var conn = CreateConnection();
        var clock = System.Diagnostics.Stopwatch.StartNew();
        long commandDoneAt = 0;

        var command = Task.Run(async () =>
        {
            await conn.ExecuteScalarAsync<long?>(SleepSql(1.5));
            Interlocked.Exchange(ref commandDoneAt, clock.ElapsedMilliseconds);
        });

        await Task.Delay(300); // the command is reliably in flight by now

        // Capped awaits: the un-fixed teardown can HANG under a running command (worse than
        // the reported exception) - a hang must fail the pin, not the whole run.
        var dispose = conn.DisposeAsync().AsTask();
        (await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromSeconds(20)))).Should().Be(dispose,
            "teardown must complete once the in-flight command finishes, never hang");
        await dispose; // propagate a teardown fault (the reported OperationInProgress shape)
        var disposeDoneAt = clock.ElapsedMilliseconds;

        // The command must complete cleanly - not die under a concurrent Reset/Close.
        (await Task.WhenAny(command, Task.Delay(TimeSpan.FromSeconds(20)))).Should().Be(command);
        await command;

        var cmdAt = Interlocked.Read(ref commandDoneAt);
        cmdAt.Should().BeGreaterThan(0);
        disposeDoneAt.Should().BeGreaterThanOrEqualTo(cmdAt,
            "teardown must wait for the in-flight command instead of racing it");
    }

    [Fact]
    public async Task Command_AfterDispose_IsRefusedWithObjectDisposed()
    {
        var conn = CreateConnection();
        await conn.ExecuteScalarAsync<long?>(ProbeSql); // open the physical connection for real
        await conn.DisposeAsync();

        var act = async () => await conn.ExecuteScalarAsync<long?>(ProbeSql);
        await act.Should().ThrowAsync<ObjectDisposedException>(
            "a command entered after teardown must be refused cleanly - the lazy loader " +
            "falls back to a detached scope on this signal");
    }
}
