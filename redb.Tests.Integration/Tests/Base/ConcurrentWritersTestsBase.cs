using Microsoft.Extensions.DependencyInjection;
using redb.Core;
using redb.Core.Exceptions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Many writers saving at the same instant through the public API, each on its own service scope (its own
/// connection), the way an application's concurrent requests do. Two shapes:
/// <list type="bullet">
/// <item>top-level saves of new objects (a key ring element per writer);</item>
/// <item>a lookup and an insert of the same unique key inside <c>ExecuteAtomicAsync</c> (a replay reservation:
/// exactly one writer may win, the others see the row or the unique violation).</item>
/// </list>
/// No writer may fail with anything else: a deadlock between writers is a failure of the save, not of the writer.
/// </summary>
public abstract class ConcurrentWritersTestsBase : IAsyncLifetime
{
    private const int Writers = 16;
    private const int Rounds = 5;

    private readonly IServiceProvider _services;

    protected ConcurrentWritersTestsBase(IServiceProvider services) => _services = services;

    public async Task InitializeAsync()
    {
        using var scope = _services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<IRedbService>().SyncSchemeAsync<ConcurrentWriterRowProps>();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    /// <summary>
    /// Starts <see cref="Writers"/> writers together: each gets its scope and service first, then all are released at
    /// once, so the saves overlap instead of running one after another as the tasks start.
    /// </summary>
    private async Task<T[]> RunTogetherAsync<T>(Func<IRedbService, int, Task<T>> write)
    {
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = Enumerable.Range(0, Writers).Select(i => Task.Run(async () =>
        {
            await using var scope = _services.CreateAsyncScope();
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            await start.Task;
            return await write(redb, i);
        })).ToArray();
        start.SetResult();
        return await Task.WhenAll(tasks);
    }

    [Fact]
    public async Task NewObjects_SavedConcurrently_AllSaved()
    {
        for (var round = 0; round < Rounds; round++)
        {
            var tag = $"new-{Guid.NewGuid():N}";
            var ids = await RunTogetherAsync((redb, i) => redb.SaveAsync(new RedbObject<ConcurrentWriterRowProps>
            {
                name = $"{tag}-{i}",
                Props = new ConcurrentWriterRowProps
                {
                    Tag = tag,
                    Key = $"{tag}-{i}",
                    Payload = new string('x', 200),
                    WrittenAt = DateTimeOffset.UtcNow
                }
            }));

            ids.Should().OnlyHaveUniqueItems().And.NotContain(0);
            var stored = await Query(tag);
            stored.Should().Be(Writers, $"round {round}: every writer's object is saved");
        }
    }

    [Fact]
    public async Task SameUniqueKey_ReservedConcurrentlyInsideExecuteAtomic_ExactlyOneWins()
    {
        for (var round = 0; round < Rounds; round++)
        {
            var tag = $"res-{Guid.NewGuid():N}";
            var key = $"{tag}-key";

            var won = await RunTogetherAsync((redb, _) => redb.Context.ExecuteAtomicAsync(async () =>
            {
                var existing = await redb.Query<ConcurrentWriterRowProps>()
                    .Where(o => o.Key == key)
                    .FirstOrDefaultAsync();
                if (existing is not null)
                    return false;

                try
                {
                    await redb.SaveAsync(new RedbObject<ConcurrentWriterRowProps>
                    {
                        name = key,
                        ValueUnique = key,
                        Props = new ConcurrentWriterRowProps { Tag = tag, Key = key, WrittenAt = DateTimeOffset.UtcNow }
                    });
                    return true;
                }
                catch (RedbUniqueViolationException)
                {
                    return false;
                }
            }));

            won.Count(w => w).Should().Be(1, $"round {round}: one reservation per key");
            (await Query(tag)).Should().Be(1);
        }
    }

    private async Task<int> Query(string tag)
    {
        using var scope = _services.CreateScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        return (await redb.Query<ConcurrentWriterRowProps>().Where(o => o.Tag == tag).ToListAsync()).Count;
    }
}
