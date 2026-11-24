using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Core.Query;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A save locks the rows of the objects that existed before it (ObjectStorage_LockObjectsForUpdate) and nothing else.
/// The ids of the objects it creates are given out before the write, and the save used to lock them too: rows that do
/// not exist yet, and on SQL Server a lock statement that then read the rows of concurrent writers and deadlocked
/// writers of disjoint objects. The statements are seen through the test context.
/// </summary>
public abstract class SaveLockScopeTestsBase
{
    private readonly SqlRecorder _recorder = new();

    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build()
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c => c.CacheDomain = $"save-lock-{GetType().Name}-{Guid.NewGuid():N}");
        });
        RecordingRedbContext.Decorate(services, _recorder);
        return services.BuildServiceProvider();
    }

    private async Task<(ServiceProvider Host, IRedbService Redb, string LockSql)> StartAsync()
    {
        var host = Build();
        var redb = host.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<ConcurrentWriterRowProps>();
        return (host, redb, host.GetRequiredService<ISqlDialect>().ObjectStorage_LockObjectsForUpdate());
    }

    private static RedbObject<ConcurrentWriterRowProps> NewRow(string tag) => new()
    {
        name = tag,
        Props = new ConcurrentWriterRowProps { Tag = tag, Key = tag, Payload = "p", WrittenAt = DateTimeOffset.UtcNow }
    };

    private int LockStatements(IReadOnlyList<RecordedCommand> commands, string lockSql)
        => commands.Count(c => c.Sql == lockSql);

    [Fact]
    public async Task NewObject_TopLevelSave_TakesNoLock()
    {
        var (host, redb, lockSql) = await StartAsync();
        await using var _ = host;

        _recorder.Start();
        var id = await redb.SaveAsync(NewRow($"lock-new-{Guid.NewGuid():N}"));
        var commands = _recorder.Stop();

        id.Should().BePositive();
        LockStatements(commands, lockSql).Should().Be(0, "an object this save creates has no row to lock yet");
    }

    [Fact]
    public async Task NewObject_SaveInsideExecuteAtomic_TakesNoLock()
    {
        var (host, redb, lockSql) = await StartAsync();
        await using var _ = host;

        _recorder.Start();
        await redb.Context.ExecuteAtomicAsync(() => redb.SaveAsync(NewRow($"lock-atomic-{Guid.NewGuid():N}")));
        var commands = _recorder.Stop();

        LockStatements(commands, lockSql).Should().Be(0, "inside a transaction too, a new object has no row to lock");
    }

    [Fact]
    public async Task ExistingObject_Resave_LocksItsRow()
    {
        var (host, redb, lockSql) = await StartAsync();
        await using var _ = host;
        var tag = $"lock-old-{Guid.NewGuid():N}";
        var id = await redb.SaveAsync(NewRow(tag));
        var loaded = (await redb.LoadAsync<ConcurrentWriterRowProps>(id))!;
        loaded.Props.Payload = "changed";

        _recorder.Start();
        await redb.SaveAsync(loaded);
        var commands = _recorder.Stop();

        LockStatements(commands, lockSql).Should().Be(1, "an existing object's row is locked before it is rewritten");
    }
}
