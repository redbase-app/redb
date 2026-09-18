using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Core.Services;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// The background deletion worker against a trash container it can never finish (trash review,
/// 2026-09-14): a live object references one of the container's objects. The container must end up
/// 'failed' - no longer claimed every 30 minutes - while the worker keeps purging the other containers.
/// The worker drains EVERY container of its database, so the suite runs on a throwaway database of its
/// own instead of the shared fixtures, where it would purge the trash other suites are asserting on.
/// </summary>
public abstract class TrashBackgroundServiceTestsBase
{
    /// <summary>Creates an empty database and returns its connection string.</summary>
    protected abstract Task<string> CreateEmptyDatabaseAsync();

    /// <summary>Drops the database created by <see cref="CreateEmptyDatabaseAsync"/>.</summary>
    protected abstract Task DropDatabaseAsync(string connectionString);

    protected abstract void UseProvider(RedbOptionsBuilder options, string connectionString);

    protected static string ConnString(string name) =>
        new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .Build()
            .GetConnectionString(name)!;

    private ServiceProvider Build(string connectionString)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddRedb(options =>
        {
            UseProvider(options, connectionString);
            options.Configure(c =>
            {
                c.EnablePropsCache = false;
                c.CacheDomain = $"trash-background-{GetType().Name}";
            });
        });
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task PoisonedContainer_EndsFailed_AndTheWorkerKeepsPurgingTheOthers()
    {
        var cs = await CreateEmptyDatabaseAsync();
        try
        {
            await using var sp = Build(cs);
            long poisonedTrash, goodTrash;
            await using (var scope = sp.CreateAsyncScope())
            {
                var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
                await redb.InitializeAsync(ensureCreated: true);
                await redb.SyncSchemeAsync<TrashTargetProps>();
                await redb.SyncSchemeAsync<TrashHolderProps>();

                var keptId = await redb.SaveAsync(new RedbObject<TrashTargetProps> { name = "kept", Props = new TrashTargetProps { Title = "kept" } });
                var goodId = await redb.SaveAsync(new RedbObject<TrashTargetProps> { name = "good", Props = new TrashTargetProps { Title = "good" } });

                // The poisoned container is the older one, so the worker meets it first in its page.
                poisonedTrash = (await redb.SoftDeleteAsync(new[] { keptId })).TrashId;
                goodTrash = (await redb.SoftDeleteAsync(new[] { goodId })).TrashId;

                await redb.SaveAsync(new RedbObject<TrashHolderProps>
                {
                    name = "holder",
                    Props = new TrashHolderProps { Target = new RedbObject<TrashTargetProps> { id = keptId } }
                });
            }

            var worker = new BackgroundDeletionService(sp);
            await worker.StartAsync(CancellationToken.None);
            PurgeProgress? poisoned = null, good = null;
            try
            {
                var deadline = DateTime.UtcNow.AddSeconds(30);
                while (DateTime.UtcNow < deadline)
                {
                    await using var scope = sp.CreateAsyncScope();
                    var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
                    poisoned = await redb.GetDeletionProgressAsync(poisonedTrash);
                    good = await redb.GetDeletionProgressAsync(goodTrash);
                    if (poisoned?.Status == PurgeStatus.Failed && (good is null || good.Status == PurgeStatus.Completed))
                        break;
                    await Task.Delay(250);
                }
            }
            finally
            {
                await worker.StopAsync(CancellationToken.None);
            }

            poisoned.Should().NotBeNull();
            poisoned!.Status.Should().Be(PurgeStatus.Failed, "a container blocked by a live reference ends failed instead of being retried forever");
            (good is null || good.Status == PurgeStatus.Completed).Should().BeTrue(
                "the blocked container must not stop the worker from purging the others");
        }
        finally
        {
            await DropDatabaseAsync(cs);
        }
    }
}
