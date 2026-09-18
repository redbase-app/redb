using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// BR-11 (redb.Tsak/docs/BOUNDARIES_AND_FOLLOWUPS.md): a temporal property changed within the same second must be
/// written. The ChangeTracking save skips an object whose hash equals the stored one, and the hash canon of a
/// DateTime/DateTimeOffset had no sub-second digits - a heartbeat updated twice within a second was silently lost.
/// The second fact guards the other side: the canon keeps milliseconds only, so the hash the save stores is the one
/// the reloaded object recomputes on every database.
/// </summary>
public abstract class SubSecondUpdateTestsBase
{
    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    /// <summary>The Pro hosts run the ChangeTracking save - the strategy with the hash shortcut.</summary>
    protected virtual PropsSaveStrategy SaveStrategy => PropsSaveStrategy.DeleteInsert;

    private ServiceProvider Build()
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = false;
                c.PropsSaveStrategy = SaveStrategy;
                c.AutoRecomputeHash = true;
                c.CacheDomain = $"sub-second-{GetType().Name}";
            });
        });
        return services.BuildServiceProvider();
    }

    private static readonly DateTimeOffset Moment = new(2026, 9, 16, 12, 30, 15, TimeSpan.Zero);

    private static async Task BootAsync(IRedbService redb)
    {
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<TemporalRoundTripProps>();
        await redb.InitializeTypeRegistryAsync();
    }

    private static async Task<long> SeedAsync(ServiceProvider sp, DateTimeOffset moment)
    {
        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        await BootAsync(redb);
        return await redb.SaveAsync(new RedbObject<TemporalRoundTripProps>
        {
            name = $"sub-second-{Guid.NewGuid():N}",
            // Every temporal field set: a default DateOnly/DateTime (MinValue) does not survive SQLite's OLE-date
            // storage (ToOADate returns 0 for MinValue) - a separate finding, not this test's subject.
            Props = new TemporalRoundTripProps
            {
                When = moment.UtcDateTime, Moment = moment, Day = new DateOnly(2026, 9, 16),
                Clock = new TimeOnly(12, 30, 15), Span = TimeSpan.FromMinutes(90), Label = "seed"
            }
        });
    }

    [Fact]
    public async Task AChangeWithinTheSameSecond_IsSaved()
    {
        await using var sp = Build();
        var id = await SeedAsync(sp, Moment);

        await using (var scope = sp.CreateAsyncScope())
        {
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            var obj = (await redb.LoadAsync<TemporalRoundTripProps>(id))!;
            obj.Props.Moment = obj.Props.Moment.AddMilliseconds(100);
            obj.Props.When = obj.Props.When.AddMilliseconds(100);
            await redb.SaveAsync(obj);
        }

        await using (var scope = sp.CreateAsyncScope())
        {
            var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
            var reloaded = (await redb.LoadAsync<TemporalRoundTripProps>(id))!;
            reloaded.Props.Moment.Should().Be(Moment.AddMilliseconds(100),
                "a 100 ms change is a change: the save must not skip it as unchanged");
            reloaded.Props.When.Should().Be(Moment.UtcDateTime.AddMilliseconds(100));
        }
    }

    [Fact]
    public async Task TheStoredHash_IsTheHashOfTheReloadedObject()
    {
        await using var sp = Build();
        // Sub-millisecond noise the databases do not keep: the canon must not keep it either.
        var id = await SeedAsync(sp, Moment.AddMilliseconds(100).AddTicks(4567));

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var reloaded = (await redb.LoadAsync<TemporalRoundTripProps>(id))!;

        reloaded.hash.Should().Be(reloaded.ComputeHash(),
            "the hash the save stored must equal the hash of the reloaded object - the props cache and the " +
            "ChangeTracking shortcut compare exactly these two");
    }
}
