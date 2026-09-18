using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Plan docs/V4/LISTITEM_OBJECT_WALKERS_AND_PRO_SYNC_LOAD_PLAN.md, item 4 (owner decision 2026-09-15: "as the save"). No
/// graph walk of a load reads a <c>[RedbIgnore]</c> property - the scheme and the save never do. Covers the walkers that
/// need a database: Pro reference substitution and trashed-target nulling, the stub collector of lazy references, the
/// props-cache collector and dirty guard, and the loader installers of both tiers. The unit twin is
/// <c>WalkerPropertyRuleTests</c>.
/// </summary>
public abstract class IgnoredPropertyWalkTestsBase
{
    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build(Action<RedbServiceConfiguration> configure)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = false;
                c.CacheDomain = "ignored-property-walk";
                configure(c);
            });
        });
        return services.BuildServiceProvider();
    }

    private sealed record Seed(long RootId, string Label);

    private static async Task<Seed> SeedAsync(ServiceProvider sp, bool trashSecondTarget, bool withReferences = true)
    {
        var tag = Guid.NewGuid().ToString("N")[..8];
        var redb = sp.GetRequiredService<IRedbService>();
        await redb.InitializeAsync(ensureCreated: true);
        await redb.SyncSchemeAsync<CtProbeChildProps>();
        await redb.SyncSchemeAsync<IgnoredGetterProbeProps>();
        await redb.InitializeTypeRegistryAsync();

        var c1 = await redb.SaveAsync(new RedbObject<CtProbeChildProps> { name = $"ignored-c1-{tag}", Props = new CtProbeChildProps { Tag = "one" } });
        var c2 = await redb.SaveAsync(new RedbObject<CtProbeChildProps> { name = $"ignored-c2-{tag}", Props = new CtProbeChildProps { Tag = "two" } });
        var label = $"ignored-root-{tag}";
        var rootId = await redb.SaveAsync(new RedbObject<IgnoredGetterProbeProps>
        {
            name = label,
            Props = new IgnoredGetterProbeProps
            {
                Label = label,
                Refs = withReferences
                    ? [new RedbObject<CtProbeChildProps> { id = c1 }, new RedbObject<CtProbeChildProps> { id = c2 }]
                    : null
            }
        });
        if (trashSecondTarget)
            await redb.SoftDeleteAsync(new[] { c2 });
        return new Seed(rootId, label);
    }

    private static int Reads(string label) => IgnoredGetterProbeProps.TechnicalReads.GetValueOrDefault(label);

    private async Task AssertLoadReadsNoIgnoredPropertyAsync(string path, Action<RedbServiceConfiguration> configure,
        bool trashSecondTarget, Func<IRedbService, long, Task> load, bool withReferences = true)
    {
        await using var sp = Build(configure);
        var seed = await SeedAsync(sp, trashSecondTarget, withReferences);
        var before = Reads(seed.Label);

        await using var scope = sp.CreateAsyncScope();
        await load(scope.ServiceProvider.GetRequiredService<IRedbService>(), seed.RootId);

        Reads(seed.Label).Should().Be(before, $"{path}: no walk of a load reads a [RedbIgnore] property");
    }

    [Fact]
    public Task Load_WithReferences_ReadsNoRedbIgnoreProperty()
        => AssertLoadReadsNoIgnoredPropertyAsync("load with references", _ => { }, trashSecondTarget: false,
            async (redb, id) => (await redb.LoadAsync<IgnoredGetterProbeProps>(id, depth: 10)).Should().NotBeNull());

    [Fact]
    public Task Load_WithTrashedReferenceTarget_ReadsNoRedbIgnoreProperty()
        => AssertLoadReadsNoIgnoredPropertyAsync("load with a trashed reference target", _ => { }, trashSecondTarget: true,
            async (redb, id) => (await redb.LoadAsync<IgnoredGetterProbeProps>(id, depth: 10)).Should().NotBeNull());

    [Fact]
    public Task Load_WithLazyReferences_ReadsNoRedbIgnoreProperty()
        => AssertLoadReadsNoIgnoredPropertyAsync("load with lazy references", c => c.EnableLazyReferences = true, trashSecondTarget: false,
            async (redb, id) => (await redb.LoadAsync<IgnoredGetterProbeProps>(id, depth: 1)).Should().NotBeNull());

    [Fact]
    public Task Load_ThroughThePropsCache_ReadsNoRedbIgnoreProperty()
        => AssertLoadReadsNoIgnoredPropertyAsync("props cache set and hit", c => c.EnablePropsCache = true, trashSecondTarget: false,
            async (redb, id) =>
            {
                ((RedbServiceBase)redb).PropsCache.Instance!.Clear();
                var first = await redb.LoadAsync<IgnoredGetterProbeProps>(id, depth: 10);
                var hit = await redb.LoadAsync<IgnoredGetterProbeProps>(id, depth: 10);
                hit.Should().BeSameAs(first, "the second load is served from the cache");
            });
}
