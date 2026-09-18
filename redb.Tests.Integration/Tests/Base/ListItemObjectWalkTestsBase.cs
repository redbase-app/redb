using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Configuration;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// Plan docs/V4/LISTITEM_OBJECT_WALKERS_AND_PRO_SYNC_LOAD_PLAN.md, part A. Reading
/// <see cref="RedbListItem.Object"/> IS a database load, so no reflective walk of a loaded graph may read it: a walk
/// that does fires one load per linked list item on every materialization (the production phantom-load class of
/// 2026-09-09) and, on Pro SQLite, throws through the sync path. The core walkers were closed then; the Pro
/// materialization walkers and the props-cache dirty guard were not.
/// <para>
/// Detector: <see cref="RedbListItem.IsObjectLoaded"/>. A read of Object on the live scope loads the object and keeps it
/// (owner decision 2026-09-15), and no walk swallows a failed read any more (plan, item 4). Every test isolates ONE walker
/// and ends with its positive control - one explicit read loads the object - so a road the detector does not cover fails
/// the test instead of hiding.
/// </para>
/// <para>
/// Free hosts depend on plan part C: until then the Free JSON builders dropped <c>IdObject</c> of list items and the
/// "carries its object link" precondition failed before any walker was judged.
/// </para>
/// </summary>
public abstract class ListItemObjectWalkTestsBase
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
                c.SkipHashValidationOnCacheCheck = false;
                // Own cache domain: this process also holds the shared fixtures on the same database.
                c.CacheDomain = "listitem-walk";
                configure(c);
            });
        });
        return services.BuildServiceProvider();
    }

    private sealed record Seed(long RootId, long LinkedObjectId);

    private static async Task<IRedbService> BootAsync(ServiceProvider sp)
    {
        var boot = sp.GetRequiredService<IRedbService>();
        await boot.InitializeAsync(ensureCreated: true);
        await boot.SyncSchemeAsync<SimpleProps>();
        await boot.SyncSchemeAsync<CtProbeChildProps>();
        await boot.SyncSchemeAsync<CtProbeProps>();
        await boot.SyncSchemeAsync<PersonProps>();
        await boot.InitializeTypeRegistryAsync();
        return boot;
    }

    private static async Task<(RedbListItem Linked, RedbListItem Plain, long LinkedObjectId)> SeedListAsync(IRedbService redb, string tag)
    {
        var linkedObjectId = await redb.SaveAsync(new RedbObject<SimpleProps>
            { name = $"liwalk-linked-{tag}", Props = new SimpleProps { Title = "linked" } });
        var list = await redb.ListProvider.SaveListAsync(RedbList.Create($"liwalk-{tag}", "liwalk"));
        var linked = await redb.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = "linked", IdObject = linkedObjectId });
        var plain = await redb.ListProvider.SaveListItemAsync(new RedbListItem { IdList = list.Id, Value = "plain" });
        return (linked, plain, linkedObjectId);
    }

    /// <summary>Props with references AND list items, one of them linked to an object.</summary>
    private static async Task<Seed> SeedWithReferencesAsync(ServiceProvider sp, string tag, bool onlyTrashedReference = false)
    {
        var redb = await BootAsync(sp);
        var (linked, plain, linkedObjectId) = await SeedListAsync(redb, tag);
        var c1 = await redb.SaveAsync(new RedbObject<CtProbeChildProps> { name = $"liwalk-c1-{tag}", Props = new CtProbeChildProps { Tag = "one" } });
        var c2 = await redb.SaveAsync(new RedbObject<CtProbeChildProps> { name = $"liwalk-c2-{tag}", Props = new CtProbeChildProps { Tag = "two" } });

        var props = new CtProbeProps { Label = "root", Status = linked, Roles = [linked, plain] };
        if (onlyTrashedReference)
        {
            // The only reference points at an object trashed below: nothing is substituted (the substitution walk
            // returns at once on an empty set) and the null-missing walk is the one that runs.
            props.Refs = [new RedbObject<CtProbeChildProps> { id = c2 }];
        }
        else
        {
            props.Refs = [new RedbObject<CtProbeChildProps> { id = c1 }, new RedbObject<CtProbeChildProps> { id = c2 }];
            props.RefDict = new() { ["first"] = new RedbObject<CtProbeChildProps> { id = c1 } };
        }

        var rootId = await redb.SaveAsync(new RedbObject<CtProbeProps> { name = $"liwalk-root-{tag}", Props = props });
        if (onlyTrashedReference)
            await redb.SoftDeleteAsync(new[] { c2 });
        return new Seed(rootId, linkedObjectId);
    }

    /// <summary>Props with list items and no references: its first load runs none of the materialization walkers.</summary>
    private static async Task<Seed> SeedPersonAsync(ServiceProvider sp, string tag)
    {
        var redb = await BootAsync(sp);
        var (linked, plain, linkedObjectId) = await SeedListAsync(redb, tag);
        var rootId = await redb.SaveAsync(new RedbObject<PersonProps>
        {
            name = $"liwalk-person-{tag}",
            Props = new PersonProps { Name = "Person", Age = 30, Email = "p@example.com", Status = linked, Roles = [linked, plain] }
        });
        return new Seed(rootId, linkedObjectId);
    }

    private static string NewTag() => Guid.NewGuid().ToString("N")[..8];

    /// <summary>
    /// No walk read Object: every given instance still carries its link and is not loaded; then one explicit read loads it.
    /// </summary>
    private static void AssertNotWoken(string path, long linkedObjectId, params RedbListItem?[] linkedItems)
    {
        foreach (var item in linkedItems)
        {
            item.Should().NotBeNull($"{path}: the linked list item must be materialized");
            item!.IdObject.Should().Be(linkedObjectId,
                $"{path}: precondition - the item carries its object link, or 'not woken' proves nothing");
            item.IsObjectLoaded.Should().BeFalse(
                $"{path}: no walk may read RedbListItem.Object - reading it IS a database load per linked item");
        }

        linkedItems[0]!.Object.Should().NotBeNull(
            $"{path}: positive control - an explicit read of Object loads it, so the detector is live");
        linkedItems[0]!.IsObjectLoaded.Should().BeTrue();
    }

    [Fact]
    public async Task Load_WithReferences_SubstitutionWalk_DoesNotWakeLinkedListItems()
    {
        await using var sp = Build(_ => { });
        var seed = await SeedWithReferencesAsync(sp, NewTag());

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var root = await redb.LoadAsync<CtProbeProps>(seed.RootId, depth: 10);

        root!.Props.Refs.Should().HaveCount(2).And.OnlyContain(r => r != null && r.IsPropsLoaded,
            "the references are materialized, so the substitution walk ran");
        AssertNotWoken("load with references", seed.LinkedObjectId, root.Props.Status, root.Props.Roles![0]);
    }

    [Fact]
    public async Task Load_WithTrashedReferenceTarget_NullMissingWalk_DoesNotWakeLinkedListItems()
    {
        await using var sp = Build(_ => { });
        var seed = await SeedWithReferencesAsync(sp, NewTag(), onlyTrashedReference: true);

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var root = await redb.LoadAsync<CtProbeProps>(seed.RootId, depth: 10);

        root!.Props.Refs.Should().ContainSingle().Which.Should().BeNull("the trashed target is nulled, so the null-missing walk ran");
        AssertNotWoken("load with a trashed reference target", seed.LinkedObjectId, root.Props.Status, root.Props.Roles![0]);
    }

    [Fact]
    public async Task Load_WithLazyReferences_StubWalk_DoesNotWakeLinkedListItems()
    {
        await using var sp = Build(c => c.EnableLazyReferences = true);
        var seed = await SeedWithReferencesAsync(sp, NewTag());

        await using var scope = sp.CreateAsyncScope();
        var redb = scope.ServiceProvider.GetRequiredService<IRedbService>();
        var root = await redb.LoadAsync<CtProbeProps>(seed.RootId, depth: 1);

        root!.Props.Refs.Should().HaveCount(2).And.OnlyContain(r => r != null && !r.IsPropsLoaded && r.hash != null,
            "the references are enriched stubs, so the stub walk ran");
        AssertNotWoken("load with lazy references", seed.LinkedObjectId, root.Props.Status, root.Props.Roles![0]);
    }

    /// <summary>First load fills the cache without any walker; the second is served from it.</summary>
    private async Task CacheHitAsync(Action<RedbServiceConfiguration> configure, string path,
        Func<IRedbService, long, Task<RedbObject<PersonProps>?>> secondLoad)
    {
        await using var sp = Build(c => { c.EnablePropsCache = true; configure(c); });
        var seed = await SeedPersonAsync(sp, NewTag());
        ((RedbServiceBase)sp.GetRequiredService<IRedbService>()).PropsCache.Instance!.Clear();

        await using (var first = sp.CreateAsyncScope())
        {
            var cached = await first.ServiceProvider.GetRequiredService<IRedbService>().LoadAsync<PersonProps>(seed.RootId, depth: 10);
            cached.Should().NotBeNull();
            cached!.Props.Status!.IsObjectLoaded.Should().BeFalse($"{path}: precondition - the load that fills the cache wakes nothing");
        }

        await using var second = sp.CreateAsyncScope();
        var served = await secondLoad(second.ServiceProvider.GetRequiredService<IRedbService>(), seed.RootId);
        AssertNotWoken(path, seed.LinkedObjectId, served!.Props.Status, served.Props.Roles![0]);
    }

    [Fact]
    public Task CacheHit_PointLoad_DirtyGuard_DoesNotWakeLinkedListItems()
        => CacheHitAsync(_ => { }, "cache hit, point load",
            (redb, id) => redb.LoadAsync<PersonProps>(id, depth: 10));

    [Fact]
    public Task CacheHit_WithoutHashValidation_DirtyGuard_DoesNotWakeLinkedListItems()
        => CacheHitAsync(c => c.SkipHashValidationOnCacheCheck = true, "cache hit without hash validation",
            (redb, id) => redb.LoadAsync<PersonProps>(id, depth: 10));

    [Fact]
    public Task CacheHit_BulkLoad_DirtyGuard_DoesNotWakeLinkedListItems()
        => CacheHitAsync(_ => { }, "cache hit, bulk load",
            async (redb, id) => (RedbObject<PersonProps>)(await redb.LoadAsync(new[] { id }, depth: 10))[0]);

    [Fact]
    public Task CacheHit_SyncPointLoad_DirtyGuard_DoesNotWakeLinkedListItems()
        => CacheHitAsync(_ => { }, "cache hit, sync point load",
            (redb, id) => Task.FromResult(redb.Load<PersonProps>(id, depth: 10)));
}
