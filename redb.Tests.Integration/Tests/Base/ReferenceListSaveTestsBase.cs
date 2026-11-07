using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using redb.Core;
using redb.Core.Extensions;
using redb.Core.Models.Entities;
using redb.Tests.Integration.Models;

namespace redb.Tests.Integration.Tests.Base;

/// <summary>
/// A list of references, <c>List&lt;RedbObject&lt;T&gt;&gt;</c>, saved the way an array of them is (plan
/// docs/V4/PROPS_CACHE_PROD_AND_TAILS_PLAN.md, finding 6). The save recognised arrays only: references by id in a list
/// got no hash, so the parent's stored hash never matched the loaded one and the object missed the props cache for
/// ever; a new object in a list was never saved (FOREIGN KEY failure); an edit to a loaded one was silently lost.
/// <c>AddNewObjectsAsync</c> hashed its objects before references were resolved at all. Every test builds its own host.
/// </summary>
public abstract class ReferenceListSaveTestsBase
{
    /// <summary>Registers the provider on the options builder (Free or Pro, see <see cref="Register"/>).</summary>
    protected abstract void UseProvider(RedbOptionsBuilder options);

    protected virtual void Register(IServiceCollection services, Action<RedbOptionsBuilder> configure)
        => services.AddRedb(configure);

    private ServiceProvider Build(bool propsCache)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        Register(services, options =>
        {
            UseProvider(options);
            options.Configure(c =>
            {
                c.EnablePropsCache = propsCache;
                // One domain per suite: the Free and Pro suites of one database run in parallel.
                c.CacheDomain = $"reference-list-{GetType().Name}";
            });
        });
        return services.BuildServiceProvider();
    }

    private static async Task<IRedbService> BootAsync(ServiceProvider sp)
    {
        var service = sp.GetRequiredService<IRedbService>();
        await service.InitializeAsync(ensureCreated: true);
        await service.SyncSchemeAsync<CtProbeChildProps>();
        await service.SyncSchemeAsync<ReferenceListProbeProps>();
        await service.InitializeTypeRegistryAsync();
        return service;
    }

    private static string NewTag() => Guid.NewGuid().ToString("N")[..8];

    private static Task<long> SaveChildAsync(IRedbService service, string tag)
        => service.SaveAsync(new RedbObject<CtProbeChildProps> { name = $"reflist-child-{tag}", Props = new CtProbeChildProps { Tag = tag } });

    private static async Task<long> SaveRootWithReferencesAsync(IRedbService service, string tag)
    {
        var first = await SaveChildAsync(service, $"{tag}-1");
        var second = await SaveChildAsync(service, $"{tag}-2");
        return await service.SaveAsync(new RedbObject<ReferenceListProbeProps>
        {
            name = $"reflist-{tag}",
            Props = new ReferenceListProbeProps { Label = tag, Items = [new() { id = first }, new() { id = second }] }
        });
    }

    [Fact]
    public async Task ReferencesById_StoredHashIsTheLoadedHash()
    {
        await using var sp = Build(propsCache: false);
        var service = await BootAsync(sp);
        var rootId = await SaveRootWithReferencesAsync(service, NewTag());

        var loaded = (await service.LoadAsync<ReferenceListProbeProps>(rootId, depth: 10))!;

        loaded.Props.Items.Should().HaveCount(2);
        loaded.ComputeHash().Should().Be(loaded.hash!.Value,
            "the save hashes every reference in the list as id:hash, exactly as the load carries it");
    }

    [Fact]
    public async Task ReferencesById_SecondLoadIsServedFromThePropsCache()
    {
        await using var sp = Build(propsCache: true);
        var service = await BootAsync(sp);
        var rootId = await SaveRootWithReferencesAsync(service, NewTag());
        var cache = ((RedbServiceBase)service).PropsCache.Instance;
        cache.Should().NotBeNull("the props cache must be active, or this test proves nothing");
        cache!.Clear();

        var first = await service.LoadAsync<ReferenceListProbeProps>(rootId, depth: 10);
        var second = await service.LoadAsync<ReferenceListProbeProps>(rootId, depth: 10);

        second.Should().BeSameAs(first, "an object with a list of references is served from the cache like any other");
    }

    [Fact]
    public async Task NewObjectInTheList_IsSavedWithItsParent()
    {
        await using var sp = Build(propsCache: false);
        var service = await BootAsync(sp);
        var tag = NewTag();
        var fresh = new RedbObject<CtProbeChildProps> { name = $"reflist-new-{tag}", Props = new CtProbeChildProps { Tag = $"new-{tag}" } };

        var rootId = await service.SaveAsync(new RedbObject<ReferenceListProbeProps>
        {
            name = $"reflist-{tag}",
            Props = new ReferenceListProbeProps { Label = tag, Items = [fresh] }
        });

        fresh.id.Should().BeGreaterThan(0, "a new object in a list is saved with its parent, as an array element is");
        var loaded = await service.LoadAsync<ReferenceListProbeProps>(rootId, depth: 10);
        loaded!.Props.Items.Should().ContainSingle().Which.Props.Tag.Should().Be($"new-{tag}");
    }

    [Fact]
    public async Task EditedLoadedObjectInTheList_IsSavedWithItsParent()
    {
        await using var sp = Build(propsCache: false);
        var service = await BootAsync(sp);
        var tag = NewTag();
        var childId = await SaveChildAsync(service, $"before-{tag}");
        var rootId = await service.SaveAsync(new RedbObject<ReferenceListProbeProps>
        {
            name = $"reflist-{tag}",
            Props = new ReferenceListProbeProps { Label = tag, Items = [new() { id = childId }] }
        });

        var root = (await service.LoadAsync<ReferenceListProbeProps>(rootId, depth: 10))!;
        root.Props.Items![0].Props.Tag = $"after-{tag}";
        await service.SaveAsync(root);

        (await service.LoadAsync<CtProbeChildProps>(childId, depth: 1))!.Props.Tag.Should().Be($"after-{tag}",
            "an edit to a loaded object in a list is saved with its parent, as for an array element or a single reference");
    }

    [Fact]
    public async Task AddNewObjects_ReferencesById_StoredHashIsTheLoadedHash()
    {
        await using var sp = Build(propsCache: false);
        var service = await BootAsync(sp);
        var tag = NewTag();
        var first = await SaveChildAsync(service, $"{tag}-1");
        var second = await SaveChildAsync(service, $"{tag}-2");
        var root = new RedbObject<ReferenceListProbeProps>
        {
            name = $"reflist-bulk-{tag}",
            Props = new ReferenceListProbeProps { Label = tag, Items = [new() { id = first }, new() { id = second }] }
        };

        var ids = await service.AddNewObjectsAsync(new[] { root });

        var loaded = (await service.LoadAsync<ReferenceListProbeProps>(ids[0], depth: 10))!;
        loaded.ComputeHash().Should().Be(loaded.hash!.Value,
            "the bulk insert resolves the references' hashes before hashing, as the save does");
    }
}
